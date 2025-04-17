const { Firestore } = require('@google-cloud/firestore');
const { GoogleAuth } = require('google-auth-library');
const { sheets } = require('@googleapis/sheets');
const admin = require('firebase-admin'); 
require('dotenv').config();
const fs = require('fs');

// Initialize Firestore
const db = new Firestore({
  projectId: process.env.GCP_PROJECT_ID,
  databaseId: 'utm-tracker-db',
  keyFilename: '/secrets/secrets'
});

// Initialize Google Sheets API client
async function initializeSheetsClient() {
  try {
    const credentials = JSON.parse(fs.readFileSync('/secrets/secrets', 'utf8'));
    
    const auth = new GoogleAuth({
      scopes: [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
      ],
      credentials
    });

    return sheets({ version: 'v4', auth: await auth.getClient() });
  } catch (error) {
    console.error('🔥 Sheets client initialization failed:', error.message);
    throw error;
  }
}

function convertToSheetRows(docs) {
  return docs.map(doc => {
    const data = doc.data();
    console.log('Processing document ID:', doc.id);
    console.log('Raw document data:', JSON.stringify(data, null, 2));
    
    // Enhanced deep field access with debugging
    const getField = (fieldPath, defaultValue = 'N/A') => {
      try {
        const parts = fieldPath.split('.');
        let value = data;
        
        for (const part of parts) {
          if (!value || typeof value !== 'object') {
            console.log(`⚠️ Field path lookup failed at "${part}" in "${fieldPath}"`);
            return defaultValue;
          }
          value = value[part];
          if (value === undefined || value === null) {
            console.log(`⚠️ Null/undefined value at "${part}" in "${fieldPath}"`);
            return defaultValue;
          }
        }
        
        console.log(`✅ Found value for "${fieldPath}": ${value}`);
        return value;
      } catch (err) {
        console.error(`❌ Error accessing "${fieldPath}":`, err.message);
        return defaultValue;
      }
    };
    
    // Extract timestamps
    let timestamp;
    if (data.click_time && typeof data.click_time.toDate === 'function') {
      timestamp = data.click_time.toDate();
    } else if (data.timestamp && typeof data.timestamp.toDate === 'function') {
      timestamp = data.timestamp.toDate();
    } else {
      timestamp = new Date();
    }
    
    let engagedTimestamp = 'N/A';
    if (data.engagedAt) {
      if (typeof data.engagedAt.toDate === 'function') {
        engagedTimestamp = data.engagedAt.toDate().toISOString();
      } else if (data.engagedAt instanceof Date) {
        engagedTimestamp = data.engagedAt.toISOString();
      }
    }
    
    // Extract original params with priority resolution
    const originalParams = data.original_params || {};
    
    // Create explicit string values with fallbacks for all fields
    const sourceValue = String(getField('original_params.source') || data.source || 'direct');
    const mediumValue = String(getField('original_params.medium') || data.medium || 'organic');
    const campaignValue = String(getField('original_params.campaign') || data.campaign || 'none');
    const contentValue = String(getField('original_params.content') || data.content || 'none');
    const placementValue = String(getField('original_params.placement') || data.placement || 'N/A');
    const attributionValue = String(data.attribution_source || 'unknown');
    const contactIdValue = String(data.contactId || 'N/A');
    const conversationIdValue = String(data.conversationId || 'N/A');
    const contactNameValue = String(data.contactName || 'Anonymous');
    const lastMessageValue = data.lastMessage ? 
      String(data.lastMessage).substring(0, 150).replace(/\n/g, ' ') : '';
      
    console.log('Attribution Source:', attributionValue);
    console.log('Contact ID:', contactIdValue);
    console.log('Conversation ID:', conversationIdValue);
    console.log('Contact Name:', contactNameValue);
    
    // Create the row with explicit String conversions to avoid data type issues
    const row = [
      timestamp.toISOString(),
      String(data.phoneNumber || 'N/A'),
      sourceValue,
      mediumValue,
      campaignValue,
      contentValue,
      placementValue,
      data.hasEngaged ? '✅ YES' : '❌ NO',
      engagedTimestamp,
      attributionValue,
      contactIdValue,
      conversationIdValue,
      contactNameValue,
      lastMessageValue
    ];
    
    console.log('Generated row data:', JSON.stringify(row));
    return row;
  });
}

async function syncToSheets() {
  const SPREADSHEET_ID = process.env.SHEETS_SPREADSHEET_ID;
  const SHEET_NAME = 'UTM_Tracking';
  const MAX_RETRIES = 3;
  let attempt = 0;

  console.log(`🔄 Starting sync (Attempt ${attempt + 1}/${MAX_RETRIES})`);

  while (attempt < MAX_RETRIES) {
    try {
      const sheetsClient = await initializeSheetsClient();
      
      const { data: spreadsheet } = await sheetsClient.spreadsheets.get({
        spreadsheetId: SPREADSHEET_ID
      });
      console.log(`✅ Accessing spreadsheet: "${spreadsheet.properties.title}"`);

      // Verify headers match expected format
      const { data: sheetsData } = await sheetsClient.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: `${SHEET_NAME}!A1:N1`
      });
      
      const expectedHeaders = [
        'Timestamp', 'Phone Number', 'UTM Source', 'UTM Medium', 'UTM Campaign',
        'UTM Content', 'Placement', 'Engaged', 'Engaged At', 'Attribution Source',
        'Contact ID', 'Conversation ID', 'Contact Name', 'Last Message'
      ];
      
      if (!sheetsData.values || !sheetsData.values[0]) {
        // Sheet is empty, add headers
        await sheetsClient.spreadsheets.values.update({
          spreadsheetId: SPREADSHEET_ID,
          range: `${SHEET_NAME}!A1:N1`,
          valueInputOption: 'RAW',
          requestBody: { values: [expectedHeaders] }
        });
        console.log('✅ Added header row to empty sheet');
      } else {
        console.log('Existing headers:', JSON.stringify(sheetsData.values[0]));
        console.log('Expected headers:', JSON.stringify(expectedHeaders));
      }

      const snapshot = await db.collection('utmClicks')
        .where('hasEngaged', '==', true)
        .where('syncedToSheets', '==', false)
        .where('source', '!=', 'direct_message')
        .limit(250)
        .get();

      if (snapshot.empty) {
        console.log('ℹ️ No new records to sync');
        return { count: 0 };
      }

      console.log(`🔍 Found ${snapshot.docs.length} documents to sync`);
      const rows = convertToSheetRows(snapshot.docs);
      
      // Batch update operations for better reliability
      const batch = db.batch();
      const updateTime = admin.firestore.FieldValue.serverTimestamp();
      
      snapshot.docs.forEach(doc => {
        batch.update(doc.ref, {
          syncedToSheets: true,
          lastSynced: updateTime
        });
      });

      // Add debug logs for row data
      console.log(`Sending ${rows.length} rows to Google Sheets`);
      console.log('First row sample:', JSON.stringify(rows[0]));
      
      // Use update instead of append for more reliable insertion
      const rowsRange = `${SHEET_NAME}!A${sheetsData.values.length + 1}:N${sheetsData.values.length + rows.length}`;
      console.log(`Writing to range: ${rowsRange}`);
      
      const updateResponse = await sheetsClient.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: rowsRange,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: rows }
      });

      await batch.commit();
      console.log('✅ Sync completed successfully');
      console.log('📝 Updated range:', updateResponse.data.updatedRange);
      
      return { 
        count: rows.length,
        spreadsheetId: SPREADSHEET_ID,
        sheetName: SHEET_NAME
      };

    } catch (err) {
      attempt++;
      console.error(`❌ Attempt ${attempt} failed:`, err.message);
      
      if (attempt >= MAX_RETRIES) {
        console.error('💥 Maximum retries exceeded');
        throw new Error(`Final sync failure: ${err.message}`);
      }
      
      await new Promise(resolve => setTimeout(resolve, attempt * 2000));
    }
  }
}

async function scheduledSync() {
  const startTime = Date.now();
  const result = { 
    success: false,
    duration: 0,
    syncedCount: 0
  };

  try {
    const syncResult = await syncToSheets();
    result.success = true;
    result.syncedCount = syncResult.count;
    result.duration = Date.now() - startTime;
    result.spreadsheetId = syncResult.spreadsheetId;
  } catch (err) {
    result.error = err.message;
    result.retryable = err.message.includes('quota') || err.code === 429;
  } finally {
    result.timestamp = new Date().toISOString();
    console.log('⏱️ Sync result:', result);
    return result;
  }
}

module.exports = { syncToSheets, scheduledSync };
