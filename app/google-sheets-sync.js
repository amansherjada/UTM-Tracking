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

// COMPLETELY REWRITTEN: Fixed nested object extraction with proper Firestore handling
function convertToSheetRows(docs) {
  return docs.map(doc => {
    // Get raw data and create JavaScript object
    const rawData = doc.data();
    console.log('Raw document data:', JSON.stringify(rawData, null, 2));
    
    // Safe access function to handle potential undefined properties
    const safeGet = (obj, path, defaultValue = 'N/A') => {
      if (!obj) return defaultValue;
      
      const parts = path.split('.');
      let current = obj;
      
      for (const part of parts) {
        if (current[part] === undefined || current[part] === null) {
          return defaultValue;
        }
        current = current[part];
      }
      
      return current;
    };
    
    // Get timestamp with fallbacks
    let timestamp;
    if (rawData.click_time && typeof rawData.click_time.toDate === 'function') {
      timestamp = rawData.click_time.toDate();
    } else if (rawData.timestamp && typeof rawData.timestamp.toDate === 'function') {
      timestamp = rawData.timestamp.toDate();
    } else {
      timestamp = new Date();
    }
    
    // Extract engaged timestamp with fallbacks
    let engagedTimestamp = 'N/A';
    if (rawData.engagedAt) {
      if (typeof rawData.engagedAt.toDate === 'function') {
        engagedTimestamp = rawData.engagedAt.toDate().toISOString();
      } else if (rawData.engagedAt instanceof Date) {
        engagedTimestamp = rawData.engagedAt.toISOString();
      }
    }
    
    // Check if original_params exists and is not null/undefined
    let campaignValue = 'none';
    let contentValue = 'none';
    
    // Explicitly check for original_params and its properties
    if (rawData.original_params) {
      console.log('Found original_params:', JSON.stringify(rawData.original_params, null, 2));
      
      // Try to access the campaign property
      if (rawData.original_params.campaign) {
        campaignValue = rawData.original_params.campaign;
        console.log('Using campaign from original_params:', campaignValue);
      }
      
      // Try to access the content property (if it exists)
      if (rawData.original_params.content) {
        contentValue = rawData.original_params.content;
      } else if (rawData.original_params.ad_name) {
        // Try alternative name for content
        contentValue = rawData.original_params.ad_name;
      }
    } else {
      console.log('original_params not found or is null/undefined');
      campaignValue = rawData.campaign || 'none';
      contentValue = rawData.content || 'none';
    }
    
    // Prepare the row data with explicit type checking and conversions
    return [
      timestamp.toISOString(),
      rawData.phoneNumber || 'N/A',
      rawData.source || 'direct',
      rawData.medium || 'organic',
      campaignValue,  // Use extracted campaign value
      contentValue,   // Use extracted content value
      rawData.placement || 'N/A',
      rawData.hasEngaged === true ? '✅ YES' : '❌ NO',
      engagedTimestamp,
      rawData.attribution_source || 'unknown',
      rawData.contactId || 'N/A',
      rawData.conversationId || 'N/A',
      rawData.contactName || 'Anonymous',
      rawData.lastMessage ? rawData.lastMessage.substring(0, 150).replace(/\n/g, ' ') : ''
    ];
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

      // Get Google Sheets structure to check for header row
      const { data: sheetsData } = await sheetsClient.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: `${SHEET_NAME}!A1:N1`, // Check header row
      });
      
      // If sheet is empty or needs header row update, add it
      if (!sheetsData.values || !sheetsData.values[0] || sheetsData.values[0].length < 14) {
        const headers = [
          'Timestamp',
          'Phone Number',
          'Source',
          'Medium',
          'Campaign',
          'Content',
          'Placement',
          'Engaged',
          'Engaged At',
          'Attribution',
          'Contact ID',
          'Conversation ID',
          'Contact Name',
          'Last Message'
        ];
        
        await sheetsClient.spreadsheets.values.update({
          spreadsheetId: SPREADSHEET_ID,
          range: `${SHEET_NAME}!A1:N1`,
          valueInputOption: 'RAW',
          requestBody: { values: [headers] }
        });
        
        console.log('✅ Updated sheet headers with new placement column');
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
      console.log(`📊 Processing ${rows.length} records`);

      const batch = db.batch();
      const updateTime = admin.firestore.FieldValue.serverTimestamp();
      
      snapshot.docs.forEach(doc => {
        batch.update(doc.ref, {
          syncedToSheets: true,
          lastSynced: updateTime
        });
      });

      // Modified range to include the new placement column
      const appendResponse = await sheetsClient.spreadsheets.values.append({
        spreadsheetId: SPREADSHEET_ID,
        range: `${SHEET_NAME}!A:N`,
        valueInputOption: 'USER_ENTERED',
        insertDataOption: 'INSERT_ROWS',
        requestBody: { values: rows }
      });

      await batch.commit();
      console.log('✅ Sync completed successfully');
      console.log('📝 Updated range:', appendResponse.data.updates.updatedRange);
      
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
