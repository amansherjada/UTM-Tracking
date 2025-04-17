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
    
    // IMPROVED: Enhanced path checking for multiple levels of nesting
    const getNestedValue = (data, possiblePaths, defaultValue) => {
      // Try each possible path in order of priority
      for (const path of possiblePaths) {
        try {
          let value = data;
          const parts = path.split('.');
          
          for (const part of parts) {
            if (value === undefined || value === null || typeof value !== 'object') {
              value = undefined;
              break;
            }
            value = value[part];
          }
          
          if (value !== undefined && value !== null && value !== '') {
            console.log(`✅ Found value using path ${path}: ${value}`);
            return value;
          }
        } catch (err) {
          console.log(`❌ Path ${path} failed: ${err.message}`);
        }
      }
      console.log(`⚠️ Using default value for ${possiblePaths[0]}: ${defaultValue}`);
      return defaultValue;
    };
    
    // Extract timestamp with fallbacks
    let timestamp;
    if (data.click_time && typeof data.click_time.toDate === 'function') {
      timestamp = data.click_time.toDate();
    } else if (data.timestamp && typeof data.timestamp.toDate === 'function') {
      timestamp = data.timestamp.toDate();
    } else {
      timestamp = new Date();
    }
    
    // Extract engagement timestamp
    let engagedTimestamp = 'N/A';
    if (data.engagedAt) {
      if (typeof data.engagedAt.toDate === 'function') {
        engagedTimestamp = data.engagedAt.toDate().toISOString();
      } else if (data.engagedAt instanceof Date) {
        engagedTimestamp = data.engagedAt.toISOString();
      }
    }
    
    // IMPROVED: Check multiple possible paths for each field with different naming conventions
    return [
      timestamp.toISOString(),
      data.phoneNumber || 'N/A',
      getNestedValue(data, [
        'original_params.campaign_source',
        'original_params.original_params.campaign_source',
        'source'
      ], 'direct'),
      
      getNestedValue(data, [
        'original_params.medium',
        'original_params.adset_name',
        'original_params.original_params.adset_name',
        'medium'
      ], 'organic'),
      
      getNestedValue(data, [
        'original_params.campaign',
        'original_params.campaign_name',
        'original_params.original_params.campaign_name',
        'campaign'
      ], 'none'),
      
      getNestedValue(data, [
        'original_params.content',
        'original_params.ad_name',
        'original_params.original_params.ad_name',
        'content'
      ], 'none'),
      
      getNestedValue(data, [
        'original_params.placement',
        'original_params.original_params.placement',
        'placement'
      ], 'N/A'),
      
      data.hasEngaged ? '✅ YES' : '❌ NO',
      engagedTimestamp,
      data.attribution_source || 'unknown',
      data.contactId || 'N/A',
      data.conversationId || 'N/A',
      data.contactName || 'Anonymous',
      data.lastMessage?.substring(0, 150).replace(/\n/g, ' ') || ''
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

      // Verify/update headers
      const { data: sheetsData } = await sheetsClient.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: `${SHEET_NAME}!A1:N1`
      });
      
      if (!sheetsData.values || sheetsData.values[0]?.length < 14) {
        const headers = [
          'Timestamp', 'Phone Number', 'Source', 'Medium', 'Campaign',
          'Content', 'Placement', 'Engaged', 'Engaged At', 'Attribution',
          'Contact ID', 'Conversation ID', 'Contact Name', 'Last Message'
        ];
        
        await sheetsClient.spreadsheets.values.update({
          spreadsheetId: SPREADSHEET_ID,
          range: `${SHEET_NAME}!A1:N1`,
          valueInputOption: 'RAW',
          requestBody: { values: [headers] }
        });
        console.log('✅ Updated sheet headers');
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
      
      const batch = db.batch();
      const updateTime = admin.firestore.FieldValue.serverTimestamp();
      
      snapshot.docs.forEach(doc => {
        batch.update(doc.ref, {
          syncedToSheets: true,
          lastSynced: updateTime
        });
      });

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
