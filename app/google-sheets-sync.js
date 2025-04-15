const { Firestore } = require('@google-cloud/firestore');
const { GoogleAuth } = require('google-auth-library');
const { sheets } = require('@googleapis/sheets');
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

// Enhanced row conversion with new fields
function convertToSheetRows(docs) {
  return docs.map(doc => {
    const data = doc.data();
    return [
      data.timestamp?.toDate().toISOString() || new Date().toISOString(),
      data.phoneNumber || 'N/A',
      data.source === 'direct_message' ? 'Direct Message' : data.source || 'direct',
      data.medium || 'organic',
      data.campaign || 'none',
      data.content || 'none',
      data.hasEngaged ? '✅ YES' : '❌ NO',
      data.engagedAt?.toDate().toISOString() || 'N/A',
      data.attribution_source || 'unknown',
      data.contactId || 'N/A',          // New field
      data.conversationId || 'N/A',     // New field
      data.contactName || 'Anonymous',  // New field
      data.lastMessage?.substring(0, 150).replace(/\n/g, ' ') || '' // Truncated message
    ];
  });
}

// Main sync function with enhanced error handling
async function syncToSheets() {
  const SPREADSHEET_ID = process.env.SHEETS_SPREADSHEET_ID;
  const SHEET_NAME = 'UTM_Tracking';
  const MAX_RETRIES = 3;
  let attempt = 0;

  console.log(`🔄 Starting sync (Attempt ${attempt + 1}/${MAX_RETRIES})`);

  while (attempt < MAX_RETRIES) {
    try {
      const sheetsClient = await initializeSheetsClient();
      
      // Verify spreadsheet access
      const { data: spreadsheet } = await sheetsClient.spreadsheets.get({
        spreadsheetId: SPREADSHEET_ID
      });
      console.log(`✅ Accessing spreadsheet: "${spreadsheet.properties.title}"`);

      // Get unsynced records
      const snapshot = await db.collection('utmClicks')
        .where('hasEngaged', '==', true)
        .where('syncedToSheets', '==', false)
        .limit(250)  // Increased batch size
        .get();

      if (snapshot.empty) {
        console.log('ℹ️ No new records to sync');
        return { count: 0 };
      }

      const rows = convertToSheetRows(snapshot.docs);
      console.log(`📊 Processing ${rows.length} records`);

      // Batch update Firestore
      const batch = db.batch();
      const updateTime = admin.firestore.FieldValue.serverTimestamp();
      
      snapshot.docs.forEach(doc => {
        batch.update(doc.ref, {
          syncedToSheets: true,
          lastSynced: updateTime
        });
      });

      // Append to Google Sheets
      const appendResponse = await sheetsClient.spreadsheets.values.append({
        spreadsheetId: SPREADSHEET_ID,
        range: `${SHEET_NAME}!A1`,
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

// Enhanced scheduled sync
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
