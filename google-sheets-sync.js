const { Firestore } = require('@google-cloud/firestore');
const { GoogleAuth } = require('google-auth-library');
const { sheets } = require('@googleapis/sheets');
require('dotenv').config();
const fs = require('fs');

// Initialize Firestore with enhanced configuration
const db = new Firestore({
  projectId: process.env.GCP_PROJECT_ID,
  databaseId: 'utm-tracker-db',
  keyFilename: '/secrets/secrets'
});

// Initialize Google Sheets API client
async function initializeSheetsClient() {
  try {
    // Read credentials directly from the mounted file
    const credentials = JSON.parse(fs.readFileSync('/secrets/secrets', 'utf8'));

    const auth = new GoogleAuth({
      scopes: [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
      ],
      credentials
    });

    const client = await auth.getClient();
    return sheets({ version: 'v4', auth: client });
  } catch (error) {
    console.error('🔥 Failed to initialize Sheets client:', error.message);
    throw error;
  }
}

// Convert Firestore docs to sheet rows
function convertToSheetRows(docs) {
  return docs.map(doc => {
    const data = doc.data();
    return [
      data.timestamp?.toDate().toISOString() || new Date().toISOString(),
      data.phoneNumber || 'N/A',
      data.source || 'Unknown',
      data.medium || 'Unknown',
      data.campaign || 'Unnamed Campaign',
      data.content || 'No Content',
      data.hasEngaged ? '✅ YES' : '❌ NO',
      data.engagedAt?.toDate().toISOString() || 'N/A',
      data.lastMessage?.substring(0, 100) + (data.lastMessage?.length > 100 ? '...' : '') || ''
    ];
  });
}

// Main sync function with retry logic
async function syncToSheets() {
  const SPREADSHEET_ID = process.env.SHEETS_SPREADSHEET_ID;
  const SHEET_NAME = 'Sheet1';
  const MAX_RETRIES = 3;
  let attempt = 0;

  console.log(`🔄 Starting Google Sheets sync (Attempt ${attempt + 1}/${MAX_RETRIES})`);

  while (attempt < MAX_RETRIES) {
    try {
      const sheetsClient = await initializeSheetsClient();
      
      // Verify spreadsheet access
      const { data: spreadsheet } = await sheetsClient.spreadsheets.get({
        spreadsheetId: SPREADSHEET_ID
      });
      console.log(`✅ Accessed spreadsheet: "${spreadsheet.properties.title}"`);

      // Get unsynced records
      const snapshot = await db.collection('utmClicks')
        .where('hasEngaged', '==', true)
        .where('syncedToSheets', '==', false)
        .limit(100)
        .get();

      if (snapshot.empty) {
        console.log('ℹ️ No new engaged users to sync');
        return { count: 0 };
      }

      const rows = convertToSheetRows(snapshot.docs);
      console.log(`📊 Preparing to sync ${rows.length} rows`);

      // Batch update Firestore
      const batch = db.batch();
      snapshot.docs.forEach(doc => {
        batch.update(doc.ref, { syncedToSheets: true });
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
      console.log('🎉 Sync completed successfully');
      console.log('📈 Updated range:', appendResponse.data.updates.updatedRange);
      
      return { count: rows.length };

    } catch (err) {
      attempt++;
      console.error(`❌ Attempt ${attempt} failed:`, err.message);
      
      if (attempt >= MAX_RETRIES) {
        console.error('💥 Maximum retries exceeded');
        if (err.response?.data) {
          console.error('🔍 Error details:', JSON.stringify(err.response.data.error, null, 2));
        }
        throw new Error(`Sheets sync failed: ${err.message}`);
      }
      
      console.log(`⏳ Retrying in ${attempt * 2} seconds...`);
      await new Promise(resolve => setTimeout(resolve, attempt * 2000));
    }
  }
}

// Scheduled sync with enhanced logging
async function scheduledSync() {
  const startTime = Date.now();
  const result = { success: false };

  try {
    const syncResult = await syncToSheets();
    result.success = true;
    result.syncedCount = syncResult.count;
    result.duration = Date.now() - startTime;
  } catch (err) {
    result.error = err.message;
    result.retryable = err.code === 429 || err.message.includes('quota');
  } finally {
    result.timestamp = new Date().toISOString();
    console.log('📆 Scheduled sync result:', result);
    return result;
  }
}

module.exports = { syncToSheets, scheduledSync };
