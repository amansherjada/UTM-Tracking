const { Firestore } = require('@google-cloud/firestore');
const { GoogleAuth } = require('google-auth-library');
require('dotenv').config();

// Initialize Firestore with explicit configuration
const db = new Firestore({
  projectId: process.env.GCP_PROJECT_ID,
  keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS,
  databaseId: 'utm-tracker-db'
});

// Service account details logging
const serviceAccount = require(process.env.GOOGLE_APPLICATION_CREDENTIALS);
console.log('Using service account:', serviceAccount.client_email);

// Sheet configuration
const SPREADSHEET_ID = process.env.SHEETS_SPREADSHEET_ID;
const SHEET_NAME = 'Sheet1';

function convertToSheetRows(docs) {
  return docs.map(doc => {
    const data = doc.data();
    return [
      data.timestamp?.toDate().toISOString() || '',
      data.phoneNumber || 'N/A',
      data.source || 'Unknown',
      data.medium || 'Unknown',
      data.campaign || 'Unnamed Campaign',
      data.content || 'No Content',
      data.hasEngaged ? 'YES' : 'NO',
      data.engagedAt?.toDate().toISOString() || 'N/A',
      data.lastMessage?.substring(0, 100) || ''
    ];
  });
}

async function syncToSheets() {
  console.log('🔄 Starting Google Sheets sync...');
  console.log(`📄 Spreadsheet ID: ${SPREADSHEET_ID}`);

  try {
    const auth = new GoogleAuth({
      scopes: [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
      ],
      projectId: process.env.GCP_PROJECT_ID
    });

    const client = await auth.getClient();
    
    // Verify spreadsheet access
    try {
      const res = await client.request({
        url: `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}`,
        method: 'GET'
      });
      console.log('✅ Successfully accessed spreadsheet:', res.data.properties.title);
    } catch (err) {
      console.error('❌ Spreadsheet access failed!');
      console.error('Verify:');
      console.error(`1. Spreadsheet ID is correct (current: ${SPREADSHEET_ID})`);
      console.error(`2. Shared with service account: ${serviceAccount.client_email}`);
      throw err;
    }

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
    const appendResponse = await client.request({
      url: `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/${SHEET_NAME}!A1:append`,
      method: 'POST',
      params: {
        valueInputOption: 'USER_ENTERED',
        insertDataOption: 'INSERT_ROWS'
      },
      data: { values: rows }
    });

    await batch.commit();
    console.log('✅ Sync completed successfully');
    console.log('📈 Updated range:', appendResponse.data.updates.updatedRange);
    
    return { count: rows.length };

  } catch (err) {
    console.error('❌ Sync failed:', err.message);
    if (err.response?.data) {
      console.error('Google API error details:', err.response.data.error);
    }
    throw new Error(`Sheets sync failed: ${err.message}`);
  }
}

async function scheduledSync() {
  try {
    const startTime = Date.now();
    const result = await syncToSheets();
    
    return {
      success: true,
      syncedCount: result.count,
      duration: Date.now() - startTime,
      timestamp: new Date().toISOString()
    };
  } catch (err) {
    return {
      success: false,
      error: err.message,
      retryable: err.code === 429,
      timestamp: new Date().toISOString()
    };
  }
}

module.exports = { syncToSheets, scheduledSync };