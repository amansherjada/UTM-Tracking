const { Firestore, FieldValue } = require('@google-cloud/firestore');
const { GoogleAuth } = require('google-auth-library');
const { sheets } = require('@googleapis/sheets');
const fs = require('fs');
require('dotenv').config();

// Configuration mapping for multi-database support
const syncConfig = {
  default: {
    databaseId: 'utm-tracker-db',
    spreadsheetId: process.env.SHEETS_SPREADSHEET_ID,
    sheetName: 'Sheet1'
  },
  alchemane: {
    databaseId: 'alchemane-utm-tracker-db',
    spreadsheetId: process.env.SHEETS_SPREADSHEET_ID_ALCHEMANE,
    sheetName: 'Sheet1'
  }
};

// Database factory function
function getDatabase(website = 'default') {
  const config = syncConfig[website] || syncConfig.default;
  return new Firestore({
    projectId: process.env.GCP_PROJECT_ID,
    databaseId: config.databaseId,
    keyFilename: '/secrets/secrets'
  });
}

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
    console.error('Sheets client initialization failed:', error.message);
    throw error;
  }
}

// Convert Firestore documents to sheet rows
function convertToSheetRows(docs) {
  return docs.map(doc => {
    const data = doc.data();
    const originalParams = data.original_params || {};

    // Timestamp handling
    const timestamp = (data.click_time?.toDate?.() || data.timestamp?.toDate?.() || new Date()).toISOString();
    
    // Engagement timestamp handling
    const engagedTimestamp = data.engagedAt?.toDate?.()?.toISOString() || 'N/A';

    return [
      timestamp,
      data.phoneNumber || 'N/A',
      originalParams.source || data.source || 'direct',
      originalParams.medium || data.medium || 'organic',
      originalParams.campaign || data.campaign || 'none',
      originalParams.content || data.content || 'none',
      originalParams.placement || data.placement || 'N/A',
      data.hasEngaged ? '✅ YES' : '❌ NO',
      engagedTimestamp,
      data.attribution_source || 'unknown',
      data.contactId || 'N/A',
      data.conversationId || 'N/A',
      data.contactName || 'Anonymous',
      data.lastMessage?.substring(0, 150).replace(/\n/g, ' ') || 'No text content'
    ];
  });
}

// Updated sync function with multi-database support
async function syncToSheets(website = 'default') {
  const config = syncConfig[website] || syncConfig.default;
  const db = getDatabase(website);
  const SPREADSHEET_ID = config.spreadsheetId;
  const SHEET_NAME = config.sheetName;
  const MAX_RETRIES = 3;
  let attempt = 0;

  while (attempt < MAX_RETRIES) {
    try {
      const sheetsClient = await initializeSheetsClient();

      // Verify/Create sheet
      const { data: spreadsheet } = await sheetsClient.spreadsheets.get({
        spreadsheetId: SPREADSHEET_ID
      });

      if (!spreadsheet.sheets?.some(s => s.properties?.title === SHEET_NAME)) {
        await sheetsClient.spreadsheets.batchUpdate({
          spreadsheetId: SPREADSHEET_ID,
          resource: { requests: [{ addSheet: { properties: { title: SHEET_NAME } } }] }
        });
      }

      // Check/Set headers
      const { data: sheetsData } = await sheetsClient.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: `${SHEET_NAME}!A1:N1`
      });

      if (!sheetsData.values?.[0]) {
        await sheetsClient.spreadsheets.values.update({
          spreadsheetId: SPREADSHEET_ID,
          range: `${SHEET_NAME}!A1:N1`,
          valueInputOption: 'RAW',
          resource: { values: [[
            'Timestamp', 'Phone Number', 'UTM Source', 'UTM Medium',
            'UTM Campaign', 'UTM Content', 'Placement', 'Engaged',
            'Engaged At', 'Attribution Source', 'Contact ID',
            'Conversation ID', 'Contact Name', 'Last Message'
          ]]}
        });
      }

      // Get unsynced documents
      const snapshot = await db.collection('utmClicks')
        .where('hasEngaged', '==', true)
        .where('syncedToSheets', '==', false)
        .limit(250)
        .get();

      if (snapshot.empty) return { count: 0 };

      const rows = convertToSheetRows(snapshot.docs);

      // Mark as synced
      const updatePromises = snapshot.docs.map(doc => 
        doc.ref.update({
          syncedToSheets: true,
          lastSynced: FieldValue.serverTimestamp()
        })
      );

      // Append to Sheets
      await sheetsClient.spreadsheets.values.append({
        spreadsheetId: SPREADSHEET_ID,
        range: `${SHEET_NAME}!A:N`,
        valueInputOption: 'USER_ENTERED',
        resource: { values: rows }
      });

      await Promise.all(updatePromises);
      return { count: rows.length };

    } catch (err) {
      if (++attempt >= MAX_RETRIES) throw new Error(`Sync failed: ${err.message}`);
      await new Promise(resolve => setTimeout(resolve, attempt * 2000));
    }
  }
}

// Scheduled sync for all databases
async function scheduledSync() {
  const results = [];
  for (const website of Object.keys(syncConfig)) {
    try {
      results.push({ website, ...await syncToSheets(website) });
    } catch (err) {
      results.push({ website, error: err.message });
    }
  }
  return results;
}

// Real-time sync setup for all databases
async function setupRealtimeSync() {
  const unsubscribers = [];

  for (const website of Object.keys(syncConfig)) {
    const config = syncConfig[website];
    const db = getDatabase(website);

    const unsubscribe = db.collection('utmClicks')
      .where('hasEngaged', '==', true)
      .where('syncedToSheets', '==', false)
      .onSnapshot(async (snapshot) => {
        if (snapshot.empty) return;

        const sheetsClient = await initializeSheetsClient();
        const rows = convertToSheetRows(snapshot.docs);

        await Promise.all(snapshot.docs.map(doc => 
          doc.ref.update({ syncedToSheets: true })
        ));

        await sheetsClient.spreadsheets.values.append({
          spreadsheetId: config.spreadsheetId,
          range: `${config.sheetName}!A:N`,
          valueInputOption: 'USER_ENTERED',
          resource: { values: rows }
        });
      });

    unsubscribers.push(unsubscribe);
  }

  return () => unsubscribers.forEach(unsub => unsub());
}

module.exports = { syncToSheets, scheduledSync, setupRealtimeSync };
