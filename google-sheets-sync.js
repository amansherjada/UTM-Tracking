const { Firestore, FieldValue } = require('@google-cloud/firestore');
const { GoogleAuth } = require('google-auth-library');
const { sheets } = require('@googleapis/sheets');
const fs = require('fs');
require('dotenv').config();

// Initialize Firestore
const db = new Firestore({
  projectId: process.env.GCP_PROJECT_ID,
  databaseId: 'utm-tracker-db',
  keyFilename: '/secrets/secrets'
});

async function initializeSheetsClient() {
  try {
    const credentials = JSON.parse(fs.readFileSync('/secrets/secrets', 'utf8'));
    const auth = new GoogleAuth({
      scopes: ['https://www.googleapis.com/auth/spreadsheets'],
      credentials
    });
    return sheets({ version: 'v4', auth: await auth.getClient() });
  } catch (error) {
    console.error('Sheets client initialization failed:', error.message);
    throw error;
  }
}

function convertToSheetRows(docs) {
  return docs.map(doc => {
    const data = doc.data();
    
    // Improved timestamp handling
    const timestamp = data.click_time?.toDate() || data.timestamp?.toDate() || new Date();
    const engagedTimestamp = data.engagedAt?.toDate()?.toISOString() || 'N/A';
    
    // Safely handle potential undefined values
    const originalParams = data.original_params || {};
    
    return [
      timestamp.toISOString(),
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
      (data.contactName || 'Anonymous').substring(0, 100),
      (data.lastMessage || 'No text').substring(0, 150).replace(/\n/g, ' ')
    ];
  });
}

async function ensureSheetExists(sheetsClient, spreadsheetId, sheetName) {
  try {
    const { data: spreadsheet } = await sheetsClient.spreadsheets.get({
      spreadsheetId,
      includeGridData: false
    });

    const sheetExists = spreadsheet.sheets.some(s => 
      s.properties.title === sheetName
    );

    if (!sheetExists) {
      await sheetsClient.spreadsheets.batchUpdate({
        spreadsheetId,
        resource: {
          requests: [{
            addSheet: {
              properties: {
                title: sheetName,
                gridProperties: { rowCount: 1000, columnCount: 14 }
              }
            }
          }]
        }
      });
    }

    // Verify headers
    const requiredHeaders = [
      'Timestamp', 'Phone Number', 'UTM Source', 'UTM Medium',
      'UTM Campaign', 'UTM Content', 'Placement', 'Engaged',
      'Engaged At', 'Attribution Source', 'Contact ID',
      'Conversation ID', 'Contact Name', 'Last Message'
    ];

    const { data: headerData } = await sheetsClient.spreadsheets.values.get({
      spreadsheetId,
      range: `${sheetName}!A1:N1`
    });

    if (!headerData.values || !headerData.values[0]) {
      await sheetsClient.spreadsheets.values.update({
        spreadsheetId,
        range: `${sheetName}!A1:N1`,
        valueInputOption: 'RAW',
        resource: { values: [requiredHeaders] }
      });
    }

  } catch (error) {
    console.error('Sheet setup failed:', error.message);
    throw error;
  }
}

async function syncCollectionToSheet(collectionName, spreadsheetId) {
  const MAX_RETRIES = 3;
  let attempt = 0;
  const SHEET_NAME = 'Sheet1';

  while (attempt < MAX_RETRIES) {
    try {
      const sheetsClient = await initializeSheetsClient();
      
      // Ensure sheet exists and has headers
      await ensureSheetExists(sheetsClient, spreadsheetId, SHEET_NAME);

      const snapshot = await db.collection(collectionName)
        .where('hasEngaged', '==', true)
        .where('syncedToSheets', '==', false)
        .limit(250)
        .get();

      if (snapshot.empty) return { count: 0 };

      const rows = convertToSheetRows(snapshot.docs);
      const batch = db.batch();
      
      snapshot.docs.forEach(doc => {
        batch.update(doc.ref, {
          syncedToSheets: true,
          lastSynced: FieldValue.serverTimestamp()
        });
      });

      await sheetsClient.spreadsheets.values.append({
        spreadsheetId,
        range: `${SHEET_NAME}!A:N`,
        valueInputOption: 'USER_ENTERED',
        resource: { values: rows }
      });

      await batch.commit();
      return { count: rows.length };

    } catch (err) {
      attempt++;
      console.error(`Sync failed (attempt ${attempt}):`, err.message);
      if (attempt >= MAX_RETRIES) throw err;
      await new Promise(resolve => setTimeout(resolve, attempt * 2000));
    }
  }
}

async function syncToSheets() {
  const results = {
    americanhairline: { success: false },
    alchemane: { success: false }
  };

  try {
    if (process.env.SHEETS_SPREADSHEET_ID) {
      results.americanhairline = await syncCollectionToSheet(
        'utmClicks',
        process.env.SHEETS_SPREADSHEET_ID
      );
    }

    if (process.env.SHEETS_SPREADSHEET_ID_ALCHEMANE) {
      results.alchemane = await syncCollectionToSheet(
        'alchemaneutmClicks',
        process.env.SHEETS_SPREADSHEET_ID_ALCHEMANE
      );
    }

    return results;
  } catch (error) {
    console.error('Global sync error:', error);
    return results;
  }
}

async function setupRealtimeSync() {
  const collections = [
    { 
      name: 'utmClicks',
      spreadsheetId: process.env.SHEETS_SPREADSHEET_ID 
    },
    { 
      name: 'alchemaneutmClicks',
      spreadsheetId: process.env.SHEETS_SPREADSHEET_ID_ALCHEMANE 
    }
  ];

  const unsubscribeFunctions = [];

  for (const { name, spreadsheetId } of collections) {
    if (!spreadsheetId) continue;

    const listener = db.collection(name)
      .where('hasEngaged', '==', true)
      .where('syncedToSheets', '==', false)
      .onSnapshot(async (snapshot) => {
        try {
          if (snapshot.empty) return;
          
          const sheetsClient = await initializeSheetsClient();
          await ensureSheetExists(sheetsClient, spreadsheetId, 'Sheet1');
          
          const rows = convertToSheetRows(snapshot.docs);
          const batch = db.batch();

          await sheetsClient.spreadsheets.values.append({
            spreadsheetId,
            range: 'Sheet1!A:N',
            valueInputOption: 'USER_ENTERED',
            resource: { values: rows }
          });

          snapshot.docs.forEach(doc => {
            batch.update(doc.ref, { 
              syncedToSheets: true,
              lastSynced: FieldValue.serverTimestamp()
            });
          });

          await batch.commit();
          console.log(`Real-time sync: ${snapshot.size} docs to ${name}`);

        } catch (error) {
          console.error(`Real-time sync error (${name}):`, error.message);
        }
      });

    unsubscribeFunctions.push(listener);
  }

  return () => unsubscribeFunctions.forEach(fn => fn());
}

module.exports = { 
  syncToSheets, 
  scheduledSync: syncToSheets,
  setupRealtimeSync 
};
