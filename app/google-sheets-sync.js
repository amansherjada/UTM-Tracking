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

    // Extract timestamps with proper handling
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

    // Extract parameters with explicit precedence
    const originalParams = data.original_params || {};

    const rowValues = [
      timestamp.toISOString(),
      data.phoneNumber || 'N/A',
      (data.original_params?.source || data.source || 'direct').substring(0, 100),
      (data.original_params?.medium || data.medium || 'organic').substring(0, 100),
      (data.original_params?.campaign || data.campaign || 'none').substring(0, 100),
      (data.original_params?.content || data.content || 'none').substring(0, 100),
      (data.original_params?.placement || data.placement || 'N/A').substring(0, 100),
      data.hasEngaged ? '✅ YES' : '❌ NO',
      engagedTimestamp,
      (data.attribution_source || 'unknown').substring(0, 50),
      data.contactId || 'N/A',
      data.conversationId || 'N/A',
      (data.contactName || 'Anonymous').substring(0, 100),
      data.lastMessage ? data.lastMessage.substring(0, 150).replace(/\n/g, ' ') : 'No text content'
    ];

    return rowValues;
  });
}

async function syncCollectionToSheet(collectionName, spreadsheetId) {
  const MAX_RETRIES = 3;
  let attempt = 0;

  console.log(`🔄 Starting sync for ${collectionName} (Attempt ${attempt + 1}/${MAX_RETRIES})`);

  while (attempt < MAX_RETRIES) {
    try {
      const sheetsClient = await initializeSheetsClient();
      const SHEET_NAME = 'Sheet1';

      // 1. Get spreadsheet metadata
      const { data: spreadsheet } = await sheetsClient.spreadsheets.get({
        spreadsheetId,
        includeGridData: false
      });

      console.log(`✅ Accessing spreadsheet: "${spreadsheet.properties.title}"`);

      // 2. Check/create sheet
      const sheetExists = spreadsheet.sheets?.some(s => s.properties?.title === SHEET_NAME);
      if (!sheetExists) {
        await sheetsClient.spreadsheets.batchUpdate({
          spreadsheetId,
          resource: {
            requests: [{
              addSheet: {
                properties: {
                  title: SHEET_NAME,
                  gridProperties: {
                    rowCount: 1000,
                    columnCount: 14
                  }
                }
              }
            }]
          }
        });
      }

      // 3. Verify headers
      const { data: sheetsData } = await sheetsClient.spreadsheets.values.get({
        spreadsheetId,
        range: `${SHEET_NAME}!A1:N1`
      });

      const requiredHeaders = [
        'Timestamp', 'Phone Number', 'UTM Source', 'UTM Medium',
        'UTM Campaign', 'UTM Content', 'Placement', 'Engaged',
        'Engaged At', 'Attribution Source', 'Contact ID',
        'Conversation ID', 'Contact Name', 'Last Message'
      ];

      if (!sheetsData.values || !sheetsData.values[0]) {
        await sheetsClient.spreadsheets.values.update({
          spreadsheetId,
          range: `${SHEET_NAME}!A1:N1`,
          valueInputOption: 'RAW',
          resource: { values: [requiredHeaders] }
        });
      }

      // 4. Fetch and process documents
      const snapshot = await db.collection(collectionName)
        .where('hasEngaged', '==', true)
        .where('syncedToSheets', '==', false)
        .where('source', '!=', 'direct_message')
        .limit(250)
        .get();

      if (snapshot.empty) {
        console.log(`ℹ️ No new records in ${collectionName}`);
        return { count: 0 };
      }

      const rows = convertToSheetRows(snapshot.docs);
      const updatePromises = snapshot.docs.map(doc => 
        doc.ref.update({
          syncedToSheets: true,
          lastSynced: FieldValue.serverTimestamp()
        })
      );

      // 5. Append to sheet
      const appendResponse = await sheetsClient.spreadsheets.values.append({
        spreadsheetId,
        range: `${SHEET_NAME}!A:N`,
        valueInputOption: 'USER_ENTERED',
        insertDataOption: 'INSERT_ROWS',
        resource: { values: rows }
      });

      await Promise.all(updatePromises);
      console.log(`✅ ${collectionName} documents updated`);

      return {
        count: rows.length,
        spreadsheetId,
        sheetName: SHEET_NAME
      };

    } catch (err) {
      attempt++;
      console.error(`❌ ${collectionName} sync attempt ${attempt} failed:`, err.message);
      
      if (attempt >= MAX_RETRIES) {
        throw new Error(`Final sync failure for ${collectionName}: ${err.message}`);
      }
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
    // Sync americanhairline
    if (process.env.SHEETS_SPREADSHEET_ID) {
      results.americanhairline = await syncCollectionToSheet(
        'utmClicks',
        process.env.SHEETS_SPREADSHEET_ID
      );
    }

    // Sync alchemane
    if (process.env.SHEETS_SPREADSHEET_ID_ALCHEMANE) {
      results.alchemane = await syncCollectionToSheet(
        'alchemaneutmClicks',
        process.env.SHEETS_SPREADSHEET_ID_ALCHEMANE
      );
    }

    return results;
  } catch (error) {
    console.error('💥 Global sync error:', error);
    return results;
  }
}

async function setupRealtimeSync() {
  console.log('🔄 Setting up real-time sync for both collections');
  
  const collectionsToSync = [
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

  for (const { name, spreadsheetId } of collectionsToSync) {
    if (!spreadsheetId) {
      console.warn(`⚠️ No spreadsheet ID configured for ${name}`);
      continue;
    }

    const listener = db.collection(name)
      .where('hasEngaged', '==', true)
      .where('syncedToSheets', '==', false)
      .onSnapshot(async (snapshot) => {
        try {
          if (snapshot.empty) return;

          const sheetsClient = await initializeSheetsClient();
          const changes = snapshot.docChanges();

          const rows = convertToSheetRows(changes.map(c => c.doc));
          const updatePromises = changes.map(c => 
            c.doc.ref.update({ 
              syncedToSheets: true,
              lastSynced: FieldValue.serverTimestamp()
            })
          );

          await Promise.all(updatePromises);
          
          await sheetsClient.spreadsheets.values.append({
            spreadsheetId,
            range: 'Sheet1!A:N',
            valueInputOption: 'USER_ENTERED',
            resource: { values: rows }
          });

          console.log(`🔥 Realtime sync completed for ${name}: ${changes.length} docs`);
        } catch (error) {
          console.error(`❌ Realtime sync error for ${name}:`, error);
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
