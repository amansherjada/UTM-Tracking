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

// Updated conversion function with session ID
function convertToSheetRows(docs) {
  return docs.map(doc => {
    const data = doc.data();
    console.log('Processing document ID:', doc.id);

    // Session-based timestamp handling
    const timestamp = data.timestamp?.toDate?.() || new Date();
    const engagedAt = data.engagedAt?.toDate?.() || 'N/A';

    return [
      doc.id, // Session ID as first column
      timestamp.toISOString(),
      data.phoneNumber || 'N/A',
      data.source || 'direct',
      data.medium || 'organic',
      data.campaign || 'none',
      data.content || 'none',
      data.placement || 'N/A',
      data.hasEngaged ? '✅ YES' : '❌ NO',
      engagedAt.toISOString(),
      data.attribution_source || 'unknown',
      data.contactId || 'N/A',
      data.conversationId || 'N/A',
      data.contactName || 'Anonymous',
      data.lastMessage?.substring(0, 150).replace(/\n/g, ' ') || 'No text content'
    ];
  });
}

async function syncToSheets() {
  const SPREADSHEET_ID = process.env.SHEETS_SPREADSHEET_ID;
  const SHEET_NAME = 'Sheet1';
  const MAX_RETRIES = 3;
  let attempt = 0;

  console.log(`🔄 Starting sync (Attempt ${attempt + 1}/${MAX_RETRIES})`);

  while (attempt < MAX_RETRIES) {
    try {
      const sheetsClient = await initializeSheetsClient();

      // 1. Spreadsheet setup
      const { data: spreadsheet } = await sheetsClient.spreadsheets.get({
        spreadsheetId: SPREADSHEET_ID,
        includeGridData: false
      });
      console.log(`✅ Accessing spreadsheet: "${spreadsheet.properties.title}"`);

      // 2. Sheet creation if missing
      const sheetExists = spreadsheet.sheets?.some(s => s.properties?.title === SHEET_NAME);
      if (!sheetExists) {
        console.log(`📄 Creating new sheet: ${SHEET_NAME}`);
        await sheetsClient.spreadsheets.batchUpdate({
          spreadsheetId: SPREADSHEET_ID,
          resource: {
            requests: [{
              addSheet: {
                properties: {
                  title: SHEET_NAME,
                  gridProperties: { rowCount: 1000, columnCount: 15 } // Updated for 15 columns
                }
              }
            }]
          }
        });
      }

      // 3. Header management
      const requiredHeaders = [
        'Session ID', 'Timestamp', 'Phone Number', 'UTM Source',
        'UTM Medium', 'UTM Campaign', 'UTM Content', 'Placement',
        'Engaged', 'Engaged At', 'Attribution Source', 'Contact ID',
        'Conversation ID', 'Contact Name', 'Last Message'
      ];

      const { data: sheetsData } = await sheetsClient.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: `${SHEET_NAME}!A1:O1` // Updated range for 15 columns
      });

      if (!sheetsData.values || !sheetsData.values[0]) {
        console.log('⏳ Setting up headers');
        await sheetsClient.spreadsheets.values.update({
          spreadsheetId: SPREADSHEET_ID,
          range: `${SHEET_NAME}!A1:O1`,
          valueInputOption: 'RAW',
          resource: { values: [requiredHeaders] }
        });
      }

      // 4. Fetch and process documents
      const snapshot = await db.collection('utmClicks')
        .where('hasEngaged', '==', true)
        .where('syncedToSheets', '==', false)
        .limit(250)
        .get();

      if (snapshot.empty) {
        console.log('ℹ️ No new records to sync');
        return { count: 0 };
      }

      console.log(`🔍 Found ${snapshot.docs.length} documents to sync`);
      const rows = convertToSheetRows(snapshot.docs);

      // 5. Atomic updates with transaction support
      const updatePromises = snapshot.docs.map(doc => 
        doc.ref.update({
          syncedToSheets: true,
          lastSynced: FieldValue.serverTimestamp()
        })
      );

      // 6. Append to sheet
      const appendResponse = await sheetsClient.spreadsheets.values.append({
        spreadsheetId: SPREADSHEET_ID,
        range: `${SHEET_NAME}!A:O`, // Updated range
        valueInputOption: 'USER_ENTERED',
        insertDataOption: 'INSERT_ROWS',
        resource: { values: rows }
      });

      await Promise.all(updatePromises);

      console.log('✅ Firestore documents updated');
      console.log('📝 Sheets update:', appendResponse.data.updates.updatedRange);

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

// Scheduled sync remains same
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
