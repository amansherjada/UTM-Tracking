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
    
    // Build the row with ALL fields mapped explicitly
    const rowValues = [
      // 1. Timestamp
      timestamp.toISOString(),
      
      // 2. Phone Number
      data.phoneNumber || 'N/A',
      
      // 3. UTM Source
      originalParams.source || data.source || 'direct',
      
      // 4. UTM Medium
      originalParams.medium || data.medium || 'organic',
      
      // 5. UTM Campaign
      originalParams.campaign || data.campaign || 'none',
      
      // 6. UTM Content
      originalParams.content || data.content || 'none',
      
      // 7. Placement
      originalParams.placement || data.placement || 'N/A',
      
      // 8. Engaged
      data.hasEngaged ? '✅ YES' : '❌ NO',
      
      // 9. Engaged At
      engagedTimestamp,
      
      // 10. Attribution Source
      data.attribution_source || 'unknown',
      
      // 11. Contact ID
      data.contactId || 'N/A',
      
      // 12. Conversation ID
      data.conversationId || 'N/A',
      
      // 13. Contact Name
      data.contactName || 'Anonymous',
      
      // 14. Last Message
      data.lastMessage ? data.lastMessage.substring(0, 150).replace(/\n/g, ' ') : 'No text content'
    ];

    return rowValues;
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
      
      // 1. Get spreadsheet metadata and verify sheet exists
      const { data: spreadsheet } = await sheetsClient.spreadsheets.get({
        spreadsheetId: SPREADSHEET_ID,
        includeGridData: false
      });
      console.log(`✅ Accessing spreadsheet: "${spreadsheet.properties.title}"`);
      
      // 2. Check if sheet exists
      let sheetExists = spreadsheet.sheets.some(s => s.properties.title === SHEET_NAME);
      
      // 3. Create sheet if it doesn't exist
      if (!sheetExists) {
        console.log(`📄 Creating new sheet: ${SHEET_NAME}`);
        await sheetsClient.spreadsheets.batchUpdate({
          spreadsheetId: SPREADSHEET_ID,
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
      
      // 4. Now handle headers
      const { data: sheetsData } = await sheetsClient.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: `${SHEET_NAME}!A1:N1`
      });

      const requiredHeaders = [
        'Timestamp', 'Phone Number', 'UTM Source', 'UTM Medium', 
        'UTM Campaign', 'UTM Content', 'Placement', 'Engaged', 
        'Engaged At', 'Attribution Source', 'Contact ID', 
        'Conversation ID', 'Contact Name', 'Last Message'
      ];

      if (!sheetsData.values || !sheetsData.values[0]) {
        console.log('⏳ Setting up headers');
        await sheetsClient.spreadsheets.values.update({
          spreadsheetId: SPREADSHEET_ID,
          range: `${SHEET_NAME}!A1:N1`,
          valueInputOption: 'RAW',
          resource: { values: [requiredHeaders] }
        });
        console.log('✅ Created header row');
      }

      // Get documents to sync
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

      // Batch update Firestore documents
      const batch = db.batch();
      
      // FIX: Use the proper Firestore field value syntax based on the initialized client
      snapshot.docs.forEach(doc => {
        batch.update(doc.ref, {
          syncedToSheets: true,
          lastSynced: admin.firestore.FieldValue.serverTimestamp()
        });
      });

      // FIXED: Use append instead of update for more reliable insertion
      const appendResponse = await sheetsClient.spreadsheets.values.append({
        spreadsheetId: SPREADSHEET_ID,
        range: `${SHEET_NAME}!A:N`,
        valueInputOption: 'USER_ENTERED',
        insertDataOption: 'INSERT_ROWS',
        resource: { values: rows }
      });

      await batch.commit();
      console.log('✅ Firestore batch committed');
      console.log('📝 Sheets append response:', JSON.stringify(appendResponse.data, null, 2));
      
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
