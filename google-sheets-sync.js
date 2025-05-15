const { Firestore, FieldValue } = require('@google-cloud/firestore');
const { GoogleAuth } = require('google-auth-library');
const { sheets } = require('@googleapis/sheets');
const fs = require('fs');
require('dotenv').config();

// Initialize Firestore (using native GCP Firestore SDK)
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
      // UTM Source
      originalParams.CampaignSource || originalParams['Campaign Source'] || originalParams['Campaign_Source'] || originalParams.source || data.source || 'direct',
      // UTM Medium
      originalParams.AdSetName || originalParams['Ad Set Name'] || originalParams['Ad_Set_Name'] || originalParams.medium || data.medium || 'organic',
      // UTM Campaign
      originalParams.CampaignName || originalParams['Campaign Name'] || originalParams['Campaign_Name'] || originalParams.campaign || data.campaign || 'none',
      // UTM Content
      originalParams.AdName || originalParams['Ad Name'] || originalParams['Ad_Name'] || originalParams.content || data.content || 'none',
      // Placement
      originalParams.Placement || originalParams.placement || data.placement || 'N/A',
      data.hasEngaged ? '✅ YES' : '❌ NO',
      engagedTimestamp,
      data.attribution_source || 'unknown',
      data.contactId || 'N/A',
      data.conversationId || 'N/A',
      data.contactName || 'Anonymous',
      data.lastMessage ? data.lastMessage.substring(0, 150).replace(/\n/g, ' ') : 'No text content'
    ];

    return rowValues;
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

      // 1. Get spreadsheet metadata and verify sheet exists
      const { data: spreadsheet } = await sheetsClient.spreadsheets.get({
        spreadsheetId: SPREADSHEET_ID,
        includeGridData: false
      });

      console.log(`✅ Accessing spreadsheet: "${spreadsheet.properties.title}"`);

      // 2. Check if sheet exists
      const sheetExists = spreadsheet.sheets?.some(s => s.properties?.title === SHEET_NAME);

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

      // 🔥 CRITICAL FIX: Use native Firestore FieldValue
      const updatePromises = snapshot.docs.map(doc => {
        return doc.ref.update({
          syncedToSheets: true,
          lastSynced: FieldValue.serverTimestamp() // Fixed line
        });
      });

      const appendResponse = await sheetsClient.spreadsheets.values.append({
        spreadsheetId: SPREADSHEET_ID,
        range: `${SHEET_NAME}!A:N`,
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

// In google-sheets-sync.js

async function setupRealtimeSync() {
    console.log('🔄 Setting up real-time Firestore to Sheets sync');
    
    try {
      // Create a query for documents that need syncing
      const query = db.collection('utmClicks')
        .where('hasEngaged', '==', true)
        .where('syncedToSheets', '==', false)
        .where('source', '!=', 'direct_message');
      
      // Set up the listener with error handling
      const unsubscribe = query.onSnapshot(async (snapshot) => {
        try {
          // Skip empty snapshots
          if (snapshot.empty) {
            return;
          }
          
          // Get the modified or added documents
          const docsToSync = [];
          snapshot.docChanges().forEach((change) => {
            // Only process new or modified documents
            if (change.type === 'added' || change.type === 'modified') {
              docsToSync.push(change.doc);
            }
          });
          
          if (docsToSync.length === 0) {
            return;
          }
          
          console.log(`🔥 Real-time sync triggered for ${docsToSync.length} documents`);
          
          // Initialize the sheets client
          const sheetsClient = await initializeSheetsClient();
          const SPREADSHEET_ID = process.env.SHEETS_SPREADSHEET_ID;
          const SHEET_NAME = 'Sheet1';
          
          // Convert the documents to sheet rows
          const rows = convertToSheetRows(docsToSync);
          
          // First mark these documents as synced to prevent duplicate syncs
          // This is important in case the sheets API call fails
          const updatePromises = docsToSync.map(doc => {
            return doc.ref.update({
              syncedToSheets: true,
              lastSynced: FieldValue.serverTimestamp()
            });
          });
          
          // Wait for all updates to complete
          await Promise.all(updatePromises);
          
          // Now append the data to Google Sheets
          const appendResponse = await sheetsClient.spreadsheets.values.append({
            spreadsheetId: SPREADSHEET_ID,
            range: `${SHEET_NAME}!A:N`,
            valueInputOption: 'USER_ENTERED',
            insertDataOption: 'INSERT_ROWS',
            resource: { values: rows }
          });
          
          console.log('✅ Real-time sync completed');
          console.log('📝 Sheets update:', appendResponse.data.updates.updatedRange);
          
        } catch (err) {
          console.error('❌ Real-time sync error:', err);
        }
      }, (error) => {
        console.error('🚨 Listener error:', error);
        // Attempt to recreate the listener after a delay
        setTimeout(() => setupRealtimeSync(), 60000);
      });
      
      // Return the unsubscribe function to allow cleanup if needed
      return unsubscribe;
    } catch (err) {
      console.error('💥 Failed to set up real-time sync:', err);
      // Attempt to recreate the listener after a delay
      setTimeout(() => setupRealtimeSync(), 60000);
    }
  }

module.exports = { syncToSheets, scheduledSync, setupRealtimeSync  };
