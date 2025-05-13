const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const admin = require('firebase-admin');
const { getFirestore } = require('firebase-admin/firestore');
const { SecretManagerServiceClient } = require('@google-cloud/secret-manager');
const crypto = require('crypto');
const fs = require('fs/promises');
const secretClient = new SecretManagerServiceClient();
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 8080;

// Immediate server startup with minimal configuration
const server = app.listen(PORT, '0.0.0.0', () => {
  console.log(`Server listening on port ${PORT}`);
  console.log('Immediate endpoints available: /health');
});

// Global error handling
process.on('unhandledRejection', (reason) => {
  console.error('Unhandled Rejection:', reason);
});

process.on('uncaughtException', (error) => {
  console.error('Uncaught Exception:', error);
});

server.on('error', (err) => {
  console.error('Server error:', err);
  process.exit(1);
});

// Essential middleware
app.use(cors());
app.use(helmet());
app.use(express.json());

// Health endpoint
app.get('/health', (req, res) => {
  res.status(200).json({
    status: 'healthy',
    timestamp: new Date().toISOString(),
  });
});

// Deferred initialization
setImmediate(async () => {
  try {
    console.log('Starting async initialization...');

    // Load credentials
    const credsPath = '/secrets/secrets';
    const serviceAccount = JSON.parse(await fs.readFile(credsPath));
    
    // Initialize Firebase with a single app
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
      databaseURL: `https://${process.env.GCP_PROJECT_ID}.firebaseio.com`,
    });

    // Initialize the default database (American Hairline)
    const americanHairlineDb = getFirestore();
    americanHairlineDb.settings({ timeout: 10000 });

    // Initialize the Alchemane database
    const alchemaneDb = getFirestore('alchemane-utm-tracker-db');
    alchemaneDb.settings({ timeout: 10000 });
    
    // Verify both Firestore connections
    let firestoreConnected = false;
    const maxRetries = 3;
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        await americanHairlineDb.listCollections();
        await alchemaneDb.listCollections();
        firestoreConnected = true;
        break;
      } catch (err) {
        console.error(`Firestore connection attempt ${attempt} failed:`, err);
        await new Promise(resolve => setTimeout(resolve, 2000));
      }
    }

    if (!firestoreConnected) {
      throw new Error('Failed to connect to Firestore');
    }

    // Define collections in each database
    const americanHairlineCollection = americanHairlineDb.collection('utmClicks');
    const alchemaneCollection = alchemaneDb.collection('utmClicks');
    console.log('Firestore databases connected successfully');

    // Database/collection selector function
    function getDatabaseForWebsite(website) {
      return website === 'alchemane' ? alchemaneDb : americanHairlineDb;
    }
    
    function getCollectionForWebsite(website) {
      return website === 'alchemane' ? alchemaneCollection : americanHairlineCollection;
    }

    // Secret management
    const secretCache = new Map();
    async function getSecret(secretName) {
      if (secretCache.has(secretName)) return secretCache.get(secretName);
      const name = `projects/${process.env.GCP_PROJECT_ID}/secrets/${secretName}/versions/latest`;
      const [version] = await secretClient.accessSecretVersion({ name });
      const secretValue = version.payload.data.toString('utf8');
      secretCache.set(secretName, secretValue);
      return secretValue;
    }

    // Security middleware
    const verifyGallabox = async (req, res, next) => {
      try {
        const token = req.headers['x-gallabox-token'];
        const gallaboxToken = await getSecret('gallabox-token');
        if (token !== gallaboxToken) {
          return res.status(401).send('Invalid token');
        }
        next();
      } catch (error) {
        console.error('Token verification error:', error);
        res.status(500).send('Authentication failed');
      }
    };

    // Enhanced Gallabox Webhook Handler
    app.post('/gallabox-webhook', verifyGallabox, async (req, res) => {
      try {
        const event = req.body;
        console.log('Incoming webhook payload:', JSON.stringify(event, null, 2));
        
        // Extract critical identifiers
        const senderPhone = event.whatsapp?.from?.replace(/^0+/, '') || '';
        const contactId = event.contactId || event.contact?.id || null;
        const conversationId = event.conversationId || null;
        const contactName = event.contact?.name || null;
        const messageContent = event.whatsapp?.text?.body || 'No text content';

        // Phone number normalization
        let normalizedPhone = senderPhone;
        const countryCode = '91';
        if (normalizedPhone && !normalizedPhone.startsWith(countryCode)) {
          normalizedPhone = `${countryCode}${normalizedPhone}`;
        }

        if (!normalizedPhone) {
          return res.status(400).json({ error: 'Missing phone number' });
        }

        let sessionId;
        let utmData = {
          source: 'direct_message',
          medium: 'whatsapp',
          campaign: 'organic',
          content: 'none'
        };
        let attribution = 'direct';
        let website = 'americanhairline'; // Default website
        
        // Matching Priority 1: Context Parameter
        if (event.context) {
          try {
            const context = JSON.parse(Buffer.from(event.context, 'base64').toString());
            if (context?.session_id) {
              sessionId = context.session_id;
              utmData = context;
              attribution = 'context';
              website = context.website || 'americanhairline';
              console.log(`Context match: ${sessionId}, Website: ${website}`);
            }
          } catch (err) {
            console.warn('Invalid context format:', err);
          }
        }

        // Skip direct messages as requested
        if (utmData.source === 'direct_message') {
          console.log(`Skipping direct message from ${normalizedPhone}`);
          return res.status(200).json({
            status: 'skipped_direct_message',
            website
          });
        }

        // Get collection based on website
        const targetCollection = getCollectionForWebsite(website);
        console.log(`Using database: ${website}`);

        // Matching Priority 2: Gallabox Identifiers
        if (!sessionId && (contactId || conversationId)) {
          console.log(`Attempting Gallabox ID match for ${website}`);
          
          const fiveMinutesAgo = admin.firestore.Timestamp.fromDate(
            new Date(Date.now() - 5 * 60 * 1000)
          );

          const query = targetCollection
            .where('hasEngaged', '==', false)
            .where('timestamp', '>=', fiveMinutesAgo)
            .orderBy('timestamp', 'desc')
            .limit(5);

          const snapshot = await query.get();

          if (!snapshot.empty) {
            sessionId = snapshot.docs[0].id;
            utmData = snapshot.docs[0].data();
            attribution = 'gallabox_id_match';
            console.log(`Matched with recent click in ${website} database: ${sessionId}`);

            await targetCollection.doc(sessionId).update({
              contactId,
              conversationId,
              contactName: contactName || null
            });
          }
        }

        // Matching Priority 3: Phone Number (if available in click records)
        if (!sessionId) {
          const phoneMatch = await targetCollection
            .where('phoneNumber', '==', normalizedPhone)
            .where('hasEngaged', '==', false)
            .orderBy('timestamp', 'desc')
            .limit(1)
            .get();

          if (!phoneMatch.empty) {
            sessionId = phoneMatch.docs[0].id;
            utmData = phoneMatch.docs[0].data();
            attribution = 'phone_match';
            console.log(`Phone number match in ${website} database: ${sessionId}`);
          }
        }

        // Update existing records
        if (sessionId) {
          const updateData = {
            hasEngaged: true,
            phoneNumber: normalizedPhone,
            engagedAt: admin.firestore.FieldValue.serverTimestamp(),
            syncedToSheets: false,
            attribution_source: attribution,
            website,
            contactId,
            conversationId,
            ...(contactName && { contactName }),
            ...(messageContent && { lastMessage: messageContent })
          };

          try {
            await getDatabaseForWebsite(website).runTransaction(async (transaction) => {
              const docRef = targetCollection.doc(sessionId);
              const doc = await transaction.get(docRef);
              
              if (doc.exists) {
                transaction.update(docRef, updateData);
              } else {
                transaction.set(docRef, {
                  ...utmData,
                  website,
                  ...updateData,
                  timestamp: admin.firestore.FieldValue.serverTimestamp()
                });
              }
            });
          } catch (transactionError) {
            console.error('Transaction failed:', transactionError);
            throw transactionError;
          }
        } else {
          console.log(`No matching session found for ${normalizedPhone}`);
          sessionId = 'not_matched';
        }

        console.log(`Processed message from ${normalizedPhone} with attribution: ${attribution} for website: ${website}`);
        res.status(200).json({ 
          status: 'processed',
          sessionId,
          source: utmData.source,
          attribution,
          website
        });

      } catch (err) {
        console.error('Webhook processing error:', err);
        res.status(500).json({ 
          error: 'Processing failed',
          details: err.message
        });
      }
    });

    // Store-click endpoint with website param handling
    app.post('/store-click', async (req, res) => {
      try {
        const { session_id, original_params, website, ...rawData } = req.body;
        
        // Session ID validation
        if (!session_id) {
          return res.status(400).json({ error: 'Missing session_id' });
        }

        // Skip direct messages as requested
        if (rawData.source === 'direct_message') {
          return res.status(200).json({
            status: 'skipped_direct_message',
            session_id
          });
        }
        
        // Extract values with consistent naming
        const params = original_params || {};
        
        // Create standardized structure
        const utmData = {
          source: params.source || rawData.source || 'facebook',
          medium: params.medium || rawData.medium || 'fb_ads',
          campaign: params.campaign || rawData.campaign || 'unknown',
          content: params.content || rawData.content || 'unknown',
          placement: params.placement || rawData.placement || 'unknown',
          website: website || 'americanhairline', // Default to americanhairline
          
          original_params: {
            ...params,
            campaign: params.campaign || rawData.campaign || 'unknown',
            medium: params.medium || rawData.medium || 'fb_ads',
            source: params.source || rawData.source || 'facebook',
            content: params.content || rawData.content || 'unknown',
            placement: params.placement || rawData.placement || 'unknown'
          },
          
          click_time: admin.firestore.FieldValue.serverTimestamp()
        };

        // Select appropriate database and collection based on website
        const targetDb = getDatabaseForWebsite(utmData.website);
        const targetCollection = getCollectionForWebsite(utmData.website);
        console.log(`Storing click in ${utmData.website} database`);

        try {
          await targetDb.runTransaction(async (transaction) => {
            const docRef = targetCollection.doc(session_id);
            const doc = await transaction.get(docRef);
            
            if (!doc.exists) {
              transaction.set(docRef, {
                ...utmData,
                timestamp: admin.firestore.FieldValue.serverTimestamp(),
                hasEngaged: false,
                syncedToSheets: false
              });
            }
          });
        } catch (transactionError) {
          console.error('Transaction failed:', transactionError);
          throw transactionError;
        }
        
        res.status(201).json({ 
          message: 'Click stored',
          session_id: session_id,
          website: utmData.website,
          database: utmData.website === 'alchemane' ? 'alchemane-utm-tracker-db' : 'default'
        });
      } catch (err) {
        console.error('Storage error:', err);
        res.status(500).json({ error: 'Database operation failed' });
      }
    });

    // Readiness endpoint
    app.get('/readiness', async (req, res) => {
      try {
        await americanHairlineDb.listCollections();
        await alchemaneDb.listCollections();
        res.status(200).json({ 
          status: 'ready',
          databases: ['default', 'alchemane-utm-tracker-db']
        });
      } catch (err) {
        res.status(500).json({ error: 'Not ready' });
      }
    });

    // Initialize Google Sheets sync
    const { scheduledSync, setupRealtimeSync } = require('./google-sheets-sync');
    
    // Setup scheduled sync endpoint
    app.post('/scheduled-sync', async (req, res) => {
      try {
        const result = await scheduledSync();
        res.status(200).json(result);
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });
    
    // Initialize real-time sync listener
    const unsubscribeSheetsSync = await setupRealtimeSync();
    
    // Cleanup on server shutdown
    process.on('SIGTERM', () => {
      console.log('Shutting down, cleaning up listeners...');
      if (unsubscribeSheetsSync) unsubscribeSheetsSync();
      server.close();
    });

    console.log('Async initialization completed');

  } catch (err) {
    console.error('Critical initialization error:', err);
    server.close(() => process.exit(1));
  }
});

module.exports = app;
