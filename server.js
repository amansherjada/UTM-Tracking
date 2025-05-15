const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const admin = require('firebase-admin');
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
    
    // Initialize Firebase
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
      databaseURL: `https://${process.env.GCP_PROJECT_ID}.firebaseio.com`,
    });

    const db = admin.firestore();
    db.settings({ 
      databaseId: 'utm-tracker-db',
      timeout: 10000,
    });

    // Verify Firestore connection
    let firestoreConnected = false;
    const maxRetries = 3;
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        await db.listCollections();
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

    const clicksCollection = db.collection('utmClicks');
    console.log('Firestore connected successfully');

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

        // Check if this is for the American Hairline number
        const receivingNumber = (event.channelNumber || '').replace(/\D/g, '');
        const americanHairlineNumber = '919137279145';
        console.log('DEBUG: channelNumber received:', receivingNumber);
        if (receivingNumber !== americanHairlineNumber) {
          console.log(`Skipping: Message was for ${receivingNumber}, not American Hairline`);
          return res.status(200).json({ status: 'skipped', reason: 'wrong_number' });
        }
        // Extract critical identifiers
        const senderPhone = event.whatsapp?.from?.replace(/^0+/, '') || '';
        const contactId = event.contactId || event.contact?.id || null;
        const conversationId = event.conversationId || null;
        const contactName = event.contact?.name || null;
        const messageContent = event.whatsapp?.text?.body || (event.whatsapp?.interactive?.list_reply?.title || 'No text content');
        
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
        
        // Matching Priority 1: Context Parameter
        if (event.context) {
          try {
            const context = JSON.parse(Buffer.from(event.context, 'base64').toString());
            if (context?.session_id) {
              sessionId = context.session_id;
              utmData = context;
              attribution = 'context';
              console.log(`Context match: ${sessionId}`);
            }
          } catch (err) {
            console.warn('Invalid context format:', err);
          }
        }

        // Matching Priority 2: Gallabox Identifiers
        if (!sessionId && (contactId || conversationId)) {
          console.log(`Attempting Gallabox ID match - Contact: ${contactId}, Conversation: ${conversationId}`);
          
          const fiveMinutesAgo = admin.firestore.Timestamp.fromDate(
            new Date(Date.now() - 5 * 60 * 1000)
          );

          const query = clicksCollection
            .where('hasEngaged', '==', false)
            .where('timestamp', '>=', fiveMinutesAgo)
            .orderBy('timestamp', 'desc')
            .limit(5);

          const snapshot = await query.get();

          if (!snapshot.empty) {
            sessionId = snapshot.docs[0].id;
            utmData = snapshot.docs[0].data();
            attribution = 'gallabox_id_match';
            console.log(`Matched with recent click: ${sessionId}`);

            // Update click record with Gallabox IDs
            await clicksCollection.doc(sessionId).update({
              contactId,
              conversationId,
              contactName: contactName || null
            });
          }
        }

        // Matching Priority 3: Phone Number (if available in click records)
        if (!sessionId) {
          const phoneMatch = await clicksCollection
            .where('phoneNumber', '==', normalizedPhone)
            .where('hasEngaged', '==', false)
            .orderBy('timestamp', 'desc')
            .limit(1)
            .get();

          if (!phoneMatch.empty) {
            sessionId = phoneMatch.docs[0].id;
            utmData = phoneMatch.docs[0].data();
            attribution = 'phone_match';
            console.log(`Phone number match: ${sessionId}`);
          }
        }

        // Start of Modified Direct Message Handling
        if (!sessionId) {
          if (conversationId) {
            const existingDirectQuery = await clicksCollection
              .where('conversationId', '==', conversationId)
              .where('source', '==', 'direct_message')
              .limit(1)
              .get();

            if (!existingDirectQuery.empty) {
              sessionId = existingDirectQuery.docs[0].id;
              attribution = 'existing_direct';
              console.log(`Found existing direct conversation: ${conversationId}`);

              await clicksCollection.doc(sessionId).update({
                lastMessage: messageContent,
                engagedAt: admin.firestore.FieldValue.serverTimestamp()
              });
            } else if (process.env.STORE_DIRECT_MESSAGES === 'true') {
              sessionId = `direct-${Date.now()}-${crypto.randomUUID().slice(0, 8)}`;
              attribution = 'new_direct';
              console.log(`Creating new direct record: ${sessionId}`);
              
              await clicksCollection.doc(sessionId).set({
                ...utmData,
                timestamp: admin.firestore.FieldValue.serverTimestamp(),
                hasEngaged: true,
                phoneNumber: normalizedPhone,
                lastMessage: messageContent,
                engagedAt: admin.firestore.FieldValue.serverTimestamp(),
                syncedToSheets: false,
                contactId,
                conversationId,
                contactName
              });
            } else {
              console.log(`Skipping direct message from ${normalizedPhone}`);
              attribution = 'ignored_direct';
              sessionId = 'not_stored';
            }
          }
        }
        // End of Modified Direct Message Handling

        // Update existing records
        if (attribution !== 'new_direct' && sessionId !== 'not_stored') {
          const updateData = {
            hasEngaged: true,
            phoneNumber: normalizedPhone,
            engagedAt: admin.firestore.FieldValue.serverTimestamp(),
            syncedToSheets: false,
            attribution_source: attribution,
            contactId,
            conversationId,
            ...(contactName && { contactName }),
            ...(messageContent && { lastMessage: messageContent })
          };

          await db.runTransaction(async (transaction) => {
            const docRef = clicksCollection.doc(sessionId);
            const doc = await transaction.get(docRef);
            
            if (doc.exists) {
              transaction.update(docRef, updateData);
            } else {
              transaction.set(docRef, {
                ...utmData,
                ...updateData,
                timestamp: admin.firestore.FieldValue.serverTimestamp()
              });
            }
          });
        }

        console.log(`Processed message from ${normalizedPhone} with attribution: ${attribution}`);
        res.status(200).json({ 
          status: 'processed',
          sessionId,
          source: utmData.source,
          attribution
        });

      } catch (err) {
        console.error('Webhook processing error:', err);
        res.status(500).json({ 
          error: 'Processing failed',
          details: err.message
        });
      }
    });

    app.post('/store-click', async (req, res) => {
      try {
        const { session_id, original_params, ...rawData } = req.body;
        
        // Extract values with consistent naming
        const params = original_params || {};
        
        // Create standardized structure
        const utmData = {
          source: params.source || rawData.source || 'facebook',
          medium: params.medium || rawData.medium || 'fb_ads',
          campaign: params.campaign || rawData.campaign || 'unknown',
          content: params.content || rawData.content || 'unknown',
          placement: params.placement || rawData.placement || 'unknown',
          
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

        await db.runTransaction(async (transaction) => {
          const docRef = clicksCollection.doc(session_id);
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
        
        res.status(201).json({ 
          message: 'Click stored',
          session_id: session_id
        });
      } catch (err) {
        console.error('Storage error:', err);
        res.status(500).json({ error: 'Database operation failed' });
      }
    });

    // Readiness endpoint
    app.get('/readiness', async (req, res) => {
      try {
        await db.listCollections();
        res.status(200).json({ status: 'ready' });
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
