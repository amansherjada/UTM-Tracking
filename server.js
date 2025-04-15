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

    // ********************************************
    // UPDATED GALABOX WEBHOOK HANDLER STARTS HERE
    // ********************************************
    app.post('/gallabox-webhook', verifyGallabox, async (req, res) => {
      try {
        const event = req.body;
        
        // Extract message details
        const messageContent = event.whatsapp?.text?.body || 
                            (event.whatsapp?.image ? 'Image message' : 'No text content');
        const senderPhone = event.whatsapp?.from || event.sender;
        const contactName = event.contact?.name || null;
        
        if (!senderPhone) {
          console.warn('Missing phone number in webhook payload');
          return res.status(400).json({ error: 'Missing phone number' });
        }

        // Initialize tracking variables
        let sessionId;
        let utmData = {
          source: 'direct_message',
          medium: 'whatsapp',
          campaign: 'organic',
          content: 'none'
        };
        let attribution = 'direct';

        console.log(`Processing message from ${senderPhone}`);

        // STEP 1: Try to use context parameter from WhatsApp
        if (event.context) {
          try {
            const context = JSON.parse(Buffer.from(event.context, 'base64').toString());
            if (context?.session_id) {
              sessionId = context.session_id;
              utmData = context;
              attribution = 'context';
              console.log(`Found context with session ID: ${sessionId}`);
            }
          } catch (err) {
            console.warn('Invalid context format:', err);
          }
        }

        // STEP 2: Check for existing records with matching phone
        if (!sessionId) {
          console.log(`No context found, searching for records with phone: ${senderPhone}`);
          
          const existingPhoneQuery = await clicksCollection
            .where('phoneNumber', '==', senderPhone)
            .where('hasEngaged', '==', false)
            .orderBy('timestamp', 'desc')
            .limit(1)
            .get();
          
          if (!existingPhoneQuery.empty) {
            sessionId = existingPhoneQuery.docs[0].id;
            utmData = existingPhoneQuery.docs[0].data();
            attribution = 'phone_match';
            console.log(`Found existing record with matching phone: ${sessionId}`);
          } else {
            // STEP 3: Create new direct message record
            sessionId = `direct-${Date.now()}-${crypto.randomUUID().slice(0, 8)}`;
            attribution = 'new_direct';
            console.log(`Creating new direct message session: ${sessionId}`);
            
            await clicksCollection.doc(sessionId).set({
              ...utmData,
              timestamp: admin.firestore.FieldValue.serverTimestamp(),
              hasEngaged: true,
              phoneNumber: senderPhone,
              lastMessage: messageContent,
              engagedAt: admin.firestore.FieldValue.serverTimestamp(),
              syncedToSheets: false,
              contactName: contactName,
              attribution_source: attribution
            });
          }
        }

        // STEP 4: Update existing record if needed
        if (attribution !== 'new_direct') {
          const updateData = {
            hasEngaged: true,
            phoneNumber: senderPhone,
            engagedAt: admin.firestore.FieldValue.serverTimestamp(),
            syncedToSheets: false,
            attribution_source: attribution
          };

          if (contactName) {
            updateData.contactName = contactName;
          }

          if (messageContent && messageContent !== 'No text content') {
            updateData.lastMessage = messageContent;
          }

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

        console.log(`Processed message from ${senderPhone}, session: ${sessionId}, attribution: ${attribution}`);
        res.status(200).json({ 
          status: 'processed',
          sessionId,
          source: utmData.source,
          attribution: attribution
        });

      } catch (err) {
        console.error('Webhook processing error:', err);
        res.status(500).json({ 
          error: 'Processing failed',
          details: err.message
        });
      }
    });
    // ******************************************
    // UPDATED GALABOX WEBHOOK HANDLER ENDS HERE
    // ******************************************

    // Click storage endpoint
    app.post('/store-click', async (req, res) => {
      try {
        const { session_id, ...utmData } = req.body;
        
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
    const { scheduledSync } = require('./google-sheets-sync');
    app.post('/scheduled-sync', async (req, res) => {
      try {
        const result = await scheduledSync();
        res.status(200).json(result);
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    console.log('Async initialization completed');

  } catch (err) {
    console.error('Critical initialization error:', err);
    server.close(() => process.exit(1));
  }
});

module.exports = app;
