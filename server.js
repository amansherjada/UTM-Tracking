const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const { Firestore, FieldValue } = require('@google-cloud/firestore');
const { SecretManagerServiceClient } = require('@google-cloud/secret-manager');
const crypto = require('crypto');
const fs = require('fs/promises');
const secretClient = new SecretManagerServiceClient();
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 8080;

// Immediate server startup
const server = app.listen(PORT, '0.0.0.0', () => {
  console.log(`Server listening on port ${PORT}`);
  console.log('Immediate endpoints available: /health');
});

// Error handling
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

// Middleware
app.use(cors());
app.use(helmet());
app.use(express.json());

// Health endpoint
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'healthy', timestamp: new Date().toISOString() });
});

// Deferred initialization
setImmediate(async () => {
  try {
    console.log('Starting async initialization...');

    // Initialize Firestore
    const db = new Firestore({
      projectId: process.env.GCP_PROJECT_ID,
      databaseId: 'utm-tracker-db',
      keyFilename: '/secrets/secrets'
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

    if (!firestoreConnected) throw new Error('Failed to connect to Firestore');
    console.log('Firestore connected successfully');

    // Collections
    const clicksCollection = db.collection('utmClicks');
    const directMessagesCollection = db.collection('directMessages');

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
        if (token !== gallaboxToken) return res.status(401).send('Invalid token');
        next();
      } catch (error) {
        console.error('Token verification error:', error);
        res.status(500).send('Authentication failed');
      }
    };

    // Webhook handler with session-based attribution (FIXED)
    app.post('/gallabox-webhook', verifyGallabox, async (req, res) => {
      try {
        const event = req.body;
        
        // Correct payload structure handling
        const messageData = event.data || event;
        const whatsappInfo = messageData.whatsapp || {};
        const contactInfo = messageData.contact || {};

        // Validate required fields
        if (!messageData.conversationId || !contactInfo.id) {
          return res.status(400).json({ error: 'Invalid message format' });
        }

        const senderPhone = whatsappInfo.from;
        const normalizedPhone = senderPhone ? senderPhone.replace(/^0+/, '91') : null;

        if (!normalizedPhone) {
          return res.status(400).json({ error: 'Missing phone number' });
        }

        let sessionId;
        const sessionIdsToCheck = [
          messageData.conversationId,
          normalizedPhone
        ];

        await db.runTransaction(async (transaction) => {
          // Check session sources
          for (const sid of sessionIdsToCheck.filter(Boolean)) {
            const docRef = clicksCollection.doc(sid);
            const doc = await transaction.get(docRef);
            
            if (doc.exists && !doc.data().hasEngaged) {
              sessionId = sid;
              transaction.update(docRef, {
                hasEngaged: true,
                phoneNumber: normalizedPhone,
                engagedAt: FieldValue.serverTimestamp(),
                contactId: contactInfo.id,
                conversationId: messageData.conversationId,
                contactName: contactInfo.name,
                lastMessage: whatsappInfo.text?.body
              });
              break;
            }
          }

          // Direct message fallback
          if (!sessionId) {
            const directRef = directMessagesCollection.doc();
            transaction.set(directRef, {
              phoneNumber: normalizedPhone,
              message: whatsappInfo.text?.body,
              timestamp: FieldValue.serverTimestamp()
            });
          }
        });

        res.status(200).json({ 
          status: 'processed',
          sessionId: sessionId || 'direct_message'
        });

      } catch (err) {
        console.error('Webhook error:', err);
        res.status(500).json({ error: err.message });
      }
    });

    // Store click endpoint (unchanged)
    app.post('/store-click', async (req, res) => {
      try {
        const { session_id, ...utmData } = req.body;
        
        await db.runTransaction(async (transaction) => {
          const docRef = clicksCollection.doc(session_id);
          const doc = await transaction.get(docRef);
          
          if (!doc.exists) {
            transaction.set(docRef, {
              ...utmData,
              timestamp: FieldValue.serverTimestamp(),
              hasEngaged: false,
              syncedToSheets: false
            });
          }
        });
        
        res.status(201).json({ message: 'Click stored', session_id });
      } catch (err) {
        console.error('Storage error:', err);
        res.status(500).json({ error: 'Database operation failed' });
      }
    });

    // Readiness check (unchanged)
    app.get('/readiness', async (req, res) => {
      try {
        await db.listCollections();
        res.status(200).json({ status: 'ready' });
      } catch (err) {
        res.status(500).json({ error: 'Not ready' });
      }
    });

    console.log('Async initialization completed');

  } catch (err) {
    console.error('Critical initialization error:', err);
    server.close(() => process.exit(1));
  }
});

module.exports = app;
