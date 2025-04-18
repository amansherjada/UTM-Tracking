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

    // Load cryptographic secret
    let SESSION_SECRET;
    const [secretVersion] = await secretClient.accessSecretVersion({
      name: `projects/${process.env.GCP_PROJECT_ID}/secrets/SESSION_SECRET/versions/latest`
    });
    SESSION_SECRET = secretVersion.payload.data.toString('utf8');

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

    //
    app.post('/gallabox-webhook', verifyGallabox, async (req, res) => {
      try {
        // Extract data from Gallabox payload structure
        const event = req.body.request?.data || req.body;
        const messageData = event.data || event;
        
        // Safely extract nested properties with defaults
        const whatsappInfo = messageData.whatsapp || {};
        const contactInfo = messageData.contact || {};
        const phone = whatsappInfo.from ? whatsappInfo.from.replace(/^0+/, '91') : null;
    
        if (!phone) return res.status(400).json({ error: 'Missing phone number' });
    
        await db.runTransaction(async (transaction) => {
          // 1. Try conversation ID match
          if (messageData.conversationId) {
            const convRef = clicksCollection.doc(`conv-${messageData.conversationId}`);
            const convDoc = await transaction.get(convRef);
            
            if (convDoc.exists) {
              transaction.update(convRef, {
                hasEngaged: true,
                engagedAt: FieldValue.serverTimestamp(),
                phoneNumber: phone,
                contactId: contactInfo.id,
                lastMessage: whatsappInfo.text?.body
              });
              return res.json({ status: 'conversation_matched' });
            }
          }
    
          // 2. Fallback: Phone + Contact ID match
          const contactQuery = await transaction.get(
            clicksCollection
              .where('phoneNumber', '==', phone)
              .where('contactId', '==', contactInfo.id)
              .limit(1)
          );
    
          if (!contactQuery.empty) {
            const doc = contactQuery.docs[0];
            transaction.update(doc.ref, {
              hasEngaged: true,
              engagedAt: FieldValue.serverTimestamp(),
              lastMessage: whatsappInfo.text?.body
            });
            return res.json({ status: 'contact_matched' });
          }
    
          // 3. Direct message fallback
          const directRef = directMessagesCollection.doc();
          transaction.set(directRef, {
            phoneNumber: phone,
            message: whatsappInfo.text?.body,
            timestamp: FieldValue.serverTimestamp()
          });
          return res.json({ status: 'direct_message' });
        });
    
      } catch (err) {
        console.error('Webhook error:', err);
        res.status(500).json({ 
          error: err.message,
          receivedBody: req.body // For debugging
        });
      }
    });

    // Store click endpoint
    app.post('/store-click', async (req, res) => {
      try {
        const { session_id, ...utmData } = req.body;
        
        // Validate session ID before proceeding
        if (!session_id || typeof session_id !== 'string' || session_id.trim() === '') {
          throw new Error('Invalid session ID: ' + JSON.stringify(session_id));
        }
    
        console.log('Storing UTM data for session:', session_id);
        
        await db.runTransaction(async (transaction) => {
          const docRef = clicksCollection.doc(session_id);
          const doc = await transaction.get(docRef);
          
          if (!doc.exists) {
            console.log('Creating new document for session:', session_id);
            transaction.set(docRef, {
              ...utmData,
              timestamp: FieldValue.serverTimestamp(),
              hasEngaged: false,
              syncedToSheets: false,
              phoneNumber: '' // Initialize empty for later update
            });
          }
        });
        
        res.status(201).json({ 
          message: 'Click stored successfully',
          session_id: session_id
        });
    
      } catch (err) {
        console.error('🔥 Storage error:', err.message, err.stack);
        res.status(500).json({ 
          error: 'Failed to store click data',
          details: err.message,
          receivedSessionId: req.body.session_id
        });
      }
    });

    // Readiness check
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
