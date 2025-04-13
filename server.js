const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const admin = require('firebase-admin');
const { SecretManagerServiceClient } = require('@google-cloud/secret-manager');
const fs = require('fs/promises');
const secretClient = new SecretManagerServiceClient();
require('dotenv').config();

const app = express();
// const PORT = process.env.PORT;
const PORT = process.env.PORT || 8080; // Cloud Run default port

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

// Essential middleware (non-blocking)
app.use(cors());
app.use(helmet());
app.use(express.json());

// Immediate health endpoint
app.get('/health', (req, res) => {
  res.status(200).json({
    status: 'healthy',
    timestamp: new Date().toISOString()
  });
});

// Deferred heavy initialization
setImmediate(async () => {
  try {
    console.log('Starting async initialization...');

    // Load service account asynchronously with error handling
    const credsPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;
    console.log(`Loading credentials from: ${credsPath}`);
    
    const serviceAccount = JSON.parse(await fs.readFile(credsPath));
    console.log('Credentials loaded successfully');

    // Initialize Firebase
    console.log('Initializing Firebase...');
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
      databaseURL: `https://${process.env.GCP_PROJECT_ID}.firebaseio.com`
    });
    console.log('Firebase initialized');

    // Configure Firestore with timeout
    const db = admin.firestore();
    db.settings({
      databaseId: 'utm-tracker-db',
      timeout: 10000
    });

    // Non-blocking connection check with retries
    let firestoreConnected = false;
    const maxRetries = 3;
    
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        console.log(`Firestore connection attempt ${attempt}/${maxRetries}`);
        await db.listCollections();
        firestoreConnected = true;
        break;
      } catch (err) {
        console.error(`Firestore connection warning (attempt ${attempt}):`, err);
        await new Promise(resolve => setTimeout(resolve, 2000));
      }
    }

    if (!firestoreConnected) {
      throw new Error('Failed to connect to Firestore after multiple attempts');
    }

    const clicksCollection = db.collection('utmClicks');
    console.log('Firestore connection verified');

    // Readiness endpoint
    app.get('/readiness', async (req, res) => {
      try {
        await db.listCollections();
        res.status(200).json({ status: 'ready' });
      } catch (err) {
        console.error('Readiness check failed:', err);
        res.status(500).json({ error: err.message });
      }
    });

    // Secret manager helper with caching
    const secretCache = new Map();
    
    async function getSecret(secretName) {
      if (secretCache.has(secretName)) {
        return secretCache.get(secretName);
      }

      try {
        // First try to read from mounted secret if available
        if (secretName === 'gallabox-token' && process.env.GALLABOX_TOKEN) {
          secretCache.set(secretName, process.env.GALLABOX_TOKEN);
          return process.env.GALLABOX_TOKEN;
        }
        
        // Otherwise use Secret Manager
        const name = `projects/${process.env.GCP_PROJECT_ID}/secrets/${secretName}/versions/latest`;
        const [version] = await secretClient.accessSecretVersion({ name });
        const secretValue = version.payload.data.toString('utf8');
        secretCache.set(secretName, secretValue);
        
        return secretValue;
      } catch (err) {
        console.error(`Error retrieving secret ${secretName}:`, err);
        throw err;
      }
    }

    // Security middleware
    const verifyGallabox = async (req, res, next) => {
      try {
        const token = req.headers['x-gallabox-token'];
        if (!token) {
          return res.status(401).json({ error: 'Missing authentication token' });
        }

        const gallaboxToken = await getSecret('gallabox-token');
        
        if (token !== gallaboxToken) {
          console.warn('Invalid token received');
          return res.status(401).json({ error: 'Unauthorized' });
        }
        
        next();
      } catch (error) {
        console.error('Token verification error:', error);
        res.status(500).json({ error: 'Authentication failed' });
      }
    };

    // Data endpoints
    app.post('/store-click', async (req, res) => {
      try {
        const { session_id, ...utmData } = req.body;
        if (!session_id) {
          return res.status(400).json({ error: 'Session ID required' });
        }

        const docRef = clicksCollection.doc(session_id);
        await docRef.set({
          ...utmData,
          timestamp: admin.firestore.FieldValue.serverTimestamp(),
          hasEngaged: false,
          syncedToSheets: false
        });

        console.log(`Click stored for session: ${session_id}`);
        res.status(201).json({ message: 'Click stored', session_id });
      } catch (err) {
        console.error('Storage error:', err);
        res.status(500).json({ error: 'Database operation failed' });
      }
    });

    // Webhook endpoint
    app.post('/gallabox-webhook', verifyGallabox, async (req, res) => {
      try {
        const event = req.body;
        if (event.event !== 'Message.received') {
          return res.status(200).json({ status: 'ignored' });
        }

        let sessionId;
        if (event.context) {
          try {
            const context = JSON.parse(Buffer.from(event.context, 'base64').toString());
            sessionId = context?.session_id;
          } catch (err) {
            console.warn('Invalid context format received');
            return res.status(400).json({ error: 'Invalid context' });
          }
        }

        if (!sessionId) {
          return res.status(400).json({ error: 'Missing session ID' });
        }

        const docRef = clicksCollection.doc(sessionId);
        const doc = await docRef.get();
        
        if (!doc.exists) {
          console.warn(`Session not found: ${sessionId}`);
          return res.status(404).json({ error: 'Session not found' });
        }

        await docRef.update({
          hasEngaged: true,
          phoneNumber: event.sender,
          lastMessage: event.text,
          engagedAt: admin.firestore.FieldValue.serverTimestamp()
        });

        console.log(`Webhook processed for session: ${sessionId}`);
        res.status(200).json({ status: 'updated', sessionId });
      } catch (err) {
        console.error('Webhook error:', err);
        res.status(500).json({ error: 'Processing failed' });
      }
    });

    // Background services
    const { scheduledSync } = await import('./google-sheets-sync.js');
    await scheduledSync();

    app.post('/test-scheduled-sync', async (req, res) => {
      try {
        console.log('Triggering scheduledSync...');
        const result = await scheduledSync();
        res.status(200).json({
          message: 'Scheduled sync executed successfully',
          result
        });
      } catch (err) {
        console.error('Error during scheduledSync:', err);
        res.status(500).json({ error: err.message });
      }
    });

    console.log('Background services initialized');

    console.log('Async initialization completed');
    console.log('All endpoints available:');
    console.log(`- http://0.0.0.0:${PORT}/readiness`);
    console.log(`- http://0.0.0.0:${PORT}/store-click`);
    console.log(`- http://0.0.0.0:${PORT}/gallabox-webhook`);

  } catch (err) {
    console.error('Critical initialization error:', err);
    // Graceful shutdown on fatal errors
    server.close(() => {
      console.log('Server closed due to initialization failure');
      process.exit(1);
    });
  }
});

module.exports = app;
