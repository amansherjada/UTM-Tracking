const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const admin = require('firebase-admin');
const { SecretManagerServiceClient } = require('@google-cloud/secret-manager');
const fs = require('fs/promises');
const secretClient = new SecretManagerServiceClient();

const app = express();
const PORT = process.env.PORT || 8080; // Fallback for local development

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

    // Load service account asynchronously
    const credsPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;
    const serviceAccount = JSON.parse(await fs.readFile(credsPath));
    
    // Initialize Firebase
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
      databaseURL: `https://${process.env.GCP_PROJECT_ID}.firebaseio.com`
    });

    // Configure Firestore with timeout
    const db = admin.firestore();
    db.settings({
      databaseId: 'utm-tracker-db',
      timeout: 10000
    });

    // Non-blocking connection check
    db.listCollections()
      .then(() => console.log('Firestore connection verified'))
      .catch(err => console.error('Firestore connection warning:', err));

    const clicksCollection = db.collection('utmClicks');

    // Readiness endpoint
    app.get('/readiness', async (req, res) => {
      try {
        await Promise.race([
          db.listCollections(),
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error('Connection timeout')), 15000)
          )
        ]);
        res.status(200).json({ status: 'ready' });
      } catch (err) {
        console.error('Readiness check failed:', err);
        res.status(500).json({ error: err.message });
      }
    });

    // Secret manager helper
    async function getSecret(secretName) {
      const name = `projects/${process.env.GCP_PROJECT_ID}/secrets/${secretName}/versions/latest`;
      const [version] = await secretClient.accessSecretVersion({ name });
      return version.payload.data.toString('utf8');
    }

    // Security middleware
    const verifyGallabox = async (req, res, next) => {
      try {
        const token = req.headers['x-gallabox-token'];
        const gallaboxToken = await getSecret('gallabox-token');
        
        if (!token || token !== gallaboxToken) {
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
        if (!session_id) return res.status(400).json({ error: 'Session ID required' });

        await clicksCollection.doc(session_id).set({
          ...utmData,
          timestamp: admin.firestore.FieldValue.serverTimestamp(),
          hasEngaged: false,
          syncedToSheets: false
        });

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
        if (event.event !== 'Message.received') return res.status(200).json({ status: 'ignored' });

        let sessionId;
        if (event.context) {
          try {
            const context = JSON.parse(Buffer.from(event.context, 'base64').toString());
            sessionId = context?.session_id;
          } catch (err) {
            return res.status(400).json({ error: 'Invalid context' });
          }
        }

        if (!sessionId) return res.status(400).json({ error: 'Missing session ID' });

        const docRef = clicksCollection.doc(sessionId);
        const doc = await docRef.get();
        if (!doc.exists) return res.status(404).json({ error: 'Session not found' });

        await docRef.update({
          hasEngaged: true,
          phoneNumber: event.sender,
          lastMessage: event.text,
          engagedAt: admin.firestore.FieldValue.serverTimestamp()
        });

        res.status(200).json({ status: 'updated', sessionId });
      } catch (err) {
        console.error('Webhook error:', err);
        res.status(500).json({ error: 'Processing failed' });
      }
    });

    // Background services
    const { scheduledSync } = await import('./google-sheets-sync.js');
    await scheduledSync();
    console.log('Background services initialized');

    console.log('Async initialization completed');
    console.log('All endpoints available:');
    console.log(`- http://0.0.0.0:${PORT}/readiness`);
    console.log(`- http://0.0.0.0:${PORT}/store-click`);
    console.log(`- http://0.0.0.0:${PORT}/gallabox-webhook`);

  } catch (err) {
    console.error('Async initialization error:', err);
  }
});

module.exports = app;
