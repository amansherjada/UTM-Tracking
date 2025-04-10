const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const admin = require('firebase-admin');
const {SecretManagerServiceClient} = require('@google-cloud/secret-manager');
const secretClient = new SecretManagerServiceClient();
require('dotenv').config();

// Helper function to access secrets
async function getSecret(secretName) {
  const projectId = process.env.GCP_PROJECT_ID;
  const name = `projects/${projectId}/secrets/${secretName}/versions/latest`;
  
  try {
    const [version] = await secretClient.accessSecretVersion({name});
    return version.payload.data.toString('utf8');
  } catch (error) {
    console.error(`Error accessing secret ${secretName}:`, error);
    throw error;
  }
}

const app = express();
const PORT = process.env.PORT || 8080;

// Middleware
app.use(cors());
app.use(helmet());
app.use(express.json());

// Health endpoints
app.get('/health', (req, res) => {
  res.status(200).json({ 
    status: 'healthy',
    timestamp: new Date().toISOString()
  });
});

console.log('Starting server initialization...');

// Firebase Admin initialization with timeout
try {
  console.log('Loading service account...');
  const serviceAccount = require(process.env.GOOGLE_APPLICATION_CREDENTIALS);
  
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    databaseURL: `https://${process.env.GCP_PROJECT_ID}.firebaseio.com`
  });

  // Firestore configuration with connection timeout
  const db = admin.firestore();
  db.settings({ 
    databaseId: 'utm-tracker-db',
    timeout: 10000 // 10-second operation timeout
  });

  const clicksCollection = db.collection('utmClicks');

  // Async readiness check with timeout
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

  // Gallabox verification middleware
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

  // Store UTM click endpoint
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

  // Webhook handler with async context parsing
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

  // Async server starter with timeout handling
  const startServer = async () => {
    try {
      console.log('Testing Firestore connection...');
      await Promise.race([
        db.listCollections(),
        new Promise((_, reject) => 
          setTimeout(() => reject(new Error('Firestore connection timeout')), 15000)
        )
      ]);

      // Start HTTP server
      const server = app.listen(PORT, '0.0.0.0', () => {
        console.log(`Server running on port ${PORT}`);
        console.log('Available endpoints:');
        console.log(`- http://0.0.0.0:${PORT}/health`);
        console.log(`- http://0.0.0.0:${PORT}/readiness`);
        console.log(`- http://0.0.0.0:${PORT}/store-click`);
        console.log(`- http://0.0.0.0:${PORT}/gallabox-webhook`);
        
        // Deferred background services
        initializeBackgroundServices().catch(console.error);
      });

      // Handle server errors
      server.on('error', err => {
        console.error('Server error:', err);
        process.exit(1);
      });

    } catch (err) {
      console.error('Startup failed:', err);
      process.exit(1);
    }
  };

  // Background services initializer
  async function initializeBackgroundServices() {
    try {
      const { scheduledSync } = await import('./google-sheets-sync.js');
      await scheduledSync();
      console.log('Background services initialized');
    } catch (err) {
      console.error('Background services error:', err);
    }
  }

  // Start production server
  if (process.env.NODE_ENV !== 'test') {
    startServer();
  }

} catch (initError) {
  console.error('FATAL: Initialization error:', initError);
  process.exit(1);
}

module.exports = app;