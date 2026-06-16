const express = require('express');
const { WebSocketServer } = require('ws');
const http = require('http');
const path = require('path');
const crypto = require('crypto');
const { MongoClient } = require('mongodb');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.static(path.join(__dirname, 'public')));

const MAX_MESSAGES = 500;
const clients = new Map();

const USER_COLORS = [
  '#4f9ef8', '#f87171', '#34d399', '#fbbf24',
  '#a78bfa', '#f472b6', '#38bdf8', '#fb923c'
];

// MongoDB setup
const MONGODB_URI = process.env.MONGODB_URI;
let db = null;

async function connectDB() {
  if (!MONGODB_URI) {
    console.warn('No MONGODB_URI set — using in-memory storage (messages lost on restart)');
    return;
  }
  try {
    const client = new MongoClient(MONGODB_URI);
    await client.connect();
    db = client.db('chatapp');
    await db.collection('messages').createIndex({ timestamp: 1 });
    console.log('Connected to MongoDB');
  } catch (err) {
    console.error('MongoDB connection failed:', err.message);
  }
}

async function getHistory() {
  if (!db) return [];
  return db.collection('messages')
    .find({}, { projection: { _id: 0 } })
    .sort({ timestamp: -1 })
    .limit(MAX_MESSAGES)
    .toArray()
    .then(msgs => msgs.reverse());
}

async function saveMessage(message) {
  if (!db) return;
  await db.collection('messages').insertOne({ ...message });
  // Keep only last MAX_MESSAGES
  const count = await db.collection('messages').countDocuments();
  if (count > MAX_MESSAGES) {
    const oldest = await db.collection('messages')
      .find({}, { projection: { _id: 1 } })
      .sort({ timestamp: 1 })
      .limit(count - MAX_MESSAGES)
      .toArray();
    const ids = oldest.map(d => d._id);
    await db.collection('messages').deleteMany({ _id: { $in: ids } });
  }
}

function generateId() {
  return crypto.randomBytes(6).toString('hex');
}

function broadcast(data, exclude = null) {
  const payload = JSON.stringify(data);
  for (const [ws] of clients) {
    if (ws !== exclude && ws.readyState === 1) ws.send(payload);
  }
}

function broadcastAll(data) { broadcast(data); }

wss.on('connection', (ws) => {
  const clientId = generateId();
  const colorIndex = clients.size % USER_COLORS.length;
  const client = { id: clientId, username: null, color: USER_COLORS[colorIndex] };
  clients.set(ws, client);

  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {
      case 'join': {
        const username = (msg.username || '').trim().slice(0, 32) || `User${clientId.slice(0, 4)}`;
        client.username = username;

        // Send persisted history
        const history = await getHistory();
        ws.send(JSON.stringify({ type: 'history', messages: history }));

        const userList = [...clients.values()]
          .filter(c => c.username)
          .map(c => ({ id: c.id, username: c.username, color: c.color }));
        broadcastAll({ type: 'users', users: userList });

        broadcast({ type: 'system', text: `${username} joined the chat`, timestamp: Date.now() }, ws);
        break;
      }

      case 'message': {
        if (!client.username) return;
        const text = (msg.text || '').trim().slice(0, 2000);
        if (!text) return;

        const message = {
          id: generateId(),
          type: 'message',
          authorId: client.id,
          author: client.username,
          color: client.color,
          text,
          timestamp: Date.now(),
          replyTo: msg.replyTo || null
        };

        await saveMessage(message);
        broadcastAll({ type: 'message', message });
        break;
      }

      case 'typing': {
        if (!client.username) return;
        broadcast({ type: 'typing', authorId: client.id, author: client.username, isTyping: !!msg.isTyping }, ws);
        break;
      }
    }
  });

  ws.on('close', () => {
    const username = client.username;
    clients.delete(ws);
    if (username) {
      const userList = [...clients.values()]
        .filter(c => c.username)
        .map(c => ({ id: c.id, username: c.username, color: c.color }));
      broadcastAll({ type: 'users', users: userList });
      broadcastAll({ type: 'system', text: `${username} left the chat`, timestamp: Date.now() });
    }
  });

  ws.on('error', () => clients.delete(ws));
});

const PORT = process.env.PORT || 3000;

connectDB().then(() => {
  server.listen(PORT, () => console.log(`Chat server running at http://localhost:${PORT}`));
});
