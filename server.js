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

// clients: Map<email, ws>
const clients = new Map();

const USER_COLORS = [
  '#4f9ef8', '#f87171', '#34d399', '#fbbf24',
  '#a78bfa', '#f472b6', '#38bdf8', '#fb923c'
];

function colorForEmail(email) {
  let hash = 0;
  for (const c of email) hash = (hash * 31 + c.charCodeAt(0)) & 0xffffffff;
  return USER_COLORS[Math.abs(hash) % USER_COLORS.length];
}

function generateId() {
  return crypto.randomBytes(6).toString('hex');
}

// ─── MongoDB ──────────────────────────────────────────────────────────────────
const MONGODB_URI = process.env.MONGODB_URI;
let db = null;

async function connectDB() {
  if (!MONGODB_URI) {
    console.warn('No MONGODB_URI — messages will not persist');
    return;
  }
  try {
    const client = new MongoClient(MONGODB_URI);
    await client.connect();
    db = client.db('chatapp');
    await db.collection('messages').createIndex({ from: 1, to: 1, timestamp: 1 });
    await db.collection('messages').createIndex({ to: 1, from: 1, timestamp: 1 });
    await db.collection('messages').createIndex({ id: 1 });
    console.log('Connected to MongoDB');
  } catch (err) {
    console.error('MongoDB error:', err.message);
  }
}

// Get conversation history between two users (paginated, newest-first fetch then reversed)
async function getHistory(emailA, emailB, before = null, limit = 50) {
  if (!db) return { messages: [], hasMore: false };
  const query = {
    $or: [
      { from: emailA, to: emailB },
      { from: emailB, to: emailA }
    ]
  };
  if (before) query.timestamp = { $lt: before };

  // Fetch one extra to know if there are older messages
  const rows = await db.collection('messages')
    .find(query, { projection: { _id: 0 } })
    .sort({ timestamp: -1 })
    .limit(limit + 1)
    .toArray();

  const hasMore = rows.length > limit;
  if (hasMore) rows.pop();
  return { messages: rows.reverse(), hasMore };
}

// Get recent conversations for a user (last message per contact)
async function getConversations(email) {
  if (!db) return [];
  const msgs = await db.collection('messages').aggregate([
    { $match: { $or: [{ from: email }, { to: email }] } },
    { $sort: { timestamp: -1 } },
    {
      $group: {
        _id: {
          $cond: [{ $eq: ['$from', email] }, '$to', '$from']
        },
        lastMessage: { $first: '$$ROOT' }
      }
    },
    { $sort: { 'lastMessage.timestamp': -1 } },
    { $limit: 50 }
  ]).toArray();

  return msgs.map(m => ({
    with: m._id,
    lastMessage: {
      text: m.lastMessage.text,
      from: m.lastMessage.from,
      timestamp: m.lastMessage.timestamp
    }
  }));
}

async function saveMessage(msg) {
  if (!db) return;
  await db.collection('messages').insertOne({ ...msg });
}

// ─── WebSocket ────────────────────────────────────────────────────────────────
function sendOnlineUsers() {
  const online = [...clients.keys()];
  const payload = JSON.stringify({ type: 'online_users', users: online });
  for (const ws of clients.values()) {
    if (ws.readyState === 1) ws.send(payload);
  }
}

function sendTo(email, data) {
  const ws = clients.get(email);
  if (ws && ws.readyState === 1) {
    ws.send(JSON.stringify(data));
    return true;
  }
  return false;
}

wss.on('connection', (ws) => {
  let myEmail = null;

  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    try {

    switch (msg.type) {

      case 'join': {
        const email = (msg.email || '').trim().toLowerCase().slice(0, 200);
        if (!email || !email.includes('@')) return;
        myEmail = email;

        // Disconnect any previous session for this email
        const existing = clients.get(email);
        if (existing && existing !== ws) existing.close();
        clients.set(email, ws);

        // Send their recent conversations
        const conversations = await getConversations(email);
        ws.send(JSON.stringify({ type: 'conversations', conversations }));

        // Broadcast updated online list
        sendOnlineUsers();
        break;
      }

      case 'get_history': {
        if (!myEmail) return;
        const withEmail = (msg.with || '').trim().toLowerCase();
        if (!withEmail) return;
        const before = msg.before ? Number(msg.before) : null;
        const { messages, hasMore } = await getHistory(myEmail, withEmail, before);
        ws.send(JSON.stringify({ type: 'history', with: withEmail, messages, hasMore, isPagination: !!before }));
        break;
      }

      case 'message': {
        if (!myEmail) return;
        const to = (msg.to || '').trim().toLowerCase();
        const text = (msg.text || '').trim().slice(0, 2000);
        if (!to || !text) return;

        const message = {
          id: generateId(),
          from: myEmail,
          to,
          text,
          timestamp: Date.now(),
          replyTo: msg.replyTo || null
        };

        await saveMessage(message);

        // Send to recipient if online
        sendTo(to, { type: 'message', message });

        // Echo back to sender
        ws.send(JSON.stringify({ type: 'message', message }));
        break;
      }

      case 'edit_message': {
        if (!myEmail) return;
        const { id: editId, text: newText } = msg;
        const trimmed = (newText || '').trim().slice(0, 2000);
        if (!editId || !trimmed) return;
        if (!db) return;
        // Only allow editing own messages
        const original = await db.collection('messages').findOne({ id: editId, from: myEmail }, { projection: { _id: 0 } });
        if (!original) return;
        await db.collection('messages').updateOne({ id: editId }, { $set: { text: trimmed, editedAt: Date.now() } });
        const edited = { ...original, text: trimmed, editedAt: Date.now() };
        ws.send(JSON.stringify({ type: 'message_edited', message: edited }));
        sendTo(original.to, { type: 'message_edited', message: edited });
        break;
      }

      case 'typing': {
        if (!myEmail) return;
        const to = (msg.to || '').trim().toLowerCase();
        sendTo(to, { type: 'typing', from: myEmail, isTyping: !!msg.isTyping });
        break;
      }

      case 'search_user': {
        const email = (msg.email || '').trim().toLowerCase();
        const online = clients.has(email);
        // Check if they exist in message history
        let known = false;
        if (db) {
          const count = await db.collection('messages').countDocuments({
            $or: [{ from: email }, { to: email }]
          });
          known = count > 0;
        }
        ws.send(JSON.stringify({ type: 'search_result', email, online, known }));
        break;
      }
    }
    } catch (err) {
      console.error(`[WS error] type=${msg?.type}:`, err.message);
      try { ws.send(JSON.stringify({ type: 'error', code: msg?.type, message: 'Server error — please retry' })); } catch {}
    }
  });

  ws.on('close', () => {
    if (myEmail && clients.get(myEmail) === ws) {
      clients.delete(myEmail);
      sendOnlineUsers();
    }
  });

  ws.on('error', () => {
    if (myEmail && clients.get(myEmail) === ws) {
      clients.delete(myEmail);
    }
  });
});

const PORT = process.env.PORT || 3000;
connectDB().then(() => {
  server.listen(PORT, () => console.log(`Chat server running at http://localhost:${PORT}`));
});
