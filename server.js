// Relay متوافق مع شكل ntfy: نفس المسارات، فالكاشير وصفحة العميل
// بيتغير فيهم الرابط الأساسي بس.
const express = require('express');
const cors = require('cors');
const crypto = require('crypto');

const app = express();
app.use(cors());
app.use(express.text({ type: '*/*', limit: '1mb' }));

const KEEP_MS = 12 * 3600 * 1000;   // نحتفظ بالرسائل 12 ساعة
const MAX_MSGS = 300;               // أقصى عدد رسائل للجلسة
const MAX_TOPICS = 2000;
const topics = new Map();           // topic -> { msgs: [], clients: Set }

const okTopic = t => /^[A-Za-z0-9_-]{4,100}$/.test(t);
const getTopic = (name, create) => {
  let t = topics.get(name);
  if (!t && create) {
    if (topics.size >= MAX_TOPICS) return null;
    t = { msgs: [], clients: new Set() };
    topics.set(name, t);
  }
  return t;
};

function parseSince(s) {
  if (!s || s === 'none') return Infinity;       // مفيش backlog
  if (s === 'all') return 0;
  const m = /^(\d+)([smhd])$/.exec(s);
  if (m) return Date.now() / 1000 - Number(m[1]) * { s: 1, m: 60, h: 3600, d: 86400 }[m[2]];
  if (/^\d+$/.test(s)) return Number(s);
  return 0;
}
const backlog = (t, since) => t.msgs.filter(m => m.time >= since);

// نشر رسالة (الكاشير أو العميل)
app.post('/:topic', (req, res) => {
  const { topic } = req.params;
  if (!okTopic(topic)) return res.status(400).json({ error: 'bad topic' });
  const t = getTopic(topic, true);
  if (!t) return res.status(503).json({ error: 'too many topics' });
  const msg = {
    id: crypto.randomBytes(6).toString('hex'),
    time: Math.floor(Date.now() / 1000),
    expires: Math.floor((Date.now() + KEEP_MS) / 1000),
    event: 'message',
    topic,
    message: typeof req.body === 'string' ? req.body : ''
  };
  t.msgs.push(msg);
  if (t.msgs.length > MAX_MSGS) t.msgs.splice(0, t.msgs.length - MAX_MSGS);
  const line = `data: ${JSON.stringify(msg)}\n\n`;
  t.clients.forEach(c => c.write(line));
  res.json(msg);
});

// استماع لحظي SSE
app.get('/:topic/sse', (req, res) => {
  const { topic } = req.params;
  if (!okTopic(topic)) return res.sendStatus(400);
  const t = getTopic(topic, true);
  if (!t) return res.sendStatus(503);
  res.set({
    'Content-Type': 'text/event-stream; charset=utf-8',
    'Cache-Control': 'no-cache, no-transform',
    Connection: 'keep-alive',
    'X-Accel-Buffering': 'no'
  });
  res.flushHeaders();
  res.write(`data: ${JSON.stringify({ event: 'open', topic, time: Math.floor(Date.now() / 1000) })}\n\n`);
  backlog(t, parseSince(req.query.since)).forEach(m => res.write(`data: ${JSON.stringify(m)}\n\n`));
  t.clients.add(res);
  const ping = setInterval(() => res.write(': ping\n\n'), 20000);
  req.on('close', () => { clearInterval(ping); t.clients.delete(res); });
});

// قراءة مرة واحدة (poll)
app.get('/:topic/json', (req, res) => {
  const { topic } = req.params;
  if (!okTopic(topic)) return res.sendStatus(400);
  const t = getTopic(topic, false);
  const list = t ? backlog(t, parseSince(req.query.since || 'all')) : [];
  res.type('application/x-ndjson').send(list.map(m => JSON.stringify(m)).join('\n') + (list.length ? '\n' : ''));
});

// تنظيف الذاكرة كل 10 دقايق
setInterval(() => {
  const cut = Date.now() / 1000 - KEEP_MS / 1000;
  for (const [name, t] of topics) {
    t.msgs = t.msgs.filter(m => m.time >= cut);
    if (!t.msgs.length && !t.clients.size) topics.delete(name);
  }
}, 600000);

app.get('/', (_, res) => res.send('lounge relay ok'));
app.listen(process.env.PORT || 3000, () => console.log('relay up'));
