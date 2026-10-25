const express = require('express');
const config = require('./config');
const kafka = require('./kafka');

function requireApiKey(req, res, next) {
  if (req.get('x-api-key') !== config.apiKey) {
    return res.status(401).json({ error: 'Missing or invalid x-api-key header' });
  }
  next();
}

const wrap = (fn) => (req, res) =>
  fn(req, res).catch((err) => {
    const type = err.type || err.name || '';
    let status = 500;
    if (/AUTHORIZATION_FAILED/.test(type)) status = 403;
    else if (/SASLAuthentication/.test(type)) status = 401;
    console.error(`[${req.method} ${req.path}]`, type, err.message);
    res.status(status).json({ error: type, message: err.message });
  });

function buildRouter(name) {
  const router = express.Router();

  router.post('/messages', wrap(async (req, res) => {
    const { key, value, headers } = req.body || {};
    if (value === undefined) return res.status(400).json({ error: '"value" is required' });
    const result = await kafka.produce(name, { key, value, headers });
    res.status(201).json({ topic: config[name].topic, result });
  }));

  router.get('/messages', wrap(async (req, res) => {
    const limit = Math.min(Number(req.query.limit) || 10, 500);
    const timeoutMs = Math.min(Number(req.query.timeoutMs) || 10000, 60000);
    let groupId = req.query.groupId || `${name}-api-consumer`;
    // ACLs only allow consumer groups prefixed "secure-" on the secure topic
    if (name === 'secure' && !groupId.startsWith('secure-')) groupId = `secure-${groupId}`;
    const fromBeginning = req.query.fromBeginning !== 'false';
    const messages = await kafka.consume(name, { groupId, limit, timeoutMs, fromBeginning });
    res.json({ topic: config[name].topic, groupId, count: messages.length, messages });
  }));

  return router;
}

const router = express.Router();
router.use('/public', buildRouter('public'));
router.use('/secure', requireApiKey, buildRouter('secure'));

module.exports = router;
