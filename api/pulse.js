// Anonymous counts for aheadof.app: agrees on the post's highlighted lines.
//
// GET  /api/pulse                          -> { lines: { id: n } }
// POST /api/pulse { kind: "agree", id, on: true | false }   -> { id, n }
//
// Storage is Upstash Redis over its REST API (connect it in Vercel → Storage; it sets
// KV_REST_API_URL / KV_REST_API_TOKEN). Only totals are kept. To slow spam, each IP is
// hashed with the hour into a key that expires after an hour; the IP itself is never stored.
const crypto = require('crypto');

// Keep in step with the <mark data-line> ids in index.html.
const LINES = ['surface', 'control', 'worse', 'agency', 'remove', 'ownership', 'honest', 'apple', 'seconds', 'nothing', 'plus', 'trust', 'craft'];
const WRITES_PER_HOUR = 60;

const STORE_URL = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const STORE_TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;
const ORIGIN_OK = /^(https:\/\/(www\.)?aheadof\.app|https:\/\/ahead-landing[a-z0-9-]*\.vercel\.app|http:\/\/(localhost|127\.0\.0\.1)(:\d+)?)$/;

async function redis(commands) {
  const r = await fetch(`${STORE_URL}/pipeline`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${STORE_TOKEN}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(commands),
  });
  if (!r.ok) throw new Error(`store ${r.status}`);
  return (await r.json()).map(x => { if (x.error) throw new Error(x.error); return x.result; });
}

const pairs = arr => { const o = {}; for (let i = 0; arr && i < arr.length; i += 2) o[arr[i]] = Math.max(0, parseInt(arr[i + 1], 10) || 0); return o; };

function shape(lines) {
  const out = { lines: {} };
  for (const id of LINES) out.lines[id] = lines[id] || 0;
  return out;
}

async function readBody(req) {
  if (req.body && typeof req.body === 'object') return req.body;
  if (typeof req.body === 'string') return JSON.parse(req.body || '{}');
  let raw = '';
  for await (const chunk of req) { raw += chunk; if (raw.length > 2000) break; }
  return JSON.parse(raw || '{}');
}

function send(res, status, body, cache) {
  res.statusCode = status;
  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Cache-Control', cache || 'no-store');
  res.end(JSON.stringify(body));
}

module.exports = async (req, res) => {
  if (!STORE_URL || !STORE_TOKEN) return send(res, 503, { error: 'counts are not connected yet' });
  try {
    if (req.method === 'GET') {
      const [lines] = await redis([['HGETALL', 'pulse:lines']]);
      return send(res, 200, shape(pairs(lines)), 'public, s-maxage=15, stale-while-revalidate=60');
    }
    if (req.method !== 'POST') return send(res, 405, { error: 'method' });

    const origin = req.headers.origin;
    if (origin && !ORIGIN_OK.test(origin)) return send(res, 403, { error: 'origin' });

    const ip = String(req.headers['x-forwarded-for'] || req.headers['x-real-ip'] || 'unknown').split(',')[0].trim();
    const hour = new Date().toISOString().slice(0, 13);
    const key = 'pulse:rl:' + crypto.createHash('sha256').update(`${ip}|${hour}|${STORE_TOKEN.slice(-16)}`).digest('hex').slice(0, 32);
    const [writes] = await redis([['INCR', key], ['EXPIRE', key, '3600']]);
    if (writes > WRITES_PER_HOUR) return send(res, 429, { error: 'slow down' });

    const b = await readBody(req);
    if (b.kind === 'agree' && LINES.includes(b.id) && typeof b.on === 'boolean') {
      let [n] = await redis([['HINCRBY', 'pulse:lines', b.id, b.on ? '1' : '-1']]);
      if (n < 0) { await redis([['HSET', 'pulse:lines', b.id, '0']]); n = 0; }
      return send(res, 200, { id: b.id, n });
    }
    return send(res, 400, { error: 'bad request' });
  } catch (e) {
    return send(res, 502, { error: 'store unavailable' });
  }
};
