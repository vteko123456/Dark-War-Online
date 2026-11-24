#!/usr/bin/env node
// DARK WAR — ONLINE server (Update 14). Zero dependencies: plain Node.js 18+.
//
//   node online/server.js                  (port 8787, or PORT=10000 node online/server.js)
//
// What it does (and nothing more):
//   1. serves the game page (public/index.html) — the invite link https://<your-host>/?room=<id> opens it
//   2. relays messages between the two players of a room over a WebSocket (/online)
// The match itself runs on the room creator's device (host-authoritative, src/net/netcore.js) — exactly the
// netcode of LOCAL WI-FI play, only the relay now lives on the internet. Because both players connect OUT to
// this server over HTTPS/WSS, there is no peer-to-peer link: NAT / firewalls / mobile carriers don't matter.
//
// Rooms: 12-character random ids (72 bits, crypto.randomBytes), one host + one guest. Each seat gets a secret
// per-seat token (128 bits) that only that browser tab keeps (sessionStorage) — it is what lets a player who
// refreshes take their own seat back, and what stops a third person from taking it. No accounts, no API keys,
// nothing secret is shipped to the client except that player's own seat token.
//
// Environment (all optional):
//   PORT                port to listen on (hosting providers set this)
//   ALLOWED_ORIGINS     comma-separated list of page origins allowed to open the socket (default: any)
//   GRACE_MS            how long a dropped player's seat is held for a reconnect (default 60000)
//   TEST_LATENCY_MS     add artificial one-way delay to every relayed message (testing only)
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const PORT = +process.env.PORT || 8787;
const GRACE_MS = +process.env.GRACE_MS || 60000;            // seat kept after a disconnect
const EMPTY_ROOM_MS = +process.env.EMPTY_ROOM_MS || 30 * 60000;   // created but never joined
const ROOM_MAX_MS = 4 * 3600 * 1000;                         // hard cap on a room's life
const MAX_MSG = 512 * 1024, MAX_ROOMS = 2000, MAX_CONN_PER_IP = 12, CREATES_PER_MIN = 15;
const LATENCY = +process.env.TEST_LATENCY_MS || 0;
const ORIGINS = (process.env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);
const PUBLIC = fs.existsSync(path.join(__dirname, 'public', 'index.html')) ? path.join(__dirname, 'public') : path.join(__dirname, '..', 'dist');

// ---------------------------------------------------------------- rooms
const rooms = new Map();        // id -> { id, created, seats: { host: Seat, guest: Seat } }
// Seat: { token, peer (live socket wrapper or null), lostAt }
const newId = () => { let id; do { id = crypto.randomBytes(9).toString('base64url').replace(/[-_]/g, 'x'); } while (rooms.has(id)); return id; };
const newToken = () => crypto.randomBytes(16).toString('base64url');
const ID_RE = /^[A-Za-z0-9]{12}$/;
const tokenEq = (a, b) => typeof a === 'string' && typeof b === 'string' && a.length === b.length && crypto.timingSafeEqual(Buffer.from(a), Buffer.from(b));

function roomStatus(id) {
  const r = ID_RE.test(id || '') && rooms.get(id);
  if (!r) return 'invalid';
  if (r.seats.guest) return 'full';
  return 'open';
}
setInterval(() => {   // housekeeping: release seats past their grace period, expire rooms
  const now = Date.now();
  for (const [id, r] of rooms) {
    for (const role of ['host', 'guest']) {
      const s = r.seats[role];
      if (s && !s.peer && now - s.lostAt > GRACE_MS) {
        r.seats[role] = null;
        const other = r.seats[role === 'host' ? 'guest' : 'host'];
        if (other && other.peer) send(other.peer, role === 'host' ? { t: 'closed', why: 'expired' } : { t: 'peer', on: false, role, left: true });
        log(`room ${id}: ${role} seat released (no reconnect)`);
      }
    }
    const h = r.seats.host, g = r.seats.guest;
    const dead = !h || (now - r.created > ROOM_MAX_MS) || (!g && now - r.created > EMPTY_ROOM_MS);
    if (dead) { for (const s of [h, g]) if (s && s.peer) { send(s.peer, { t: 'closed', why: 'expired' }); s.peer.room = null; } rooms.delete(id); log(`room ${id}: closed`); }
  }
}, 2000);

// ---------------------------------------------------------------- http
const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://x');
  const sec = { 'X-Content-Type-Options': 'nosniff', 'Referrer-Policy': 'no-referrer', 'Cache-Control': 'no-store' };
  if (url.pathname === '/health') { res.writeHead(200, Object.assign({ 'Content-Type': 'application/json' }, sec)); res.end(JSON.stringify({ server: 'dark-war-online', rooms: rooms.size })); return; }
  if (url.pathname === '/api/room') {   // lets an invite page say "invalid / full" before opening the socket
    res.writeHead(200, Object.assign({ 'Content-Type': 'application/json' }, sec)); res.end(JSON.stringify({ status: roomStatus(url.searchParams.get('id')) })); return;
  }
  if (url.pathname === '/' || url.pathname === '/index.html') {
    const f = path.join(PUBLIC, 'index.html');
    if (!fs.existsSync(f)) { res.writeHead(500); res.end('Game page not built: run  node tools/build.js'); return; }
    res.writeHead(200, Object.assign({ 'Content-Type': 'text/html; charset=utf-8' }, sec)); fs.createReadStream(f).pipe(res); return;
  }
  res.writeHead(404, sec); res.end('not found');
});

// ---------------------------------------------------------------- minimal WebSocket (RFC 6455)
const conns = new Map();   // ip -> count
const creates = new Map(); // ip -> [timestamps]
server.on('upgrade', (req, sock) => {
  const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
  const bad = (code) => { sock.write(`HTTP/1.1 ${code}\r\nConnection: close\r\n\r\n`); sock.destroy(); };
  if (new URL(req.url, 'http://x').pathname !== '/online') return bad('404 Not Found');
  if (ORIGINS.length && !ORIGINS.includes(req.headers.origin)) return bad('403 Forbidden');
  if ((conns.get(ip) || 0) >= MAX_CONN_PER_IP) return bad('429 Too Many Requests');
  const key = req.headers['sec-websocket-key']; if (!key) return bad('400 Bad Request');
  const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  sock.setNoDelay(true);
  conns.set(ip, (conns.get(ip) || 0) + 1);
  const peer = { sock, ip, room: null, role: null, alive: true, frag: null };
  let buf = Buffer.alloc(0);
  sock.on('data', (d) => {
    buf = Buffer.concat([buf, d]);
    if (buf.length > MAX_MSG + 16) { sock.destroy(); return; }
    while (buf.length >= 2) {
      const fin = buf[0] & 0x80, op = buf[0] & 0x0f, masked = buf[1] & 0x80; let len = buf[1] & 0x7f, off = 2;
      if (len === 126) { if (buf.length < 4) return; len = buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (buf.length < 10) return; len = Number(buf.readBigUInt64BE(2)); off = 10; }
      if (len > MAX_MSG) { sock.destroy(); return; }
      const need = off + (masked ? 4 : 0) + len; if (buf.length < need) return;
      let payload = buf.subarray(off + (masked ? 4 : 0), need);
      if (masked) { const mk = buf.subarray(off, off + 4); payload = Buffer.from(payload.map((b, i) => b ^ mk[i & 3])); }
      buf = buf.subarray(need);
      if (op === 0x8) { sock.end(); return; }
      if (op === 0x9) { sendFrame(sock, payload, 0xA); continue; }
      if (op === 0xA) { peer.alive = true; continue; }
      if (op === 0x1 && !fin) { peer.frag = [payload]; continue; }
      if (op === 0x0) { if (!peer.frag) continue; peer.frag.push(payload); if (!fin) continue; payload = Buffer.concat(peer.frag); peer.frag = null; }
      else if (op !== 0x1) continue;
      peer.alive = true;
      handle(peer, payload.toString('utf8'));
    }
  });
  let closed = false;
  const gone = () => { if (closed) return; closed = true; conns.set(ip, Math.max(0, (conns.get(ip) || 1) - 1)); drop(peer); };
  sock.on('close', gone); sock.on('error', gone);
});
// keep idle sockets alive through hosting proxies (they cut silent connections after ~60 s) and detect dead ones
setInterval(() => {
  for (const r of rooms.values()) for (const s of [r.seats.host, r.seats.guest]) if (s && s.peer) {
    if (!s.peer.alive) { s.peer.sock.destroy(); continue; }
    s.peer.alive = false; sendFrame(s.peer.sock, Buffer.alloc(0), 0x9);
  }
}, 20000);
function sendFrame(sock, data, op = 0x1) {
  const p = Buffer.isBuffer(data) ? data : Buffer.from(data);
  const head = p.length < 126 ? Buffer.from([0x80 | op, p.length]) : p.length < 65536 ? Buffer.from([0x80 | op, 126, p.length >> 8, p.length & 255]) : (() => { const h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeBigUInt64BE(BigInt(p.length), 2); return h; })();
  if (!sock.destroyed) sock.write(Buffer.concat([head, p]));
}
const send = (peer, obj) => peer && sendFrame(peer.sock, JSON.stringify(obj));
const relay = (to, text) => { if (!to) return; if (LATENCY) setTimeout(() => sendFrame(to.sock, text), LATENCY); else sendFrame(to.sock, text); };

function seatTake(peer, r, role, seat) {
  if (seat.peer && seat.peer !== peer) { send(seat.peer, { t: 'closed', why: 'replaced' }); seat.peer.room = null; seat.peer.sock.end(); }
  seat.peer = peer; seat.lostAt = 0; peer.room = r.id; peer.role = role;
  const other = r.seats[role === 'host' ? 'guest' : 'host'];
  send(peer, { t: 'room', code: r.id, role, token: seat.token, peer: !!(other && other.peer) });
  if (other && other.peer) send(other.peer, { t: 'peer', on: true, role });
}
function handle(peer, text) {
  let msg; try { msg = JSON.parse(text); } catch (e) { return; }
  if (!msg || typeof msg.t !== 'string') return;
  if (msg.t === 'create') {
    if (peer.room) return;
    const now = Date.now(), list = (creates.get(peer.ip) || []).filter(t => now - t < 60000);
    if (list.length >= CREATES_PER_MIN || rooms.size >= MAX_ROOMS) { send(peer, { t: 'error', code: 'busy', error: 'The server is busy — try again in a minute.' }); return; }
    list.push(now); creates.set(peer.ip, list);
    const r = { id: newId(), created: now, seats: { host: { token: newToken(), peer: null, lostAt: 0 }, guest: null } };
    rooms.set(r.id, r); seatTake(peer, r, 'host', r.seats.host);
    log(`room ${r.id}: created`); return;
  }
  if (msg.t === 'join' || msg.t === 'rejoin') {
    if (peer.room) return;
    const id = String(msg.code || ''), r = ID_RE.test(id) && rooms.get(id);
    if (!r) { send(peer, { t: 'error', code: 'invalid', error: 'This room doesn\'t exist any more (it expired or the link is wrong). Ask your friend for a new link.' }); return; }
    if (msg.t === 'rejoin' || msg.token) {     // a refresh: take your own seat back with its token
      for (const role of ['host', 'guest']) { const s = r.seats[role]; if (s && tokenEq(s.token, msg.token)) { seatTake(peer, r, role, s); log(`room ${id}: ${role} rejoined`); return; } }
      if (msg.t === 'rejoin') { send(peer, { t: 'error', code: 'invalid', error: 'Your seat in this room is gone (it was held for a minute after you left).' }); return; }
    }
    if (r.seats.guest) { send(peer, { t: 'error', code: 'full', error: 'This room already has two players.' }); return; }
    if (!r.seats.host || !r.seats.host.peer) { send(peer, { t: 'error', code: 'invalid', error: 'The player who created this room has left. Ask them for a new link.' }); return; }
    r.seats.guest = { token: newToken(), peer: null, lostAt: 0 }; seatTake(peer, r, 'guest', r.seats.guest);
    log(`room ${id}: guest joined`); return;
  }
  if (msg.t === 'leave') { leave(peer); return; }
  if (msg.t === 'ka') return;                     // client keep-alive
  // everything else goes to the other player in the room, untouched
  const r = peer.room && rooms.get(peer.room); if (!r) return;
  const other = r.seats[peer.role === 'host' ? 'guest' : 'host'];
  if (other && other.peer) relay(other.peer, text);
}
function drop(peer) {   // socket closed without "leave": hold the seat for GRACE_MS
  const r = peer.room && rooms.get(peer.room); if (!r) return;
  const s = r.seats[peer.role]; if (!s || s.peer !== peer) return;
  s.peer = null; s.lostAt = Date.now();
  const other = r.seats[peer.role === 'host' ? 'guest' : 'host'];
  if (other && other.peer) send(other.peer, { t: 'peer', on: false, role: peer.role, left: false });
  log(`room ${r.id}: ${peer.role} disconnected (seat held ${GRACE_MS / 1000}s)`);
  peer.room = null;
}
function leave(peer) {  // explicit LEAVE: the seat is freed now; the creator leaving closes the room
  const r = peer.room && rooms.get(peer.room); if (!r) return;
  const role = peer.role, other = r.seats[role === 'host' ? 'guest' : 'host'];
  peer.room = null;
  if (role === 'host') { if (other && other.peer) { send(other.peer, { t: 'closed', why: 'left' }); other.peer.room = null; } rooms.delete(r.id); log(`room ${r.id}: creator left, closed`); return; }
  r.seats.guest = null;
  if (other && other.peer) send(other.peer, { t: 'peer', on: false, role, left: true });
  log(`room ${r.id}: guest left`);
}
const log = (s) => console.log(new Date().toISOString().slice(11, 19), s);

if (require.main === module) server.listen(PORT, () => console.log(`\n  DARK WAR online server on port ${PORT}${LATENCY ? ` (test latency ${LATENCY} ms)` : ''}\n  serving ${path.relative(process.cwd(), PUBLIC) || '.'}/index.html\n`));
module.exports = { server, rooms };
