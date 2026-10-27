// Online 2-player Tic-Tac-Toe: Cloudflare Worker + Durable Object (state lives in memory only, no database).
const AB = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789'; // no 0/O/1/I/L
const CODE_RE = /^[A-HJKMNP-Z2-9]{5}$/;
const PID_RE = /^[A-Za-z0-9_-]{16,64}$/;
const GRACE_MS = 60000, INIT_MS = 120000, DEAD_MS = 70000;
const LINES = [[0, 1, 2], [3, 4, 5], [6, 7, 8], [0, 3, 6], [1, 4, 7], [2, 5, 8], [0, 4, 8], [2, 4, 6]];

const json = (o, s = 200) => new Response(JSON.stringify(o), { status: s, headers: { 'content-type': 'application/json', 'cache-control': 'no-store' } });

function genCode() {
  let c = '';
  const lim = 256 - (256 % AB.length);
  while (c.length < 5) {
    for (const x of crypto.getRandomValues(new Uint8Array(8))) if (x < lim && c.length < 5) c += AB[x % AB.length];
  }
  return c;
}

function sameOrigin(req, url) {
  const o = req.headers.get('Origin');
  if (!o) return true;
  try { return new URL(o).host === url.host; } catch (e) { return false; }
}

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const p = url.pathname;
      if (p === '/ws') {
        if ((request.headers.get('Upgrade') || '').toLowerCase() !== 'websocket') return new Response('Expected WebSocket', { status: 426 });
        if (!sameOrigin(request, url)) return new Response('Forbidden', { status: 403 });
        const code = (url.searchParams.get('room') || '').toUpperCase();
        const pid = url.searchParams.get('pid') || '';
        if (!CODE_RE.test(code) || !PID_RE.test(pid)) return new Response('Bad request', { status: 400 });
        return env.GAME_ROOM.get(env.GAME_ROOM.idFromName(code)).fetch(request);
      }
      if (p === '/api/create') {
        if (request.method !== 'POST') return json({ error: 'method' }, 405);
        if (!sameOrigin(request, url)) return json({ error: 'origin' }, 403);
        let b;
        try { b = JSON.parse((await request.text()).slice(0, 512)); } catch (e) { return json({ error: 'bad' }, 400); }
        if (!b || !PID_RE.test(String(b.pid))) return json({ error: 'bad' }, 400);
        for (let n = 0; n < 8; n++) {
          const code = genCode();
          const r = await env.GAME_ROOM.get(env.GAME_ROOM.idFromName(code)).fetch('https://room.internal/init', { method: 'POST', body: JSON.stringify({ code, pid: b.pid }) });
          if (r.status === 200) return json({ code });
          if (r.status !== 409) break;
        }
        return json({ error: 'busy' }, 503);
      }
      if (p === '/favicon.ico') return new Response(null, { status: 204 });
      if (p === '/' || p === '/index.html') {
        if (request.method !== 'GET' && request.method !== 'HEAD') return new Response('Method not allowed', { status: 405 });
        return new Response(HTML, {
          headers: {
            'content-type': 'text/html; charset=utf-8',
            'cache-control': 'no-cache',
            'x-content-type-options': 'nosniff',
            'referrer-policy': 'no-referrer',
            'content-security-policy': "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; img-src data:; connect-src 'self' wss://" + url.host + "; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
          }
        });
      }
      return new Response('Not found', { status: 404 });
    } catch (e) {
      return new Response('Server error', { status: 500 });
    }
  }
};

// One Durable Object = one room. Everything is kept in memory (standard WebSocket API, no hibernation).
export class GameRoom {
  constructor(state, env) { this.wipe(); }

  wipe() {
    clearTimeout(this.initT); clearInterval(this.sweepT);
    if (this.pl) for (const s of ['X', 'O']) if (this.pl[s]) clearTimeout(this.pl[s].graceT);
    this.initT = 0; this.sweepT = 0;
    this.live = false; this.code = '';
    this.pl = { X: null, O: null };
    this.board = Array(9).fill(''); this.turn = 'X'; this.starter = 'X'; this.status = 'waiting';
    this.winner = ''; this.line = null; this.score = { X: 0, O: 0, D: 0 }; this.round = 1; this.rm = { X: false, O: false };
  }

  mk(pid) { return { pid, ws: null, on: false, seen: Date.now(), dead: 0, graceT: 0, tok: 10, tAt: Date.now() }; }

  async fetch(request) {
    try {
      const url = new URL(request.url);
      if (url.pathname === '/init' && request.method === 'POST') {
        const b = await request.json();
        if (this.live) return new Response('exists', { status: 409 });
        this.wipe(); this.live = true; this.code = b.code;
        this.pl.X = this.mk(b.pid); // the creator's seat (X) is reserved
        this.initT = setTimeout(() => this.close('expired', ''), INIT_MS);
        return new Response('ok');
      }
      if ((request.headers.get('Upgrade') || '').toLowerCase() === 'websocket') {
        const pair = new WebSocketPair();
        const [client, server] = Object.values(pair);
        server.accept();
        this.join(server, url.searchParams.get('pid') || '');
        return new Response(null, { status: 101, webSocket: client });
      }
      return new Response('Not found', { status: 404 });
    } catch (e) {
      return new Response('Error', { status: 500 });
    }
  }

  send(ws, o) { try { ws.send(JSON.stringify(o)); } catch (e) {} }

  reject(ws, code, n) { this.send(ws, { t: 'error', code }); try { ws.close(n, code); } catch (e) {} }

  join(ws, pid) {
    if (!this.live) return this.reject(ws, 'room_not_found', 4004);
    const s = this.pl.X && this.pl.X.pid === pid ? 'X' : this.pl.O && this.pl.O.pid === pid ? 'O' : !this.pl.O ? 'O' : '';
    if (!s) return this.reject(ws, 'room_full', 4005);
    const p = this.pl[s] || (this.pl[s] = this.mk(pid));
    if (p.ws) { this.send(p.ws, { t: 'error', code: 'replaced' }); try { p.ws.close(4001, 'replaced'); } catch (e) {} }
    p.ws = ws; p.on = true; p.seen = Date.now(); p.dead = 0; clearTimeout(p.graceT);
    if (s === 'X') clearTimeout(this.initT);
    if (!this.sweepT) this.sweepT = setInterval(() => this.sweep(), 20000);
    ws.addEventListener('message', e => this.msg(s, ws, e));
    ws.addEventListener('close', () => this.drop(s, ws));
    ws.addEventListener('error', () => this.drop(s, ws));
    if (this.status === 'waiting' && this.pl.X && this.pl.O && this.pl.X.on && this.pl.O.on) this.status = 'playing';
    this.cast();
  }

  view(s) {
    const o = s === 'X' ? 'O' : 'X', op = this.pl[o];
    return {
      t: 'state', room: this.code, you: s, board: this.board, turn: this.turn, status: this.status,
      winner: this.winner, line: this.line, score: this.score, round: this.round,
      opp: !!(op && op.on), grace: op && !op.on && op.dead ? Math.max(0, op.dead - Date.now()) : 0,
      rmMe: this.rm[s], rmOpp: this.rm[o]
    };
  }

  cast() { for (const s of ['X', 'O']) { const p = this.pl[s]; if (p && p.on) this.send(p.ws, this.view(s)); } }

  msg(s, ws, e) {
    try {
      const p = this.pl[s];
      if (!p || p.ws !== ws) return;
      const now = Date.now();
      p.seen = now;
      p.tok = Math.min(10, p.tok + (now - p.tAt) / 150); p.tAt = now; // basic rate limit
      if (p.tok < 1) return;
      p.tok -= 1;
      const d = e.data;
      if (typeof d !== 'string' || d.length > 256) return;
      const m = JSON.parse(d);
      if (!m || typeof m.t !== 'string') return;
      if (m.t === 'ping') this.send(ws, { t: 'pong' });
      else if (m.t === 'move') this.move(s, m.i);
      else if (m.t === 'rematch') this.rematch(s);
      else if (m.t === 'leave') this.close('left', s);
    } catch (err) { /* never crash the room */ }
  }

  move(s, i) {
    const o = s === 'X' ? 'O' : 'X', op = this.pl[o];
    const why = this.status !== 'playing' ? (this.status === 'over' ? 'over' : 'away')
      : !Number.isInteger(i) || i < 0 || i > 8 ? 'bad'
      : this.turn !== s ? 'turn'
      : !(op && op.on) ? 'away'
      : this.board[i] ? 'taken' : '';
    if (why) { this.send(this.pl[s].ws, { t: 'notice', code: why }); this.send(this.pl[s].ws, this.view(s)); return; }
    this.board[i] = s;
    const ln = LINES.find(l => l.every(k => this.board[k] === s));
    if (ln) { this.status = 'over'; this.winner = s; this.line = ln; this.score[s]++; }
    else if (this.board.every(Boolean)) { this.status = 'over'; this.score.D++; }
    else this.turn = o;
    this.cast();
  }

  rematch(s) {
    if (this.status !== 'over') return;
    this.rm[s] = true;
    if (this.rm.X && this.rm.O) {
      this.board = Array(9).fill(''); this.starter = this.starter === 'X' ? 'O' : 'X'; this.turn = this.starter;
      this.status = 'playing'; this.winner = ''; this.line = null; this.round++; this.rm = { X: false, O: false };
    }
    this.cast();
  }

  drop(s, ws) {
    const p = this.pl[s];
    if (!this.live || !p || p.ws !== ws) return;
    p.ws = null; p.on = false; p.dead = Date.now() + GRACE_MS;
    clearTimeout(p.graceT);
    p.graceT = setTimeout(() => this.close('timeout', s), GRACE_MS);
    this.cast(); // tell the opponent immediately
  }

  sweep() {
    const now = Date.now();
    for (const s of ['X', 'O']) {
      const p = this.pl[s];
      if (p && p.on && now - p.seen > DEAD_MS) { const w = p.ws; try { w.close(4008, 'timeout'); } catch (e) {} this.drop(s, w); }
    }
  }

  close(reason, by) {
    if (!this.live) return;
    this.live = false;
    for (const s of ['X', 'O']) {
      const p = this.pl[s];
      if (p && p.ws) { if (s !== by) this.send(p.ws, { t: 'closed', reason }); try { p.ws.close(4000, reason); } catch (e) {} }
    }
    this.wipe(); // room removed from memory
  }
}

const HTML = String.raw`<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="color-scheme" content="dark">
<title>Tic-Tac-Toe Online</title>
<link rel="icon" href="data:,">
<style>
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}
:root{--x:#ff7a8a;--o:#6fe3ff}
html{touch-action:manipulation}
[hidden]{display:none!important}
body{margin:0;min-height:100vh;min-height:100dvh;background:#0f2027;color:#fff;font:16px/1.4 system-ui,-apple-system,"Segoe UI",Roboto,Tahoma,sans-serif}
.bg{position:fixed;inset:0;opacity:0;transition:opacity .9s;z-index:0}
.bg.show{opacity:1}
main{position:relative;z-index:1;max-width:440px;margin:0 auto;padding:16px 16px 40px}
header{display:flex;align-items:center;justify-content:space-between;gap:8px}
h1{font-size:22px;margin:0}
.tools{display:flex;gap:8px}
button,input{font:inherit;color:#fff}
button{background:rgba(255,255,255,.16);border:0;border-radius:12px;padding:10px 16px;cursor:pointer;-webkit-user-select:none;user-select:none;transition:transform .1s,background .2s}
button:active{transform:scale(.96)}
button:disabled{opacity:.5}
.big{width:100%;padding:16px;font-size:18px;font-weight:700;background:#fff;color:#0f2027}
section{margin-top:20px;display:flex;flex-direction:column;gap:12px}
.or{text-align:center;opacity:.8}
.row{display:flex;gap:8px;justify-content:center}
input{flex:1;min-width:0;background:rgba(255,255,255,.12);border:2px solid rgba(255,255,255,.25);border-radius:12px;padding:12px;font-size:20px;direction:ltr;text-align:center;letter-spacing:.3em;text-transform:uppercase}
input::placeholder{letter-spacing:normal;opacity:.6}
.err{color:#ffb4b4;text-align:center;min-height:22px;margin:0}
.bar{display:flex;align-items:center;justify-content:center;flex-wrap:wrap;gap:8px}
#rc{font-size:26px;letter-spacing:.2em;direction:ltr}
#dot{width:10px;height:10px;border-radius:50%;background:#e5484d;display:inline-block}
#dot.ok{background:#3ddc84}
.stat{text-align:center;font-size:20px;font-weight:700;min-height:30px}
.mine{animation:pl 1.4s ease-in-out infinite}
@keyframes pl{50%{transform:scale(1.05)}}
#sub{text-align:center;min-height:20px;opacity:.85}
.score{display:grid;grid-template-columns:repeat(3,1fr);gap:8px}
.box{background:rgba(255,255,255,.1);border-radius:12px;padding:6px;text-align:center}
.box b{display:block;font-size:22px}
.box small{opacity:.85}
.box.me{outline:2px solid rgba(255,255,255,.7)}
.x b{color:var(--x)}.o b{color:var(--o)}
#bw{position:relative;width:min(88vw,360px);aspect-ratio:1/1;margin:0 auto}
#board{display:grid;grid-template-columns:repeat(3,1fr);grid-template-rows:repeat(3,1fr);gap:8px;width:100%;height:100%;direction:ltr}
.cell{background:rgba(255,255,255,.1);border-radius:16px;padding:0;display:flex;align-items:center;justify-content:center;cursor:default}
.cell.on{cursor:pointer;background:rgba(255,255,255,.16)}
.mk{width:68%;height:68%;fill:none;stroke-width:11;stroke-linecap:round}
.mk.X{stroke:var(--x)}.mk.O{stroke:var(--o)}
.mk path,.mk circle{stroke-dasharray:200;stroke-dashoffset:200;animation:dr .35s ease-out forwards}
.mk path+path{animation-delay:.15s}
@keyframes dr{to{stroke-dashoffset:0}}
#wl{position:absolute;inset:0;width:100%;height:100%;pointer-events:none}
#wln{stroke:#fff;stroke-width:10;stroke-linecap:round;opacity:.9;stroke-dasharray:300;stroke-dashoffset:300}
#wln.go{animation:dr .5s ease-out forwards}
#toast{position:fixed;left:50%;bottom:24px;transform:translateX(-50%) translateY(20px);background:rgba(0,0,0,.78);padding:10px 16px;border-radius:12px;opacity:0;transition:.25s;pointer-events:none;z-index:5;max-width:90vw;text-align:center}
#toast.show{opacity:1;transform:translateX(-50%)}
</style></head>
<body>
<div class="bg" id="bg1"></div><div class="bg" id="bg2"></div>
<main>
<header><h1 id="ttl"></h1><div class="tools"><button id="bLang" type="button"></button><button id="bSnd" type="button"></button></div></header>
<section id="lobby">
<button id="bCreate" class="big" type="button"></button>
<div class="or" id="tOr"></div>
<div class="row"><input id="code" maxlength="5" autocomplete="off" autocapitalize="characters" autocorrect="off" spellcheck="false" inputmode="text"><button id="bJoin" type="button"></button></div>
<p class="err" id="err"></p>
</section>
<section id="game" hidden>
<div class="bar"><span id="dot"></span><span id="tRoom"></span><b id="rc"></b><button id="bCopy" type="button"></button><button id="bShare" type="button"></button></div>
<div class="stat" id="stat"></div>
<div id="sub"></div>
<div class="score"><div id="sX"></div><div id="sD"></div><div id="sO"></div></div>
<div id="bw"><div id="board"></div><svg id="wl" viewBox="0 0 300 300"><line id="wln" x1="0" y1="0" x2="0" y2="0" style="display:none"></line></svg></div>
<div class="row"><button id="bRe" type="button" hidden></button><button id="bLeave" type="button"></button></div>
</section>
<div id="toast"></div>
</main>
<script>
(() => {
'use strict';
const $ = i => document.getElementById(i);
const h = (t, c, x) => { const e = document.createElement(t); if (c) e.className = c; if (x != null) e.textContent = x; return e; };
const CODE = /^[A-HJKMNP-Z2-9]{5}$/;
const T = {
  en: { title: 'Tic-Tac-Toe Online', create: 'Create room', join: 'Join', or: 'or join with a code', ph: 'CODE', copy: 'Copy', share: 'Share', copied: 'Copied \u2713', room: 'Room', you: 'You', draws: 'Draws',
    waiting: 'Waiting for opponent\u2026 share the code', yourTurn: 'Your turn', oppTurn: "Opponent's turn", won: '\uD83C\uDF89 You won!', lost: 'You lost \uD83D\uDE15', draw: "It's a draw \uD83E\uDD1D",
    rematch: 'Rematch', rematchWait: 'Waiting for opponent to accept\u2026', oppRematch: 'Opponent wants a rematch', leave: 'Leave', recon: 'Reconnecting\u2026', away: 'Opponent disconnected \u2014 waiting {n}s',
    errInvalid: 'Invalid room code', errNotFound: 'Room not found', errFull: 'Room is full', errNet: 'Connection failed, try again',
    closed_left: 'Your opponent left the room', closed_timeout: 'Opponent did not return \u2014 room closed', closed_expired: 'Room expired', replaced: 'This game was opened in another tab',
    n_turn: "It's not your turn", n_taken: 'That cell is taken', n_away: 'Opponent is offline', n_bad: 'Invalid move', n_over: 'The game is over' },
  fa: { title: 'بازی دوز آنلاین', create: 'ساخت اتاق', join: 'ورود', or: 'یا با کد وارد شو', ph: 'کد', copy: 'کپی', share: 'اشتراک', copied: 'کپی شد \u2713', room: 'اتاق', you: 'شما', draws: 'مساوی',
    waiting: 'منتظر حریف\u2026 کد را برای دوستت بفرست', yourTurn: 'نوبت شماست', oppTurn: 'نوبت حریف است', won: '\uD83C\uDF89 شما بردید!', lost: 'حریف برد \uD83D\uDE15', draw: 'مساوی شد \uD83E\uDD1D',
    rematch: 'بازی دوباره', rematchWait: 'منتظر تأیید حریف\u2026', oppRematch: 'حریف بازی دوباره می\u200Cخواهد', leave: 'خروج', recon: 'در حال اتصال مجدد\u2026', away: 'حریف قطع شد \u2014 منتظر بازگشت ({n} ثانیه)',
    errInvalid: 'کد اتاق نامعتبر است', errNotFound: 'اتاقی با این کد پیدا نشد', errFull: 'اتاق پر است', errNet: 'اتصال برقرار نشد، دوباره تلاش کن',
    closed_left: 'حریف از اتاق خارج شد', closed_timeout: 'حریف برنگشت و اتاق بسته شد', closed_expired: 'اتاق منقضی شد', replaced: 'این بازی در صفحهٔ دیگری باز شد',
    n_turn: 'نوبت شما نیست', n_taken: 'این خانه پر است', n_away: 'حریف آفلاین است', n_bad: 'حرکت نامعتبر', n_over: 'بازی تمام شده است' }
};
const G = ['#0f2027,#2c5364', '#41295a,#2f0743', '#0b3d2e,#1b6b4f', '#3a1c71,#8e3b6b', '#1e3c72,#2a5298', '#232526,#414345', '#4b134f,#7a2e3a', '#0f0c29,#302b63'];
let lang = '', snd = true, ws = null, room = '', S = null, P = null, fatal = false, tries = 0, downAt = 0, net = false, last = 0, graceAt = 0;
let hbT = 0, rcT = 0, toastT = 0, ac = null, tapped = false, bgI = -1, bgC = 0, lineRound = 0;
try { lang = localStorage.getItem('ttt_lang') || ''; snd = localStorage.getItem('ttt_snd') !== '0'; } catch (e) {}
if (lang !== 'en' && lang !== 'fa') lang = (navigator.language || '').slice(0, 2) === 'fa' ? 'fa' : 'en';
const t = (k, n) => String(T[lang][k] || T.en[k] || k).replace('{n}', n);

const rnd = () => { const a = new Uint8Array(16); try { crypto.getRandomValues(a); } catch (e) { for (let i = 0; i < 16; i++) a[i] = Math.random() * 256; } return Array.from(a, x => ('0' + x.toString(16)).slice(-2)).join(''); };
let pid = '';
try { pid = sessionStorage.getItem('ttt_pid') || ''; } catch (e) {}
if (!/^[a-f0-9]{32}$/.test(pid)) { pid = rnd(); try { sessionStorage.setItem('ttt_pid', pid); } catch (e) {} }

const norm = s => String(s).replace(/[\u06F0-\u06F9]/g, d => d.charCodeAt(0) - 1776).replace(/[\u0660-\u0669]/g, d => d.charCodeAt(0) - 1632).toUpperCase().replace(/[^A-Z0-9]/g, '');
const hc = s => { let n = 0; for (const c of s) n += c.charCodeAt(0); return n; };
const show = id => { $('lobby').hidden = id !== 'lobby'; $('game').hidden = id !== 'game'; };
const err = m => { $('err').textContent = m || ''; };
const toast = m => { const e = $('toast'); e.textContent = m; e.classList.add('show'); clearTimeout(toastT); toastT = setTimeout(() => e.classList.remove('show'), 2200); };
const setBg = i => { if (i === bgI) return; bgI = i; bgC = 1 - bgC; const on = $('bg' + (bgC + 1)), off = $('bg' + (2 - bgC)); on.style.background = 'linear-gradient(135deg,' + G[i] + ')'; on.classList.add('show'); off.classList.remove('show'); };

addEventListener('pointerup', () => {
  tapped = true;
  if (!ac) { try { const C = window.AudioContext || window.webkitAudioContext; if (C) ac = new C(); } catch (e) {} }
  if (ac && ac.state === 'suspended') { try { ac.resume().catch(() => {}); } catch (e) {} }
}, { passive: true });
const beep = (f, d) => {
  if (!snd || !ac || ac.state !== 'running') return;
  try { const o = ac.createOscillator(), g = ac.createGain(); o.frequency.value = f; g.gain.setValueAtTime(0.08, ac.currentTime); g.gain.exponentialRampToValueAtTime(0.001, ac.currentTime + d); o.connect(g); g.connect(ac.destination); o.start(); o.stop(ac.currentTime + d); } catch (e) {}
};
const vib = p => { try { if (snd && tapped && navigator.vibrate) navigator.vibrate(p); } catch (e) {} };

const NS = 'http://www.w3.org/2000/svg';
const mark = v => {
  const s = document.createElementNS(NS, 'svg');
  s.setAttribute('viewBox', '0 0 100 100'); s.setAttribute('class', 'mk ' + v);
  const el = (tag, a) => { const e = document.createElementNS(NS, tag); for (const k in a) e.setAttribute(k, a[k]); s.appendChild(e); };
  if (v === 'X') { el('path', { d: 'M22 22L78 78' }); el('path', { d: 'M78 22L22 78' }); } else el('circle', { cx: 50, cy: 50, r: 30 });
  return s;
};

const cells = [];
for (let i = 0; i < 9; i++) {
  const c = h('button', 'cell'); c.type = 'button'; c.dataset.v = '';
  c.addEventListener('click', () => { if (S && S.status === 'playing' && S.turn === S.you && S.opp && !S.board[i]) send({ t: 'move', i }); });
  $('board').appendChild(c); cells.push(c);
}

function status() {
  const s = S; if (!s) return;
  const left = Math.max(0, Math.ceil((graceAt - Date.now()) / 1000));
  let m, c = '', sub = '';
  if (!net) m = t('recon');
  else if (s.status === 'waiting') m = t('waiting');
  else if (s.status === 'over') {
    m = s.winner ? (s.winner === s.you ? t('won') : t('lost')) : t('draw');
    sub = !s.opp ? t('away', left) : s.rmMe ? t('rematchWait') : s.rmOpp ? t('oppRematch') : '';
  } else if (!s.opp) m = t('away', left);
  else { m = s.turn === s.you ? t('yourTurn') : t('oppTurn'); c = s.turn === s.you ? 'mine' : ''; }
  const e = $('stat'); e.textContent = m; e.className = 'stat ' + c;
  $('sub').textContent = sub; $('dot').className = net ? 'ok' : '';
}

function render() {
  const s = S; if (!s) return;
  show('game'); setBg((hc(s.room) + s.round) % G.length);
  $('rc').textContent = s.room;
  let cnt = 0;
  for (let i = 0; i < 9; i++) {
    const c = cells[i], v = s.board[i];
    if (v) cnt++;
    if (c.dataset.v !== v) { c.dataset.v = v; c.textContent = ''; if (v) c.appendChild(mark(v)); }
    c.classList.toggle('on', !!(s.status === 'playing' && s.turn === s.you && s.opp && !v));
  }
  const wl = $('wln');
  if (s.status === 'over' && s.line) {
    if (lineRound !== s.round) {
      lineRound = s.round;
      const a = s.line[0], b = s.line[2];
      wl.setAttribute('x1', (a % 3) * 100 + 50); wl.setAttribute('y1', Math.floor(a / 3) * 100 + 50);
      wl.setAttribute('x2', (b % 3) * 100 + 50); wl.setAttribute('y2', Math.floor(b / 3) * 100 + 50);
      wl.classList.remove('go'); void wl.getBoundingClientRect(); wl.classList.add('go');
    }
    wl.style.display = '';
  } else { lineRound = 0; wl.classList.remove('go'); wl.style.display = 'none'; }
  if (P) {
    if (cnt > P.board.filter(Boolean).length) beep(s.turn === s.you ? 660 : 440, 0.12);
    if (s.status === 'over' && P.status !== 'over') { beep(s.winner === s.you ? 880 : s.winner ? 220 : 330, 0.4); vib(s.winner === s.you ? [60, 40, 60] : 80); }
    else if (s.status === 'playing' && s.turn === s.you && (P.turn !== s.you || P.status !== 'playing')) vib(25);
  }
  P = s;
  const sc = (id, k) => {
    const e = $(id), me = s.you === k;
    e.textContent = ''; e.className = 'box ' + k.toLowerCase() + (me ? ' me' : '');
    e.appendChild(h('small', '', k === 'D' ? t('draws') : k + (me ? ' \u00B7 ' + t('you') : '')));
    e.appendChild(h('b', '', s.score[k]));
  };
  sc('sX', 'X'); sc('sD', 'D'); sc('sO', 'O');
  $('bRe').hidden = s.status !== 'over'; $('bRe').disabled = !!s.rmMe;
  status();
}

function send(o) { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); }

function onMsg(m) {
  if (!m || !room) return;
  if (m.t === 'state') { S = m; graceAt = Date.now() + (m.grace || 0); render(); }
  else if (m.t === 'error') toLobby(m.code === 'replaced' ? 'replaced' : m.code === 'room_full' ? 'errFull' : 'errNotFound');
  else if (m.t === 'closed') toLobby('closed_' + m.reason);
  else if (m.t === 'notice') toast(t('n_' + m.code));
}

function connect() {
  clearTimeout(rcT);
  if (ws && ws.readyState < 2) return;
  let w;
  try { w = new WebSocket((location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/ws?room=' + room + '&pid=' + pid); } catch (e) { retry(); return; }
  ws = w;
  w.onopen = () => { if (ws !== w) return; tries = 0; downAt = 0; last = Date.now(); net = true; status(); clearInterval(hbT); hbT = setInterval(beat, 25000); };
  w.onmessage = e => { if (ws !== w) return; last = Date.now(); let m; try { m = JSON.parse(e.data); } catch (x) { return; } onMsg(m); };
  w.onclose = () => { if (ws !== w) return; ws = null; clearInterval(hbT); net = false; status(); if (!fatal && room) retry(); };
}

function beat() {
  if (!ws || ws.readyState !== 1) return;
  if (Date.now() - last > 60000) { dropWs(); return; }
  send({ t: 'ping' });
}

function dropWs() {
  clearInterval(hbT);
  const w = ws; ws = null;
  if (w) { try { w.close(); } catch (e) {} }
  if (room && !fatal) retry();
}

function retry() {
  net = false; status();
  if (!downAt) downAt = Date.now();
  if (Date.now() - downAt > 75000 || (!S && tries > 3)) { toLobby('errNet'); return; }
  clearTimeout(rcT);
  rcT = setTimeout(connect, Math.min(8000, 700 * Math.pow(2, tries++)) + Math.random() * 400);
}

function toLobby(k) {
  fatal = true; room = ''; S = null; P = null; net = false;
  clearTimeout(rcT); clearInterval(hbT);
  if (ws) { const w = ws; ws = null; try { w.close(); } catch (e) {} }
  try { history.replaceState(null, '', '/'); } catch (e) {}
  show('lobby'); setBg(0); err(k ? t(k) : '');
}

function start(c) {
  if (ws) { const w = ws; ws = null; try { w.close(); } catch (e) {} }
  fatal = false; tries = 0; downAt = 0; S = null; P = null; room = c; net = false;
  try { history.replaceState(null, '', '/?room=' + c); } catch (e) {}
  connect();
}

function create() {
  const b = $('bCreate'); b.disabled = true; err('');
  fetch('/api/create', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ pid }) })
    .then(r => r.json())
    .then(d => { b.disabled = false; if (d && d.code) start(d.code); else err(t('errNet')); })
    .catch(() => { b.disabled = false; err(t('errNet')); });
}

function joinIt() {
  const c = norm($('code').value);
  if (!CODE.test(c)) { err(t('errInvalid')); return; }
  err(''); start(c);
}

const fb = c => {
  try { const a = h('textarea'); a.value = c; a.style.cssText = 'position:fixed;opacity:0'; document.body.appendChild(a); a.select(); document.execCommand('copy'); a.remove(); toast(t('copied')); } catch (e) { toast(c); }
};
const copyText = c => {
  if (navigator.clipboard && navigator.clipboard.writeText) navigator.clipboard.writeText(c).then(() => toast(t('copied')), () => fb(c));
  else fb(c);
};

function applyLang() {
  document.documentElement.lang = lang; document.documentElement.dir = lang === 'fa' ? 'rtl' : 'ltr'; document.title = t('title');
  $('ttl').textContent = t('title'); $('bCreate').textContent = t('create'); $('bJoin').textContent = t('join'); $('tOr').textContent = t('or');
  $('code').placeholder = t('ph'); $('bCopy').textContent = t('copy'); $('bShare').textContent = t('share'); $('tRoom').textContent = t('room');
  $('bRe').textContent = t('rematch'); $('bLeave').textContent = t('leave'); $('bLang').textContent = lang === 'fa' ? 'EN' : '\u0641\u0627';
  $('bSnd').textContent = snd ? '\uD83D\uDD0A' : '\uD83D\uDD07'; err('');
  render();
}

$('bCreate').addEventListener('click', create);
$('bJoin').addEventListener('click', joinIt);
$('code').addEventListener('input', e => { e.target.value = norm(e.target.value).slice(0, 5); });
$('code').addEventListener('keydown', e => { if (e.key === 'Enter') joinIt(); });
$('bLang').addEventListener('click', () => { lang = lang === 'fa' ? 'en' : 'fa'; try { localStorage.setItem('ttt_lang', lang); } catch (e) {} applyLang(); });
$('bSnd').addEventListener('click', () => { snd = !snd; try { localStorage.setItem('ttt_snd', snd ? '1' : '0'); } catch (e) {} $('bSnd').textContent = snd ? '\uD83D\uDD0A' : '\uD83D\uDD07'; });
$('bCopy').addEventListener('click', () => { if (room) copyText(room); });
$('bShare').hidden = !navigator.share;
$('bShare').addEventListener('click', () => {
  const u = location.origin + '/?room=' + room;
  if (navigator.share) navigator.share({ title: t('title'), text: t('title') + ' \u2014 ' + room, url: u }).catch(() => {}); else copyText(u);
});
$('bRe').addEventListener('click', () => send({ t: 'rematch' }));
$('bLeave').addEventListener('click', () => { send({ t: 'leave' }); toLobby(''); });

document.addEventListener('visibilitychange', () => {
  if (document.hidden || !room || fatal) return;
  if (!ws || ws.readyState > 1) { tries = 0; connect(); return; }
  send({ t: 'ping' });
  const l = Date.now();
  setTimeout(() => { if (ws && last < l) dropWs(); }, 5000);
});
addEventListener('online', () => { if (room && !fatal && (!ws || ws.readyState > 1)) { tries = 0; connect(); } });
setInterval(() => { if (S && !S.opp) status(); }, 1000);

show('lobby'); setBg(0); applyLang();
const q = norm(new URLSearchParams(location.search).get('room') || '');
if (CODE.test(q)) start(q);
})();
</script>
</body></html>`;
