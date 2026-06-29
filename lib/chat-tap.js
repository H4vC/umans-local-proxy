'use strict';

// Main-thread tap shim. The parse work (SSE/JSON scan — the dominant tap cost)
// runs in a dedicated worker thread (lib/tap-worker.js) on its own core, so the
// pipe's event loop is freed to forward bytes. This shim:
//   - counts bytes O(1) on the main thread (for the live snapshot + finalize),
//   - batches chunk COPIES (the originals go to res, so they can't be
//     transferred) and transfers each batch zero-copy to the worker,
//   - receives throttled syncs (live tok/s) + a final round-trip on end
//     (token counts + decoded responseContent for coalescing).
//
// Robustness: if the worker dies, telemetry degrades gracefully — bytes still
// flow to the client via the pipe (the Transform forwards chunks regardless),
// and onEnd() finalizes with main-only data (bytes) instead of hanging. The
// worker is restarted lazily for subsequent requests.
//
// The worker handle lives in state.js so it survives hot reload (chat-tap.js
// is purged from require.cache on reload; without state, each reload would
// orphan a worker thread).

const { Worker } = require('node:worker_threads');
const { randomUUID } = require('node:crypto');
const path = require('node:path');
const { Transform } = require('node:stream');
const state = require('./state');

const WORKER_PATH = path.join(__dirname, 'tap-worker.js');
const FLUSH_BYTES = 64 * 1024; // flush a batch when it reaches this size
const FLUSH_MS = 20;           // ...or after this long (latency cap for live tok/s)
const FINAL_TIMEOUT_MS = 1000; // give the worker this long to finalize before giving up
const RESTART_MS = 1000;       // backoff before recreating a dead worker

function ensureTapState() {
  if (!(state.tapPendingFinals instanceof Map)) state.tapPendingFinals = new Map();
  if (!Object.prototype.hasOwnProperty.call(state, 'tapWorker')) state.tapWorker = null;
  if (typeof state.tapWorkerDead !== 'boolean') state.tapWorkerDead = false;
  if (!Object.prototype.hasOwnProperty.call(state, 'tapRestartTimer')) state.tapRestartTimer = null;
}

ensureTapState();

function onWorkerMessage(msg) {
  ensureTapState();
  const { op, id } = msg;
  if (op === 'sync') {
    const s = state.sessions.get(id);
    if (s && msg.fields) Object.assign(s, msg.fields); // update live counts
  } else if (op === 'final') {
    const p = state.tapPendingFinals.get(id);
    if (p) { state.tapPendingFinals.delete(id); clearTimeout(p.timer); p.resolve(msg.fields); }
  }
}

function killWorker() {
  ensureTapState();
  state.tapWorkerDead = true;
  const w = state.tapWorker;
  state.tapWorker = null;
  if (w) { try { w.terminate(); } catch {} }
  // Resolve every pending onEnd with null (main-only finalize) so callers don't hang.
  for (const [, p] of state.tapPendingFinals) { clearTimeout(p.timer); p.resolve(null); }
  state.tapPendingFinals.clear();
}

function scheduleRestart() {
  ensureTapState();
  if (state.tapRestartTimer) return;
  state.tapRestartTimer = setTimeout(() => {
    state.tapRestartTimer = null;
    state.tapWorkerDead = false;
    ensureWorker();
  }, RESTART_MS);
  state.tapRestartTimer.unref();
}

function ensureWorker() {
  ensureTapState();
  if (state.tapWorker && !state.tapWorkerDead) return state.tapWorker;
  if (state.tapWorkerDead) return null; // waiting for restart backoff
  try {
    const w = new Worker(WORKER_PATH);
    w.unref(); // don't let the worker alone keep the process alive (proxy's HTTP server does); lets tests exit
    state.tapWorker = w;
    w.on('message', onWorkerMessage);
    w.on('error', () => { killWorker(); scheduleRestart(); });
    w.on('exit', (code) => { if (code !== 0) { killWorker(); scheduleRestart(); } });
    return w;
  } catch {
    state.tapWorkerDead = true;
    scheduleRestart();
    return null;
  }
}

function postToWorker(msg, transfer) {
  ensureTapState();
  const w = ensureWorker();
  if (!w || state.tapWorkerDead) return false;
  try { w.postMessage(msg, transfer || []); return true; }
  catch { killWorker(); scheduleRestart(); return false; }
}

class ChatTap {
  constructor(session, { stream, onFinalize, shape = 'openai' }) {
    ensureTapState();
    this.session = session;
    this.shape = shape;
    this.stream = stream;
    this.onFinalize = onFinalize || null;
    this.id = (session && session.id) || randomUUID(); // production sessions have id; tests may not
    this.batch = [];        // chunk COPIES pending transfer to the worker
    this.batchBytes = 0;
    this.flushTimer = null;
    this.ended = false;
    if (session && session.model) {
      postToWorker({ op: 'init', id: this.id, shape, model: session.model, stream });
    }
  }

  // O(1) on the byte path: count bytes + enqueue the chunk REF for the worker.
  // The copy into a transferable buffer is deferred to _flush (off the
  // synchronous transform path) — mirroring the scan-only tap's deferred
  // drain, so per-chunk main-thread cost stays O(1). Safe because undici does
  // not reuse chunk buffers across reads (the scan-only deferred drain already
  // relied on this). Bytes are forwarded to res by the Transform regardless.
  onChunk(buf) {
    if (!this.session || !buf || buf.length === 0) return;
    this.session.bytes += buf.length;
    if (state.tapWorkerDead) return; // telemetry degraded; bytes still counted
    this.batch.push(buf);
    this.batchBytes += buf.length;
    if (this.batchBytes >= FLUSH_BYTES) this._flush();
    else if (!this.flushTimer) this.flushTimer = setTimeout(() => this._flush(), FLUSH_MS);
  }

  _flush() {
    this.flushTimer = null;
    if (!this.batch.length) return;
    const refs = this.batch; this.batch = []; this.batchBytes = 0;
    // Copy each ref into a non-pool ArrayBuffer (transferable) and transfer
    // zero-copy to the worker. The originals are already in flight to res.
    const bufs = new Array(refs.length);
    const transfer = new Array(refs.length);
    for (let i = 0; i < refs.length; i++) {
      const copy = Buffer.allocUnsafeSlow(refs[i].length);
      copy.set(refs[i]);
      bufs[i] = copy;
      transfer[i] = copy.buffer;
    }
    if (!postToWorker({ op: 'chunk', id: this.id, bufs }, transfer)) {
      // worker unavailable: batch dropped (telemetry lost), bytes already counted
    }
  }

  // Finalize via a worker round-trip. Resolves once the worker has processed
  // all batches and returned final counts + decoded responseContent. If the
  // worker is dead/slow, resolves with null so the caller finalizes with
  // main-only data (bytes) — the request never hangs.
  async onEnd() {
    ensureTapState();
    if (!this.session) return;
    this.ended = true;
    if (this.flushTimer) { clearTimeout(this.flushTimer); this.flushTimer = null; }
    this._flush();
    let fields = null;
    if (!state.tapWorkerDead) {
      fields = await new Promise((resolve) => {
        const timer = setTimeout(() => {
          if (state.tapPendingFinals.get(this.id)?.resolve === resolve) {
            state.tapPendingFinals.delete(this.id);
          }
          resolve(null);
        }, FINAL_TIMEOUT_MS);
        state.tapPendingFinals.set(this.id, { resolve, timer });
        if (!postToWorker({ op: 'end', id: this.id })) {
          // worker died between the check and the post — resolve now
          clearTimeout(timer);
          state.tapPendingFinals.delete(this.id);
          resolve(null);
        }
      });
    }
    if (fields) Object.assign(this.session, fields); // chars, outputTokens, ..., responseContent
    if (this.onFinalize) { try { this.onFinalize(this.session); } catch {} }
    this.session = null;
  }
}

// Node Transform wrapping a ChatTap for the streaming pipe path. Each chunk
// is forwarded to res unmodified; tap.onChunk batches a copy for the worker.
function createTapStream(tap) {
  return new Transform({
    transform(chunk, encoding, callback) {
      try { tap.onChunk(chunk); } catch {}
      callback(null, chunk);
    },
  });
}

module.exports = { ChatTap, createTapStream };
