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
const { SCALING_DEFAULTS } = require('./config');
const { logError } = require('./http');

const WORKER_PATH = path.join(__dirname, 'tap-worker.js');
const FLUSH_BYTES = 64 * 1024; // flush a batch when it reaches this size
const FLUSH_MS = 20;           // ...or after this long (latency cap for live tok/s)
const FINAL_TIMEOUT_MS = 1000; // give the worker this long to finalize before giving up
const RESTART_MS = 1000;       // backoff before recreating a dead worker
const TAP_PENDING_BYTES_MAX = SCALING_DEFAULTS.tapPendingBytes;
const GLOBAL_PENDING_BYTES_MAX = SCALING_DEFAULTS.globalTapPendingBytes;
const MAX_TPS_SAMPLES = SCALING_DEFAULTS.tpsSamples;

function configuredBudget(name, fallback) {
  const value = Number(state.config?.limits?.[name]);
  return Number.isSafeInteger(value) && value >= 0 ? value : fallback;
}

function ensureTapState() {
  if (!(state.tapPendingFinals instanceof Map)) state.tapPendingFinals = new Map();
  if (!Object.prototype.hasOwnProperty.call(state, 'tapWorker')) state.tapWorker = null;
  if (typeof state.tapWorkerDead !== 'boolean') state.tapWorkerDead = false;
  if (!Object.prototype.hasOwnProperty.call(state, 'tapRestartTimer')) state.tapRestartTimer = null;
  if (!(state.tapActiveTaps instanceof Map)) state.tapActiveTaps = new Map();
  if (!(state.tapPendingTaps instanceof Map)) state.tapPendingTaps = new Map();
  if (!Number.isFinite(state.tapPendingBytes) || state.tapPendingBytes < 0) state.tapPendingBytes = 0;
}

ensureTapState();

function onWorkerMessage(msg) {
  ensureTapState();
  const { op, id } = msg;
  if (op === 'ack') {
    const tap = state.tapPendingTaps.get(id);
    if (tap) tap.acknowledge(msg.bytes);
    return;
  }
  if (op === 'sync') {
    const s = state.sessions.get(id);
    if (s && msg.fields) {
      Object.assign(s, msg.fields); // update live counts
      // Feed the rolling-TPS sample window (sessions.js sessionTps reads this).
      if (typeof msg.fields.outputTokens === 'number') {
        const samples = s.tpsSamples || (s.tpsSamples = []);
        const now = Date.now();
        samples.push({ ts: now, tok: msg.fields.outputTokens });
        const cutoff = now - state.TPS_WINDOW_MS;
        // Prune by advancing a head index (O(1)) instead of shift() (O(n)
        // reindex per sync). sessionTps reads only [head, length), so expired
        // entries are skipped without a per-sync reindex; compaction splices
        // the dead head periodically so it can't grow unbounded.
        let h = s.tpsHead || 0;
        while (samples.length - h > 1 && samples[h].ts < cutoff) h++;
        if (h || samples.length > configuredBudget('tpsSamples', MAX_TPS_SAMPLES)) {
          const trim = Math.max(h, samples.length - configuredBudget('tpsSamples', MAX_TPS_SAMPLES));
          if (trim > 0) samples.splice(0, trim);
          s.tpsHead = Math.max(0, h - trim);
        }
      }
    }
  } else if (op === 'final') {
    const pending = state.tapPendingFinals;
    if (pending instanceof Map) {
      const p = pending.get(id);
      if (p) { pending.delete(id); clearTimeout(p.timer); p.resolve(msg.fields); }
    }
  } else if (op === 'error') {
    // Worker reported an uncaught exception. It may be in a bad state — restart
    // it so subsequent requests get a clean parser instead of silently dropping
    // all content (which empties responseContent → breaks coalescing + tok/s).
    logError('tap worker reported error', new Error(msg.message || 'unknown'));
    killWorker();
    scheduleRestart();
  }
}

function killWorker() {
  ensureTapState();
  state.tapWorkerDead = true;
  const w = state.tapWorker;
  state.tapWorker = null;
  if (w) { try { w.terminate(); } catch {} }
  for (const tap of new Set(state.tapPendingTaps.values())) tap.releaseAllPending();
  state.tapPendingTaps.clear();
  state.tapPendingBytes = 0;
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
    w.on('exit', () => { killWorker(); scheduleRestart(); }); // any exit = dead; restart (code 0 leaves a dangling handle otherwise)
    // C8: the new worker starts with empty parsers. Re-init for every
    // still-active tap so in-flight streams recover telemetry (bytes received
    // during the dead window are lost by design; this fixes the rest of the
    // stream). init is posted before any chunk, so FIFO ordering holds.
    for (const [id, entry] of state.tapActiveTaps) {
      // Entries created before this bounded-credit version contain only
      // { shape, model, stream }. Reinitialize them too; they have no shared
      // discard flag but must not break a live hot reload.
      if (!entry || entry.active === false || entry.tap?.discarded) continue;
      try {
        w.postMessage({
          op: 'init',
          id,
          shape: entry.shape,
          model: entry.model,
          stream: entry.stream,
          discardBuffer: entry.tap?.discardSignal?.buffer,
          modelRatioLimit: configuredBudget('modelRatios', SCALING_DEFAULTS.modelRatios),
        });
      } catch {}
    }
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
    this.pendingBytes = 0;  // batch + transferred bytes awaiting worker acknowledgement
    this.discardSignal = new Int32Array(new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT));
    this.discarded = false;
    this.telemetryDropped = false;
    this.flushTimer = null;
    this.ended = false;
    if (session && session.model) {
      // Keep only active parsers here. Outstanding credit is tracked separately
      // so a finalized/discarded tap cannot be reinitialized after a restart.
      state.tapActiveTaps.set(this.id, { tap: this, shape, model: session.model, stream, active: true });
      postToWorker({ op: 'init', id: this.id, shape, model: session.model, stream, modelRatioLimit: configuredBudget('modelRatios', SCALING_DEFAULTS.modelRatios), discardBuffer: this.discardSignal.buffer });
    }
  }

  reservePending(bytes) {
    if (bytes <= 0 || this.pendingBytes + bytes > configuredBudget('tapPendingBytes', TAP_PENDING_BYTES_MAX) || state.tapPendingBytes + bytes > configuredBudget('globalTapPendingBytes', GLOBAL_PENDING_BYTES_MAX)) return false;
    this.pendingBytes += bytes;
    state.tapPendingBytes += bytes;
    state.tapPendingTaps.set(this.id, this);
    return true;
  }

  releasePending(bytes) {
    const released = Math.min(this.pendingBytes, Math.max(0, Number(bytes) || 0));
    if (!released) return;
    this.pendingBytes -= released;
    state.tapPendingBytes = Math.max(0, state.tapPendingBytes - released);
    if (this.pendingBytes === 0 && this.ended) state.tapPendingTaps.delete(this.id);
  }

  releaseAllPending() {
    this.releasePending(this.pendingBytes);
    state.tapPendingTaps.delete(this.id);
  }

  acknowledge(bytes) {
    this.releasePending(bytes);
  }

  dropBatch() {
    if (this.flushTimer) { clearTimeout(this.flushTimer); this.flushTimer = null; }
    const bytes = this.batchBytes;
    this.batch = [];
    this.batchBytes = 0;
    this.releasePending(bytes);
  }

  // Stop optional parsing immediately. The shared flag is visible to the
  // worker even for chunk messages already queued on its MessagePort, so it
  // acknowledges and releases them without parsing obsolete telemetry.
  discard() {
    if (this.discarded) return;
    this.discarded = true;
    Atomics.store(this.discardSignal, 0, 1);
    this.dropBatch();
    state.tapActiveTaps.delete(this.id);
    postToWorker({ op: 'discard', id: this.id });
  }

  // O(1) on the byte path: count bytes + enqueue the chunk REF for the worker.
  // The copy into a transferable buffer is deferred to _flush (off the
  // synchronous transform path). Bytes are forwarded to res regardless.
  onChunk(buf) {
    if (!this.session || !buf || buf.length === 0) return;
    this.session.bytes += buf.length;
    if (this.discarded || state.tapWorkerDead || !this.reservePending(buf.length)) {
      this.telemetryDropped = true;
      return;
    }
    this.batch.push(buf);
    this.batchBytes += buf.length;
    if (this.batchBytes >= FLUSH_BYTES) this._flush();
    else if (!this.flushTimer) this.flushTimer = setTimeout(() => this._flush(), FLUSH_MS);
  }

  _flush() {
    this.flushTimer = null;
    if (!this.batch.length) return;
    if (this.discarded) { this.dropBatch(); return; }
    const refs = this.batch;
    const total = this.batchBytes;
    this.batch = [];
    this.batchBytes = 0;
    // One non-pool ArrayBuffer holds the whole batch: a single allocation plus
    // a single memcpy pass replaces per-chunk allocations. The originals are
    // already in flight to res, so they cannot be transferred.
    try {
      const merged = Buffer.allocUnsafeSlow(total);
      let offset = 0;
      for (let i = 0; i < refs.length; i++) { refs[i].copy(merged, offset); offset += refs[i].length; }
      if (!postToWorker({ op: 'chunk', id: this.id, bufs: [merged], bytes: total }, [merged.buffer])) this.releasePending(total);
    } catch {
      this.telemetryDropped = true;
      this.releasePending(total);
    }
  }

  // Finalize via a worker round-trip. If local telemetry was saturated or the
  // stream was destroyed, abandon parser state rather than delaying teardown
  // to parse a partial queue; byte accounting remains exact on the main thread.
  async onEnd() {
    ensureTapState();
    if (!this.session || this.ended) return;
    this.ended = true;
    const active = state.tapActiveTaps.get(this.id);
    if (active) active.active = false;
    if (this.flushTimer) { clearTimeout(this.flushTimer); this.flushTimer = null; }
    this._flush();
    let fields = null;
    if (this.telemetryDropped || this.discarded || state.tapWorkerDead) {
      this.discard();
    } else {
      fields = await new Promise((resolve) => {
        const timer = setTimeout(() => {
          const pending = state.tapPendingFinals;
          if (pending instanceof Map && pending.get(this.id)?.resolve === resolve) pending.delete(this.id);
          // The worker may still be behind this finalization. Stop scanning its
          // queued bytes; its acknowledgements will release their bounded credit.
          this.discard();
          resolve(null);
        }, FINAL_TIMEOUT_MS);
        state.tapPendingFinals.set(this.id, { resolve, timer });
        if (!postToWorker({ op: 'end', id: this.id })) {
          clearTimeout(timer);
          state.tapPendingFinals.delete(this.id);
          this.discard();
          resolve(null);
        }
      });
    }
    state.tapActiveTaps.delete(this.id);
    if (fields) Object.assign(this.session, fields);
    if (this.onFinalize) { try { this.onFinalize(this.session); } catch (err) { logError('ChatTap onFinalize callback failed', err); } }
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
    destroy(err, callback) {
      // Normal completion has no error and still needs the final worker parse.
      // An aborted/error pipeline has no client waiting for optional telemetry.
      if (err) tap.discard();
      callback(err);
    },
  });
}

// Force-terminate the tap worker and schedule a restart. Called from the
// reload path so the new worker registers the NEW onWorkerMessage (with the
// op:error + exit-code-0 fixes) instead of keeping stale closures from the
// previous code. Without this, a dead worker handle lingers in state.tapWorker
// and the first post-reload request loses all content before the restart.
function restartTapWorker() { killWorker(); scheduleRestart(); }

module.exports = { ChatTap, createTapStream, restartTapWorker, TAP_PENDING_BYTES_MAX, GLOBAL_PENDING_BYTES_MAX, MAX_TPS_SAMPLES, onWorkerMessage };
