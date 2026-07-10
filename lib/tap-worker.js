'use strict';

// Worker thread that owns ALL tap parse state + logic, off the main event
// loop. The main-thread ChatTap (lib/chat-tap.js) is a thin shim: it counts
// bytes (O(1), for the live snapshot), batches chunk COPIES, and transfers
// them here. This worker runs the SSE/JSON scan — the dominant tap cost — on
// its own core, so the pipe core is freed to forward bytes.
//
// Protocol (main <-> worker), keyed by session id:
//   main -> worker: { op:'init', id, shape, model, stream }
//                  : { op:'chunk', id, bufs }      (bufs transferred, zero-copy)
//                  : { op:'end', id }
//   worker -> main: { op:'sync',  id, fields }    (throttled ~100ms; no responseContent)
//                  : { op:'final', id, fields }     (fields incl. responseContent)

const { parentPort } = require('node:worker_threads');
const { JsonStream } = require('./json-stream');
const { SCALING_DEFAULTS } = require('./config');

const MAX_SSE_LINE_CHARS = 256 * 1024;
const MAX_RESPONSE_CONTENT_CHARS = 256 * 1024;
let modelRatioLimit = SCALING_DEFAULTS.modelRatios;
// Worker-local char→token ratio (was state.modelCharRatio on main).
const modelCharRatio = new Map();
function setModelRatioLimit(value) {
  const next = Number(value);
  if (!Number.isSafeInteger(next) || next < 0) return;
  modelRatioLimit = next;
  while (modelCharRatio.size > modelRatioLimit) modelCharRatio.delete(modelCharRatio.keys().next().value);
}
function estimateTokensFromChars(model, chars) {
  const r = modelCharRatio.get(model);
  return r && r.chars > 0 ? Math.round(chars * (r.tokens / r.chars)) : Math.floor(chars / 4);
}
function updateModelRatio(model, chars, tokens) {
  if (!model || chars <= 0 || tokens <= 0) return;
  let r = modelCharRatio.get(model);
  if (!r) {
    if (modelCharRatio.size >= modelRatioLimit) return;
    r = { chars: 0, tokens: 0 };
  }
  r.chars += chars;
  r.tokens += tokens;
  modelCharRatio.set(model, r);
}

class TapParser {
  constructor({ shape, model, stream, discardBuffer }) {
    this.shape = shape;
    this.stream = stream;
    this.model = model;
    this.discardSignal = discardBuffer ? new Int32Array(discardBuffer) : null;
    this.lineBuf = '';
    this.lineDiscarding = false;
    this.contentParts = [];
    this.responseParts = [];
    this.capturedResponseChars = 0;
    this.contentCaptureDisabled = false;
    this.chars = 0;
    this.outputTokens = 0;
    this.exactTokens = false;
    this.promptTokens = 0;
    this.cachedTokens = 0;
    this.completionTokens = 0;
    this.firstTokenAt = null;
    this.responseContent = '';
    this.decoder = new TextDecoder('utf-8'); // streaming: carries split multibyte chars across feed() calls
    if (!stream) {
      this.jsonParser = new JsonStream({ onValue: (p, v) => this._onJsonValue(p, v) });
      this._got = { usage: false, content: false };
    }
  }

  isDiscarded() {
    return this.discardSignal && Atomics.load(this.discardSignal, 0) !== 0;
  }

  feed(buf) {
    if (!buf || buf.length === 0) return;
    const str = this.decoder.decode(buf, { stream: true });
    if (this.stream) this._feedSseString(str);
    else if (this.jsonParser) this.jsonParser.push(str);
  }

  _feedSseString(str) {
    let pos = 0;
    while (pos < str.length) {
      const newline = str.indexOf('\n', pos);
      const end = newline >= 0 ? newline : str.length;
      if (!this.lineDiscarding) {
        const room = MAX_SSE_LINE_CHARS - this.lineBuf.length;
        if (end - pos > room) {
          // This cannot be a valid bounded telemetry record. Drop it until its
          // terminator, then resume parsing later lines without retaining it.
          this.lineBuf = '';
          this.lineDiscarding = true;
        } else {
          this.lineBuf += str.slice(pos, end);
        }
      }
      if (newline < 0) break;
      if (!this.lineDiscarding) {
        const line = this.lineBuf.endsWith('\r') ? this.lineBuf.slice(0, -1) : this.lineBuf;
        this._handleSseLine(line);
      }
      this.lineBuf = '';
      this.lineDiscarding = false;
      pos = newline + 1;
    }
  }

  fields() {
    return {
      chars: this.chars,
      outputTokens: this.outputTokens,
      exactTokens: this.exactTokens,
      promptTokens: this.promptTokens,
      cachedTokens: this.cachedTokens,
      completionTokens: this.completionTokens,
      firstTokenAt: this.firstTokenAt,
    };
  }

  finish() {
    if (this.stream) {
      this._feedSseString(this.decoder.decode()); // flush trailing incomplete UTF-8 bytes
      if (!this.lineDiscarding) {
        const line = this.lineBuf.trim();
        if (line) this._handleSseLine(line);
      }
      this.lineBuf = '';
      this.lineDiscarding = false;
    } else if (this.jsonParser) {
      this.jsonParser.flush();
      if (!this.exactTokens && this.chars > 0) this.outputTokens = estimateTokensFromChars(this.model, this.chars);
    }
    if (!this.contentCaptureDisabled && this.stream && this.shape !== 'anthropic' && this.contentParts.length) {
      // One join + one JSON.parse decodes every part's escape sequences in a
      // single pass. Parts are JSON-string-body fragments (escapes intact).
      try {
        this.responseContent = JSON.parse('["' + this.contentParts.join('","') + '"]').join('');
      } catch {
        this.responseContent = this.contentParts.join('');
      }
    } else if (!this.contentCaptureDisabled) {
      this.responseContent = this.responseParts.join('');
    }
    this.contentParts.length = 0;
    this.responseParts.length = 0;
    return { ...this.fields(), responseContent: this.responseContent };
  }

  _disableContentCapture() {
    this.contentCaptureDisabled = true;
    this.contentParts.length = 0;
    this.responseParts.length = 0;
    this.responseContent = '';
  }

  _captureContent(part, raw) {
    if (!part || this.contentCaptureDisabled) return;
    if (this.capturedResponseChars + part.length > MAX_RESPONSE_CONTENT_CHARS) {
      // Never coalesce against a partial assistant response: preserve counters
      // but discard all captured text once the optional capture is saturated.
      this._disableContentCapture();
      return;
    }
    this.capturedResponseChars += part.length;
    (raw ? this.contentParts : this.responseParts).push(part);
  }

  _handleSseLine(line) {
    if (!line.startsWith('data:')) return;
    const data = line.slice(5).trim();
    if (!data || data === '[DONE]') return;
    if (this.shape === 'anthropic') {
      let chunk; try { chunk = JSON.parse(data); } catch { return; }
      return this._handleAnthropicEvent(chunk);
    }
    // Once exact tokens arrived (a real usage chunk), skip all scanning — the
    // usage chunk carried the final counts and later content deltas don't move
    // telemetry. This short-circuits the post-usage tail to O(1) per line.
    if (this.exactTokens) return;
    // Common case first: content + reasoning. A content/reasoning delta carries
    // no usage, so finding either returns without the third indexOf pass the old
    // usage-first ordering ran on every line.
    const chars = this._scanStringLen(data, '"content"', true)
                + this._scanStringLen(data, '"reasoning_content"', false);
    if (chars > 0) {
      if (!this.firstTokenAt) this.firstTokenAt = Date.now();
      this.chars += chars;
      this.outputTokens = estimateTokensFromChars(this.model, this.chars);
      return;
    }
    // No content found — maybe a usage chunk. The substring guard avoids a full
    // JSON.parse on plain deltas; a content delta whose value literally contains
    // the text "usage" (the A3 case) is already counted above via the content
    // scan, so it no longer pays the wasted usage parse either.
    if (data.indexOf('"usage"') >= 0) {
      let chunk = null;
      try { chunk = JSON.parse(data); } catch {}
      const u = chunk?.usage;
      if (u && (u.completion_tokens != null || u.output_tokens != null)) {
        const t = Number(u.completion_tokens ?? u.output_tokens ?? 0) || 0;
        this.outputTokens = t;
        this.completionTokens = t;
        this.exactTokens = true;
        this.promptTokens = Number(u.prompt_tokens ?? u.input_tokens ?? 0) || 0;
        this.cachedTokens = Number(u.prompt_tokens_details?.cached_tokens ?? u.cached_tokens ?? u.tokens_cached_read ?? 0) || 0;
        updateModelRatio(this.model, this.chars, t);
      }
    }
  }

  _scanStringLen(data, key, pushRaw) {
    const keyLen = key.length;
    const n = data.length;
    let i = data.indexOf(key);
    let total = 0;
    while (i >= 0) {
      // After the field name ("content"), skip : <whitespace> " to reach the
      // value's content start. Handles both "content":"…" and "content": "…"
      // (UMANS API serializes JSON with spaces after colons).
      let start = i + keyLen;
      while (start < n && (data[start] === ' ' || data[start] === '\t' || data[start] === '\n' || data[start] === '\r' || data[start] === ':')) start++;
      if (start >= n || data[start] !== '"') { i = data.indexOf(key, start); continue; }
      start++; // skip the opening quote of the value
      let j = start;
      // Fast path: skip plain runs via indexOf (one C++ call), handling only
      // escape characters per-char — mirrors json-stream.js. An unescaped " ends
      // the value; \X counts as one char, \uXXXX as one char.
      let closeQuote = -1;
      while (j < n) {
        const nextQuote = data.indexOf('"', j);
        const nextSlash = data.indexOf('\\', j);
        if (nextQuote < 0 && nextSlash < 0) break; // no terminator in this fragment
        if (nextQuote >= 0 && (nextSlash < 0 || nextQuote < nextSlash)) { closeQuote = nextQuote; break; }
        // Escape: count the plain run up to it, then the escape as one char.
        total += nextSlash - j + 1;
        j = data.charCodeAt(nextSlash + 1) === 117 ? nextSlash + 6 : nextSlash + 2;
      }
      if (closeQuote >= 0) {
        total += closeQuote - j;
        if (pushRaw && closeQuote > start) this._captureContent(data.slice(start, closeQuote), true);
        j = closeQuote;
      } else {
        // Fragment ended mid-value (split across chunks) — count the remainder.
        total += n - j;
        if (pushRaw && n > start) this._captureContent(data.slice(start, n), true);
        j = n;
      }
      i = data.indexOf(key, j + 1);
    }
    return total;
  }

  _handleAnthropicEvent(chunk) {
    const type = chunk?.type;
    if (type === 'message_start') {
      const u = chunk?.message?.usage;
      if (u) {
        this.promptTokens = Number(u.input_tokens ?? 0) || 0;
        this.cachedTokens = Number(u.cache_read_input_tokens ?? 0) || 0;
      }
      return;
    }
    if (type === 'message_delta') {
      const u = chunk?.usage;
      if (u && u.output_tokens != null) {
        const t = Number(u.output_tokens) || 0;
        this.outputTokens = t;
        this.completionTokens = t;
        this.exactTokens = true;
        if (u.input_tokens != null) {
          this.promptTokens = Number(u.input_tokens) || 0;
          this.cachedTokens = Number(u.cache_read_input_tokens ?? 0) || 0;
        }
        updateModelRatio(this.model, this.chars, t);
      }
      return;
    }
    if (this.exactTokens) return;
    if (type === 'content_block_delta') {
      const delta = chunk?.delta || {};
      let chars = 0;
      if (delta.type === 'text_delta' && typeof delta.text === 'string') {
        chars += delta.text.length;
        this._captureContent(delta.text, false);
      } else if (delta.type === 'thinking_delta' && typeof delta.thinking === 'string') {
        chars += delta.thinking.length;
      }
      if (chars > 0) {
        if (!this.firstTokenAt) this.firstTokenAt = Date.now();
        this.chars += chars;
        this.outputTokens = estimateTokensFromChars(this.model, this.chars);
      }
    }
  }

  _onJsonValue(path, value) {
    const p = path.join('.');
    if (this.shape === 'openai') {
      if (p === 'usage.completion_tokens' || p === 'usage.output_tokens') {
        const exact = Number(value) || 0;
        this.outputTokens = exact; this.completionTokens = exact; this.exactTokens = true;
        updateModelRatio(this.model, this.chars, exact);
        this._got.usage = true; this._checkDone(); return;
      }
      if (p === 'usage.prompt_tokens' || p === 'usage.input_tokens') { this.promptTokens = Number(value) || 0; return; }
      if (p === 'usage.prompt_tokens_details.cached_tokens' || p === 'usage.cached_tokens' || p === 'usage.tokens_cached_read') { this.cachedTokens = Number(value) || 0; return; }
      if (p.endsWith('.message.content') && typeof value === 'string') {
        this._captureContent(value, false); this.chars += value.length;
        if (!this.firstTokenAt) this.firstTokenAt = Date.now();
        if (!this.exactTokens) this.outputTokens = estimateTokensFromChars(this.model, this.chars);
        this._got.content = true; this._checkDone(); return;
      }
      if (p.endsWith('.message.reasoning_content') && typeof value === 'string') {
        this.chars += value.length;
        if (!this.exactTokens) this.outputTokens = estimateTokensFromChars(this.model, this.chars);
        return;
      }
    } else {
      if (p === 'usage.output_tokens') {
        const exact = Number(value) || 0;
        this.outputTokens = exact; this.completionTokens = exact; this.exactTokens = true;
        updateModelRatio(this.model, this.chars, exact);
        this._got.usage = true; this._checkDone(); return;
      }
      if (p === 'usage.input_tokens') { this.promptTokens = Number(value) || 0; return; }
      if (p === 'usage.cache_read_input_tokens') { this.cachedTokens = Number(value) || 0; return; }
      if (p.endsWith('.text') && typeof value === 'string' && this._currentBlockType === 'text') {
        this._captureContent(value, false); this.chars += value.length;
        if (!this.firstTokenAt) this.firstTokenAt = Date.now();
        if (!this.exactTokens) this.outputTokens = estimateTokensFromChars(this.model, this.chars);
        this._got.content = true; this._checkDone(); return;
      }
      if (p.endsWith('.thinking') && typeof value === 'string' && this._currentBlockType === 'thinking') {
        this.chars += value.length;
        if (!this.exactTokens) this.outputTokens = estimateTokensFromChars(this.model, this.chars);
        return;
      }
      if (p.endsWith('.type') && (value === 'text' || value === 'thinking')) { this._currentBlockType = value; return; }
    }
  }

  _checkDone() {
    if (this._got.usage && this._got.content) this.jsonParser.done = true;
  }
}

// ---- message handling ----
const parsers = new Map();
const lastSync = new Map();
const SYNC_MIN_MS = 100;

function handleMessage(msg) {
  const { op, id } = msg;
  if (op === 'init') {
    setModelRatioLimit(msg.modelRatioLimit);
    parsers.set(id, new TapParser({ shape: msg.shape, model: msg.model, stream: msg.stream, discardBuffer: msg.discardBuffer }));
    lastSync.set(id, 0);
  } else if (op === 'chunk') {
    const bufs = msg.bufs;
    const bytes = Number.isFinite(msg.bytes)
      ? Math.max(0, msg.bytes)
      : (bufs || []).reduce((total, buf) => total + (buf?.length || 0), 0);
    const p = parsers.get(id);
    try {
      // The shared discard flag skips parser work even when this message was
      // queued before the main thread observed an abort/finalization timeout.
      if (p && !p.isDiscarded() && bufs && bufs.length) {
        // New taps send one merged buffer; hot-reload legacy messages may carry
        // several and need one concat before feed.
        p.feed(bufs.length === 1 ? bufs[0] : Buffer.concat(bufs));
        const now = Date.now();
        if (now - (lastSync.get(id) || 0) >= SYNC_MIN_MS) {
          lastSync.set(id, now);
          parentPort.postMessage({ op: 'sync', id, fields: p.fields() });
        }
      }
    } finally {
      // Credit is returned only after this transferred buffer is no longer
      // retained by the worker message handler.
      parentPort.postMessage({ op: 'ack', id, bytes });
    }
  } else if (op === 'discard') {
    parsers.delete(id);
    lastSync.delete(id);
  } else if (op === 'end') {
    const p = parsers.get(id);
    let fields = null;
    if (p && !p.isDiscarded()) fields = p.finish();
    parsers.delete(id);
    lastSync.delete(id);
    parentPort.postMessage({ op: 'final', id, fields });
  }
}

if (parentPort) parentPort.on('message', handleMessage);

process.on('uncaughtException', (err) => {
  try { if (parentPort) parentPort.postMessage({ op: 'error', message: String(err && err.message || err) }); } catch {}
});

module.exports = { TapParser, MAX_SSE_LINE_CHARS, MAX_RESPONSE_CONTENT_CHARS };
