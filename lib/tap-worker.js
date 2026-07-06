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

// ---- worker-local char->token ratio (was state.modelCharRatio on main) ----
const modelCharRatio = new Map();
function estimateTokensFromChars(model, chars) {
  const r = modelCharRatio.get(model);
  if (r && r.chars > 0) return Math.round(chars * (r.tokens / r.chars));
  return Math.floor(chars / 4);
}
function updateModelRatio(model, chars, tokens) {
  if (!model || chars <= 0 || tokens <= 0) return;
  const r = modelCharRatio.get(model) || { chars: 0, tokens: 0 };
  r.chars += chars;
  r.tokens += tokens;
  modelCharRatio.set(model, r);
}

class TapParser {
  constructor({ shape, model, stream }) {
    this.shape = shape;
    this.stream = stream;
    this.model = model;
    this.lineBuf = '';
    this.contentParts = [];
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

  feed(buf) {
    if (!buf || buf.length === 0) return;
    const str = this.decoder.decode(buf, { stream: true });
    if (this.stream) {
      this.lineBuf += str;
      let pos = 0, idx;
      while ((idx = this.lineBuf.indexOf('\n', pos)) >= 0) {
        let end = idx;
        if (end > pos && this.lineBuf.charCodeAt(end - 1) === 13) end--;
        this._handleSseLine(this.lineBuf.slice(pos, end));
        pos = idx + 1;
      }
      if (pos > 0) this.lineBuf = pos < this.lineBuf.length ? this.lineBuf.slice(pos) : '';
    } else if (this.jsonParser) {
      this.jsonParser.push(str);
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
      this.lineBuf += this.decoder.decode(); // flush any trailing incomplete bytes
      const line = this.lineBuf.trim();
      this.lineBuf = '';
      if (line) this._handleSseLine(line);
    }
    if (this.stream && this.shape !== 'anthropic' && this.contentParts.length) {
      // One join + one JSON.parse decodes every part's escape sequences in a
      // single pass, replacing the per-part parse + O(n^2) `out +=` loop. Each
      // part is a JSON-string-body fragment (escapes intact, sliced at quote
      // boundaries), so `["p1","p2",...]` is a valid JSON array of strings.
      try {
        this.responseContent = JSON.parse('["' + this.contentParts.join('","') + '"]').join('');
      } catch {
        this.responseContent = this.contentParts.join('');
      }
      this.contentParts.length = 0;
    }
    if (!this.stream && this.jsonParser) {
      this.jsonParser.flush();
      if (!this.exactTokens && this.chars > 0) {
        this.outputTokens = estimateTokensFromChars(this.model, this.chars);
      }
    }
    return { ...this.fields(), responseContent: this.responseContent };
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
        if (pushRaw && closeQuote > start) this.contentParts.push(data.slice(start, closeQuote));
        j = closeQuote;
      } else {
        // Fragment ended mid-value (split across chunks) — count the remainder.
        total += n - j;
        if (pushRaw && n > start) this.contentParts.push(data.slice(start, n));
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
        this.responseContent += delta.text;
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
        this.responseContent += value; this.chars += value.length;
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
        this.responseContent += value; this.chars += value.length;
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

parentPort.on('message', (msg) => {
  const { op, id } = msg;
  if (op === 'init') {
    parsers.set(id, new TapParser({ shape: msg.shape, model: msg.model, stream: msg.stream }));
    lastSync.set(id, 0);
  } else if (op === 'chunk') {
    const p = parsers.get(id);
    if (!p) return;
    const bufs = msg.bufs;
    // New tap sends one merged buffer (bufs.length === 1) — feed it directly,
    // skipping the second Buffer.concat copy. In-flight taps from before a hot
    // reload still post multi-buffer arrays; concat those (legacy path).
    if (bufs && bufs.length) p.feed(bufs.length === 1 ? bufs[0] : Buffer.concat(bufs));
    const now = Date.now();
    if (now - (lastSync.get(id) || 0) >= SYNC_MIN_MS) {
      lastSync.set(id, now);
      parentPort.postMessage({ op: 'sync', id, fields: p.fields() });
    }
  } else if (op === 'end') {
    const p = parsers.get(id);
    let fields = null;
    if (p) { fields = p.finish(); parsers.delete(id); lastSync.delete(id); }
    parentPort.postMessage({ op: 'final', id, fields });
  }
});

process.on('uncaughtException', (err) => {
  try { parentPort.postMessage({ op: 'error', message: String(err && err.message || err) }); } catch {}
});
