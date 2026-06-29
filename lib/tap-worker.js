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
      let out = '';
      for (let k = 0; k < this.contentParts.length; k++) {
        try { out += JSON.parse('"' + this.contentParts[k] + '"'); } catch {}
      }
      this.responseContent = out;
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
        return; // a real usage chunk carries no content to scan
      }
      // else: the substring matched but this isn't a usage chunk — fall through
      // and scan it for content. Fixes content loss when a content delta's
      // value (or a non-standard field) literally contains the text "usage".
    }
    if (this.exactTokens) return;
    const chars = this._scanStringLen(data, '"content":"', true)
                + this._scanStringLen(data, '"reasoning_content":"', false);
    if (chars > 0) {
      if (!this.firstTokenAt) this.firstTokenAt = Date.now();
      this.chars += chars;
      this.outputTokens = estimateTokensFromChars(this.model, this.chars);
    }
  }

  _scanStringLen(data, key, pushRaw) {
    const keyLen = key.length;
    const n = data.length;
    let i = data.indexOf(key);
    let total = 0;
    while (i >= 0) {
      const start = i + keyLen;
      let j = start;
      while (j < n) {
        const c = data.charCodeAt(j);
        if (c === 34) break;
        if (c === 92) {
          if (data.charCodeAt(j + 1) === 117) j += 6;
          else j += 2;
          total++;
        } else { total++; j++; }
      }
      if (pushRaw && j > start) this.contentParts.push(data.slice(start, j));
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
    const bufs = msg.bufs || [];
    if (bufs.length) p.feed(Buffer.concat(bufs));
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
