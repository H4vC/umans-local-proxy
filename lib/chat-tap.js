'use strict';

const { JsonStream } = require('./json-stream');
const { estimateTokensFromChars, updateModelRatio } = require('./sessions');
const { Transform } = require('node:stream');

// Per-request stream/body tap. Parses SSE chunks (streaming) or a single
// JSON body (non-streaming) to count output tokens WITHOUT mutating bytes.
// ASYNC: onChunk is O(1) — it pushes the raw buffer onto a queue and
// schedules a drain via setImmediate. All parsing happens in _drain(),
// which runs between I/O ticks (between pipe flushes), so telemetry never
// delays bytes reaching the client.
//
// Non-streaming responses are parsed incrementally via JsonStream — no
// full-body buffer. Once all needed fields are extracted, the parser stops
// (parser.done = true) and remaining bytes are ignored.
class ChatTap {
  constructor(session, { stream, onFinalize, shape = 'openai' }) {
    this.session = session;
    this.stream = stream;
    this.shape = shape; // 'openai' | 'anthropic'
    this.onFinalize = onFinalize || null;
    this.lineBuf = '';     // partial SSE line buffer (streaming)
    this.queue = [];       // raw chunks pending async parse
    this.draining = false;
    this.ended = false;
    this.contentParts = []; // OpenAI streaming: raw content slices, decoded at _finish

    // Non-streaming: incremental JSON parser with early termination.
    if (!stream) {
      this.jsonParser = new JsonStream({ onValue: (path, value) => this._onJsonValue(path, value) });
      // Track what we've found for early termination.
      this._got = { usage: false, content: false };
    }
  }

  // O(1) enqueue — never blocks the read loop. Byte counting is trivial
  // integer addition; parsing is deferred to _drain().
  onChunk(buf) {
    if (!this.session || !buf || buf.length === 0) return;
    this.session.bytes += buf.length;
    this.queue.push(buf);
    if (!this.draining) {
      this.draining = true;
      setImmediate(() => this._drain());
    }
  }

  _drain() {
    while (this.queue.length) {
      const buf = this.queue.shift();
      // Decode the chunk to a UTF-8 string. Readable.fromWeb yields Uint8Array,
      // whose .toString('utf8') returns comma-separated bytes (broken) — wrap
      // in Buffer.from first. Buffer.from(uint8array) COPIES the chunk's bytes
      // into a Buffer we can safely decode: the copy is negligible (UTF-8
      // decode runs at ~14 GB/s, far above the tap ceiling) and avoids any risk
      // of a shared view into a pooled/reused upstream ArrayBuffer.
      const str = Buffer.from(buf).toString('utf8');
      if (this.stream) {
        this.lineBuf += str;
        // Cursor-based line scan: advance pos instead of re-slicing lineBuf
        // per line (which copies the remaining tail each iteration — O(n²)).
        let pos = 0, idx;
        while ((idx = this.lineBuf.indexOf('\n', pos)) >= 0) {
          let end = idx;
          if (end > pos && this.lineBuf.charCodeAt(end - 1) === 13) end--; // strip \r
          this._handleSseLine(this.lineBuf.slice(pos, end));
          pos = idx + 1;
        }
        if (pos > 0) this.lineBuf = pos < this.lineBuf.length ? this.lineBuf.slice(pos) : '';
      } else if (this.jsonParser) {
        // Incremental parse — stops early once we have all fields.
        this.jsonParser.push(str);
      }
    }
    this.draining = false;
    // If onEnd arrived while draining, finalize now that we're caught up.
    if (this.ended) this._finish();
  }

  // ---- Streaming SSE line handler ----
  // OpenAI: scans the raw JSON text for content/reasoning lengths and saves
  // raw content slices — no JSON.parse per delta (the dominant tap cost). The
  // usage chunk (one per stream) is still JSON.parsed for exact tokens.
  // Anthropic: every event carries a "type" discriminator, so parse all.
  _handleSseLine(line) {
    if (!line.startsWith('data:')) return;
    const data = line.slice(5).trim();
    if (!data || data === '[DONE]') return;
    if (this.shape === 'anthropic') {
      let chunk; try { chunk = JSON.parse(data); } catch { return; }
      return this._handleAnthropicEvent(chunk);
    }
    // ---- OpenAI ----
    if (data.indexOf('"usage"') >= 0) {
      let chunk; try { chunk = JSON.parse(data); } catch { return; }
      const u = chunk?.usage;
      if (u && (u.completion_tokens != null || u.output_tokens != null)) {
        const t = Number(u.completion_tokens ?? u.output_tokens ?? 0) || 0;
        this.session.outputTokens = t;
        this.session.completionTokens = t;
        this.session.exactTokens = true;
        const prompt = Number(u.prompt_tokens ?? u.input_tokens ?? 0) || 0;
        const cached = Number(u.prompt_tokens_details?.cached_tokens ?? u.cached_tokens ?? u.tokens_cached_read ?? 0) || 0;
        this.session.promptTokens = prompt;
        this.session.cachedTokens = cached;
        updateModelRatio(this.session.model, this.session.chars, t);
      }
      return;
    }
    if (this.session.exactTokens) return;
    // Count content + reasoning chars by scanning the JSON string values
    // directly (no full-object JSON.parse). ~2x cheaper for content deltas.
    const chars = this._scanStringLen(data, '"content":"', true)
                + this._scanStringLen(data, '"reasoning_content":"', false);
    if (chars > 0) {
      if (!this.session.firstTokenAt) this.session.firstTokenAt = Date.now();
      this.session.chars += chars;
      this.session.outputTokens = estimateTokensFromChars(this.session.model, this.session.chars);
    }
  }

  // Count decoded UTF-16 units of the JSON string value following `key` in
  // `data`, scanning the raw JSON text (no JSON.parse of the whole object).
  // Safe because `data` is the raw JSON line: indexOf('"key":"') matches only
  // the real key — a value containing the same bytes is escaped as \" and
  // can't match. charCodeAt runs on the decoded JS string, so escapes (\n, \",
  // \\uXXXX incl. surrogate pairs) count exactly as JSON.parse would, matching
  // String.length. If pushRaw, the raw escaped slice is saved for decode at
  // _finish (content is needed for coalescing; reasoning is not).
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
        if (c === 34) break;               // closing "
        if (c === 92) {                     // escape: 1 decoded UTF-16 unit
          if (data.charCodeAt(j + 1) === 117) j += 6;  // \uXXXX
          else j += 2;
          total++;
        } else { total++; j++; }
      }
      if (pushRaw && j > start) this.contentParts.push(data.slice(start, j));
      i = data.indexOf(key, j + 1);
    }
    return total;
  }

  // ---- Anthropic streaming events (unchanged) ----
  _handleAnthropicEvent(chunk) {
    const type = chunk?.type;
    if (type === 'message_start') {
      const u = chunk?.message?.usage;
      if (u) {
        this.session.promptTokens = Number(u.input_tokens ?? 0) || 0;
        this.session.cachedTokens = Number(u.cache_read_input_tokens ?? 0) || 0;
      }
      return;
    }
    if (type === 'message_delta') {
      const u = chunk?.usage;
      if (u && u.output_tokens != null) {
        const t = Number(u.output_tokens) || 0;
        this.session.outputTokens = t;
        this.session.completionTokens = t;
        this.session.exactTokens = true;
        if (u.input_tokens != null) {
          this.session.promptTokens = Number(u.input_tokens) || 0;
          this.session.cachedTokens = Number(u.cache_read_input_tokens ?? 0) || 0;
        }
        updateModelRatio(this.session.model, this.session.chars, t);
      }
      return;
    }
    if (this.session.exactTokens) return;
    if (type === 'content_block_delta') {
      const delta = chunk?.delta || {};
      let chars = 0;
      if (delta.type === 'text_delta' && typeof delta.text === 'string') {
        chars += delta.text.length;
        this.session.responseContent += delta.text;
      } else if (delta.type === 'thinking_delta' && typeof delta.thinking === 'string') {
        chars += delta.thinking.length;
      }
      if (chars > 0) {
        if (!this.session.firstTokenAt) this.session.firstTokenAt = Date.now();
        this.session.chars += chars;
        this.session.outputTokens = estimateTokensFromChars(this.session.model, this.session.chars);
      }
    }
  }

  onEnd() {
    if (!this.session) return;
    this.ended = true;
    // Drain any queued chunks synchronously so the session's token counts are
    // current when the caller (proxyRequest finally) runs finalizeSession.
    // The pipe path has fewer await gaps than a manual read loop, so the
    // deferred setImmediate drain may still be pending here — force it now.
    // A pending setImmediate then re-drains an empty queue (no-op) and calls
    // _finish, which is guarded against a second run.
    if (this.queue.length) this._drain();
    else if (!this.draining) this._finish();
  }

  _finish() {
    if (!this.session) return;
    // Streaming: flush any straggler SSE line.
    if (this.stream && this.lineBuf) {
      const line = this.lineBuf.trim();
      this.lineBuf = '';
      if (line) this._handleSseLine(line);
    }
    // OpenAI streaming: decode the raw content slices accumulated during the
    // scan into responseContent. This is off the hot path (runs once at end);
    // responseContent is needed for coalescing, not for the live tok/s estimate.
    if (this.stream && this.shape !== 'anthropic' && this.contentParts.length) {
      let out = '';
      for (let k = 0; k < this.contentParts.length; k++) {
        try { out += JSON.parse('"' + this.contentParts[k] + '"'); } catch {}
      }
      this.session.responseContent = out;
      this.contentParts.length = 0;
    }
    // Non-streaming: flush the JSON parser to emit any pending values,
    // then estimate output tokens from chars if no usage block was present.
    if (!this.stream && this.jsonParser) {
      this.jsonParser.flush();
      this._finalizeEstimate();
    }
    if (this.onFinalize) { try { this.onFinalize(this.session); } catch {} }
    this.session = null;
  }

  // ---- Non-streaming incremental JSON extraction ----
  // Called by JsonStream for each completed value. Extracts only the fields
  // the tap needs, then signals early termination.
  _onJsonValue(path, value) {
    const p = path.join('.');

    if (this.shape === 'openai') {
      // Usage fields (appear at the end of the response in most cases).
      if (p === 'usage.completion_tokens' || p === 'usage.output_tokens') {
        const exact = Number(value) || 0;
        this.session.outputTokens = exact;
        this.session.completionTokens = exact;
        this.session.exactTokens = true;
        updateModelRatio(this.session.model, this.session.chars, exact);
        this._got.usage = true;
        this._checkDone();
        return;
      }
      if (p === 'usage.prompt_tokens' || p === 'usage.input_tokens') {
        this.session.promptTokens = Number(value) || 0;
        return;
      }
      if (p === 'usage.prompt_tokens_details.cached_tokens' || p === 'usage.cached_tokens' || p === 'usage.tokens_cached_read') {
        this.session.cachedTokens = Number(value) || 0;
        return;
      }
      // Content from choices[N].message.content
      if (p.endsWith('.message.content') && typeof value === 'string') {
        this.session.responseContent += value;
        this.session.chars += value.length;
        if (!this.session.firstTokenAt) this.session.firstTokenAt = Date.now();
        if (!this.session.exactTokens) {
          this.session.outputTokens = estimateTokensFromChars(this.session.model, this.session.chars);
        }
        this._got.content = true;
        this._checkDone();
        return;
      }
      if (p.endsWith('.message.reasoning_content') && typeof value === 'string') {
        this.session.chars += value.length;
        if (!this.session.exactTokens) {
          this.session.outputTokens = estimateTokensFromChars(this.session.model, this.session.chars);
        }
        return;
      }
    } else {
      // ---- Anthropic shape ----
      if (p === 'usage.output_tokens') {
        const exact = Number(value) || 0;
        this.session.outputTokens = exact;
        this.session.completionTokens = exact;
        this.session.exactTokens = true;
        updateModelRatio(this.session.model, this.session.chars, exact);
        this._got.usage = true;
        this._checkDone();
        return;
      }
      if (p === 'usage.input_tokens') {
        this.session.promptTokens = Number(value) || 0;
        return;
      }
      if (p === 'usage.cache_read_input_tokens') {
        this.session.cachedTokens = Number(value) || 0;
        return;
      }
      // Content blocks: type appears before text/thinking in each block.
      // We accumulate text blocks for coalescing; thinking adds to char count.
      if (p.endsWith('.text') && typeof value === 'string' && this._currentBlockType === 'text') {
        this.session.responseContent += value;
        this.session.chars += value.length;
        if (!this.session.firstTokenAt) this.session.firstTokenAt = Date.now();
        if (!this.session.exactTokens) {
          this.session.outputTokens = estimateTokensFromChars(this.session.model, this.session.chars);
        }
        this._got.content = true;
        this._checkDone();
        return;
      }
      if (p.endsWith('.thinking') && typeof value === 'string' && this._currentBlockType === 'thinking') {
        this.session.chars += value.length;
        if (!this.session.exactTokens) {
          this.session.outputTokens = estimateTokensFromChars(this.session.model, this.session.chars);
        }
        return;
      }
      // Track content block type so we know whether to accumulate text/thinking.
      if (p.endsWith('.type') && (value === 'text' || value === 'thinking')) {
        this._currentBlockType = value;
        return;
      }
    }
  }

  _checkDone() {
    // Once we have both usage and content, stop parsing — the rest of the
    // JSON document is irrelevant.
    if (this._got.usage && this._got.content) {
      this.jsonParser.done = true;
    }
  }

  // If the document ends without a usage block, estimate output tokens from
  // chars so finalizeSession can record a finalTps. (No-op if exact usage
  // was already set via the usage path.)
  _finalizeEstimate() {
    if (this.session.exactTokens || this.session.chars === 0) return;
    this.session.outputTokens = estimateTokensFromChars(this.session.model, this.session.chars);
  }
}

// Node Transform wrapping a ChatTap for the streaming pipe path. Each chunk
// is forwarded unmodified; tap.onChunk does O(1) byte counting + enqueues the
// deferred parse, so telemetry never delays bytes reaching the client.
function createTapStream(tap) {
  return new Transform({
    transform(chunk, encoding, callback) {
      try { tap.onChunk(chunk); } catch {}
      callback(null, chunk);
    },
  });
}

module.exports = { ChatTap, createTapStream };
