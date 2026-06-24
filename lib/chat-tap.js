'use strict';

const state = require('./state');
const { estimateTokensFromChars, updateModelRatio, addSessionTokens } = require('./sessions');
const { JsonStream } = require('./json-stream');

// Per-request stream/body tap. Parses SSE chunks (streaming) or a single
// JSON body (non-streaming) to count output tokens WITHOUT mutating bytes.
// ASYNC: onChunk is O(1) — it pushes the raw buffer onto a queue and
// schedules a drain via setImmediate. All parsing happens in _drain(),
// which runs between I/O ticks (during the next reader.read() await),
// so telemetry never delays bytes reaching the client.
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
      if (this.stream) {
        // reader.read() returns Uint8Array, not Buffer. Uint8Array.toString('utf8')
        // returns comma-separated byte values, so wrap in Buffer.from first.
        this.lineBuf += Buffer.from(buf).toString('utf8');
        let idx;
        while ((idx = this.lineBuf.indexOf('\n')) >= 0) {
          const line = this.lineBuf.slice(0, idx).replace(/\r$/, '');
          this.lineBuf = this.lineBuf.slice(idx + 1);
          this._handleSseLine(line);
        }
      } else if (this.jsonParser) {
        // Incremental parse — stops early once we have all fields.
        this.jsonParser.push(Buffer.from(buf).toString('utf8'));
      }
    }
    this.draining = false;
    // If onEnd arrived while draining, finalize now that we're caught up.
    if (this.ended) this._finish();
  }

  // ---- Streaming SSE line handler (unchanged) ----
  _handleSseLine(line) {
    if (!line.startsWith('data:')) return;
    const data = line.slice(5).trim();
    if (!data || data === '[DONE]') return;
    let chunk;
    try { chunk = JSON.parse(data); } catch { return; }
    if (this.shape === 'anthropic') return this._handleAnthropicEvent(chunk);
    // ---- OpenAI shape ----
    const u = chunk?.usage;
    if (u && (u.completion_tokens != null || u.output_tokens != null)) {
      const t = Number(u.completion_tokens ?? u.output_tokens ?? 0) || 0;
      const prev = this.session.outputTokens;
      this.session.outputTokens = t;
      this.session.completionTokens = t;
      this.session.exactTokens = true;
      const prompt = Number(u.prompt_tokens ?? u.input_tokens ?? 0) || 0;
      const cached = Number(u.prompt_tokens_details?.cached_tokens ?? u.cached_tokens ?? u.tokens_cached_read ?? 0) || 0;
      this.session.promptTokens = prompt;
      this.session.cachedTokens = cached;
      updateModelRatio(this.session.model, this.session.chars, t);
      addSessionTokens(this.session, Math.max(0, t - prev), { exact: true });
      return;
    }
    if (this.session.exactTokens) return;
    let chars = 0;
    const choices = chunk?.choices;
    if (Array.isArray(choices)) {
      for (const c of choices) {
        const delta = c?.delta || {};
        if (typeof delta.content === 'string') {
          chars += delta.content.length;
          this.session.responseContent += delta.content;
        }
        if (typeof delta.reasoning_content === 'string') chars += delta.reasoning_content.length;
      }
    }
    if (chars > 0) {
      if (!this.session.firstTokenAt) this.session.firstTokenAt = Date.now();
      this.session.chars += chars;
      this.session.outputTokens = estimateTokensFromChars(this.session.model, this.session.chars);
    }
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
        const prev = this.session.outputTokens;
        this.session.outputTokens = t;
        this.session.completionTokens = t;
        this.session.exactTokens = true;
        if (u.input_tokens != null) {
          this.session.promptTokens = Number(u.input_tokens) || 0;
          this.session.cachedTokens = Number(u.cache_read_input_tokens ?? 0) || 0;
        }
        updateModelRatio(this.session.model, this.session.chars, t);
        addSessionTokens(this.session, Math.max(0, t - prev), { exact: true });
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
    if (!this.draining && this.queue.length) this._drain();
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
    // Non-streaming: flush the JSON parser to emit any pending values,
    // then feed the TPS bucket from the char estimate if no usage block
    // was present (exact tokens already handled via _onJsonValue).
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
        const prev = this.session.outputTokens;
        this.session.outputTokens = exact;
        this.session.completionTokens = exact;
        this.session.exactTokens = true;
        updateModelRatio(this.session.model, this.session.chars, exact);
        addSessionTokens(this.session, Math.max(0, exact - prev), { exact: true });
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
        // Estimate until exact usage arrives (if ever).
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
      // No usage block at all — estimate from content chars at the end.
      // If we see the closing } without usage, the _finish path handles it.
    } else {
      // ---- Anthropic shape ----
      if (p === 'usage.output_tokens') {
        const exact = Number(value) || 0;
        const prev = this.session.outputTokens;
        this.session.outputTokens = exact;
        this.session.completionTokens = exact;
        this.session.exactTokens = true;
        updateModelRatio(this.session.model, this.session.chars, exact);
        addSessionTokens(this.session, Math.max(0, exact - prev), { exact: true });
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

    // If no usage block was present, estimate from chars at finish time.
    // _finish calls jsonParser.flush(), but if usage never arrived, we need
    // to feed the TPS bucket from the estimate. This happens in _checkDone
    // when the document ends without usage.
  }

  _checkDone() {
    // Once we have both usage and content, stop parsing — the rest of the
    // JSON document is irrelevant. For responses without usage, we parse
    // to the end (flush) and handle the estimate in _finish.
    if (this._got.usage && this._got.content) {
      this.jsonParser.done = true;
    }
  }

  // If the document ends without a usage block, feed the TPS bucket from
  // the char estimate. Called from _finish after jsonParser.flush().
  // (No-op if exact tokens were already set via the usage path.)
  _finalizeEstimate() {
    if (this.session.exactTokens || this.session.chars === 0) return;
    const est = estimateTokensFromChars(this.session.model, this.session.chars);
    const prev = this.session.outputTokens;
    this.session.outputTokens = est;
    addSessionTokens(this.session, Math.max(0, est - prev));
  }
}

module.exports = { ChatTap };
