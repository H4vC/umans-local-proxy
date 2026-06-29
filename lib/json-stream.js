'use strict';

// Minimal streaming JSON parser. Extracts values incrementally from a byte
// stream without buffering the full document. Calls onValue(path, value)
// for each completed value (strings, numbers, booleans, null).
//
// path is an array of keys (strings, for object fields) and indices
// (numbers, for array elements). e.g. ['choices', 0, 'message', 'content']
// or ['usage', 'completion_tokens'].
//
// Early termination: if onValue sets this.done = true, parsing stops
// immediately and push() becomes a no-op. This lets the tap stop parsing
// once it has all the fields it cares about and just forward raw bytes.
//
// Performance: the hot path (scanning a plain string body) uses indexOf to
// find the next '"' or '\\' in a single C++ call, then slices the whole
// run — avoiding the per-char this.str += c that creates a ConsString node
// every iteration. Escapes and unicode sequences fall back to per-char
// handling only for the escape characters themselves, then resume the
// fast path.

class JsonStream {
  constructor({ onValue } = {}) {
    this.onValue = onValue || (() => {});
    this.done = false;
    this.buf = '';
    this.i = 0;
    this.stack = []; // [{ type: 'object'|'array', key: string|number|null }]
    this.pathArr = []; // mirror of stack keys for O(1) path construction
    this.state = 'VALUE'; // VALUE | KEY_OR_END | COLON | AFTER_VALUE
    // String parsing state
    this.inString = false;
    this.isKey = false;
    this.str = '';
    this.parts = null; // array of slice strings when fast-pathing
    this.esc = false;
    this.inUni = false;
    this.uni = '';
    // Number / literal accumulation
    this.num = '';
    this.lit = '';
  }

  push(chunk) {
    if (this.done) return;
    this.buf = this.i > 0 ? this.buf.slice(this.i) + chunk : this.buf + chunk;
    this.i = 0;
    this._run();
  }

  flush() {
    if (this.done) return;
    if (this.i > 0) { this.buf = this.buf.slice(this.i); this.i = 0; }
    this._run();
    // Emit a trailing top-level number that had no terminator (e.g. "123" at EOF).
    if (!this.done && this.num) {
      this._emit(Number(this.num));
      this.num = '';
    }
  }

  _emit(value) {
    this.onValue(this.pathArr.slice(), value);
  }

  _run() {
    const s = this.buf;
    const n = s.length;

    while (this.i < n && !this.done) {
      // ---- Inside a string (key or value) ----
      if (this.inString) {
        // Escape continuation: handle exactly one char, then resume fast path.
        if (this.esc) {
          const c = s[this.i];
          if (c === 'u') {
            this.esc = false;
            this.inUni = true;
            this.uni = '';
          } else {
            this.str += c === 'n' ? '\n' : c === 'r' ? '\r' : c === 't' ? '\t'
              : c === 'b' ? '\b' : c === 'f' ? '\f' : c === '/' ? '/' : c;
            this.esc = false;
          }
          this.i++;
          continue;
        }
        if (this.inUni) {
          this.uni += s[this.i];
          this.i++;
          if (this.uni.length === 4) {
            this.str += String.fromCharCode(parseInt(this.uni, 16));
            this.inUni = false;
            this.uni = '';
          }
          continue;
        }

        // Fast path: find the next '"' or '\\' in one C++ call.
        const nextQuote = s.indexOf('"', this.i);
        const nextSlash = s.indexOf('\\', this.i);

        if (nextQuote < 0 && nextSlash < 0) {
          // Neither found — rest of buffer is plain string content.
          this._appendStr(s.slice(this.i));
          this.i = n;
          break;
        }

        if (nextQuote >= 0 && (nextSlash < 0 || nextQuote < nextSlash)) {
          // Closing quote found — accumulate the run and finish string.
          if (nextQuote > this.i) this._appendStr(s.slice(this.i, nextQuote));
          this.i = nextQuote + 1;
          this._finishString();
          continue;
        }

        // Escape found — accumulate the run up to the backslash, then
        // set esc mode; the next loop iteration handles the escaped char.
        if (nextSlash > this.i) this._appendStr(s.slice(this.i, nextSlash));
        this.i = nextSlash + 1;
        this.esc = true;
        if (!this.parts) this.parts = [];
        continue;
      }

      const c = s[this.i];

      // ---- Inside a number ----
      if (this.num) {
        if (c === '-' || c === '+' || c === '.' || c === 'e' || c === 'E' || (c >= '0' && c <= '9')) {
          this.num += c; this.i++; continue;
        }
        this._emit(Number(this.num));
        if (this.done) return;
        this.num = '';
        this.state = 'AFTER_VALUE';
        continue; // Reprocess c in AFTER_VALUE
      }

      // ---- Inside a literal (true/false/null) ----
      if (this.lit) {
        this.lit += c; this.i++;
        if (this.lit === 'true') { this._emit(true); if (this.done) return; this.lit = ''; this.state = 'AFTER_VALUE'; }
        else if (this.lit === 'false') { this._emit(false); if (this.done) return; this.lit = ''; this.state = 'AFTER_VALUE'; }
        else if (this.lit === 'null') { this._emit(null); if (this.done) return; this.lit = ''; this.state = 'AFTER_VALUE'; }
        else if (!'true'.startsWith(this.lit) && !'false'.startsWith(this.lit) && !'null'.startsWith(this.lit)) {
          this.lit = ''; // Invalid — abort
        }
        continue;
      }

      // ---- Skip whitespace ----
      if (c === ' ' || c === '\t' || c === '\n' || c === '\r') { this.i++; continue; }

      // ---- State machine ----
      switch (this.state) {
        case 'VALUE':
          if (c === '{') { this.stack.push({ type: 'object', key: null }); this.pathArr.push(null); this.state = 'KEY_OR_END'; this.i++; break; }
          if (c === '[') { this.stack.push({ type: 'array', key: 0 }); this.pathArr.push(0); this.state = 'VALUE'; this.i++; break; }
          if (c === '"') { this.inString = true; this.isKey = false; this.str = ''; this.parts = null; this.i++; break; }
          if (c === '-' || (c >= '0' && c <= '9')) { this.num = c; this.i++; break; }
          if (c === 't' || c === 'f' || c === 'n') { this.lit = c; this.i++; break; }
          this.i++; // Unknown — skip
          break;

        case 'KEY_OR_END':
          if (c === '}') { this._popStack(); this.i++; this.state = 'AFTER_VALUE'; break; }
          if (c === '"') { this.inString = true; this.isKey = true; this.str = ''; this.parts = null; this.i++; break; }
          this.i++;
          break;

        case 'COLON':
          if (c === ':') { this.state = 'VALUE'; this.i++; break; }
          this.i++;
          break;

        case 'AFTER_VALUE':
          if (c === ',') {
            const top = this.stack[this.stack.length - 1];
            if (top && top.type === 'object') { top.key = null; this.pathArr[this.pathArr.length - 1] = null; this.state = 'KEY_OR_END'; }
            else if (top && top.type === 'array') { top.key = (top.key || 0) + 1; this.pathArr[this.pathArr.length - 1] = top.key; this.state = 'VALUE'; }
            this.i++;
            break;
          }
          if (c === '}' || c === ']') { this._popStack(); this.i++; this.state = 'AFTER_VALUE'; break; }
          this.i++;
          break;
      }
    }

    // Keep unparsed remainder for the next push()
    if (this.i > 0) { this.buf = this.buf.slice(this.i); this.i = 0; }
  }

  // Append a slice to the current string. Uses the parts array when active
  // (escape encountered), otherwise appends directly to str.
  _appendStr(slice) {
    if (this.parts) {
      if (this.str) { this.parts.push(this.str); this.str = ''; }
      this.parts.push(slice);
    } else {
      this.str += slice;
    }
  }

  // Finish a string value: emit it (or store as key), reset string state.
  _finishString() {
    const value = this._strValue();
    this.inString = false;
    this.parts = null;
    this.str = '';
    if (this.isKey) {
      const top = this.stack[this.stack.length - 1];
      if (top) top.key = value;
      this.pathArr[this.pathArr.length - 1] = value;
      this.state = 'COLON';
    } else {
      this._emit(value);
      if (this.done) return;
      this.state = 'AFTER_VALUE';
    }
  }

  _strValue() {
    if (this.parts) {
      if (this.str) this.parts.push(this.str);
      return this.parts.length > 0 ? this.parts.join('') : '';
    }
    return this.str;
  }

  _popStack() {
    this.stack.pop();
    this.pathArr.pop();
  }
}

module.exports = { JsonStream };
