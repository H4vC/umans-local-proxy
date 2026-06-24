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

class JsonStream {
  constructor({ onValue } = {}) {
    this.onValue = onValue || (() => {});
    this.done = false;
    this.buf = '';
    this.i = 0;
    this.stack = []; // [{ type: 'object'|'array', key: string|number|null }]
    this.state = 'VALUE'; // VALUE | KEY_OR_END | COLON | AFTER_VALUE
    // String parsing state
    this.inString = false;
    this.isKey = false;
    this.str = '';
    this.esc = false;
    this.inUni = false;
    this.uni = '';
    // Number / literal accumulation
    this.num = '';
    this.lit = '';
  }

  push(chunk) {
    if (this.done) return;
    this.buf = this.buf.slice(this.i) + chunk;
    this.i = 0;
    this._run();
  }

  flush() {
    if (this.done) return;
    this.buf = this.buf.slice(this.i);
    this.i = 0;
    this._run();
  }

  _path() {
    return this.stack.map((c) => c.key);
  }

  _emit(value) {
    this.onValue(this._path(), value);
  }

  _run() {
    const s = this.buf;
    const n = s.length;

    while (this.i < n) {
      const c = s[this.i];

      // ---- Inside a string (key or value) ----
      if (this.inString) {
        if (this.esc) {
          if (c === 'u') { this.esc = false; this.inUni = true; this.uni = ''; this.i++; continue; }
          this.str += c === 'n' ? '\n' : c === 'r' ? '\r' : c === 't' ? '\t'
            : c === 'b' ? '\b' : c === 'f' ? '\f' : c === '/' ? '/' : c;
          this.esc = false; this.i++; continue;
        }
        if (this.inUni) {
          this.uni += c; this.i++;
          if (this.uni.length === 4) {
            this.str += String.fromCharCode(parseInt(this.uni, 16));
            this.inUni = false;
            this.uni = '';
          }
          continue;
        }
        if (c === '\\') { this.esc = true; this.i++; continue; }
        if (c === '"') {
          this.inString = false; this.i++;
          if (this.isKey) {
            const top = this.stack[this.stack.length - 1];
            if (top) top.key = this.str;
            this.state = 'COLON';
          } else {
            this._emit(this.str);
            if (this.done) return;
            this.state = 'AFTER_VALUE';
          }
          this.str = '';
          continue;
        }
        this.str += c; this.i++;
        continue;
      }

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
          if (c === '{') { this.stack.push({ type: 'object', key: null }); this.state = 'KEY_OR_END'; this.i++; break; }
          if (c === '[') { this.stack.push({ type: 'array', key: 0 }); this.state = 'VALUE'; this.i++; break; }
          if (c === '"') { this.inString = true; this.isKey = false; this.str = ''; this.i++; break; }
          if (c === '-' || (c >= '0' && c <= '9')) { this.num = c; this.i++; break; }
          if (c === 't' || c === 'f' || c === 'n') { this.lit = c; this.i++; break; }
          this.i++; // Unknown — skip
          break;

        case 'KEY_OR_END':
          if (c === '}') { this.stack.pop(); this.i++; this.state = 'AFTER_VALUE'; break; }
          if (c === '"') { this.inString = true; this.isKey = true; this.str = ''; this.i++; break; }
          this.i++;
          break;

        case 'COLON':
          if (c === ':') { this.state = 'VALUE'; this.i++; break; }
          this.i++;
          break;

        case 'AFTER_VALUE':
          if (c === ',') {
            const top = this.stack[this.stack.length - 1];
            if (top && top.type === 'object') { top.key = null; this.state = 'KEY_OR_END'; }
            else if (top && top.type === 'array') { top.key = (top.key || 0) + 1; this.state = 'VALUE'; }
            this.i++;
            break;
          }
          if (c === '}' || c === ']') { this.stack.pop(); this.i++; this.state = 'AFTER_VALUE'; break; }
          this.i++;
          break;
      }
    }

    // Keep unparsed remainder for the next push()
    this.buf = this.buf.slice(this.i);
    this.i = 0;
  }
}

module.exports = { JsonStream };
