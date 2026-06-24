'use strict';

// FNV-1a 32-bit. ~1ns/byte, no allocation beyond output.

// Returns raw uint32 — used internally for numeric chain hashing so each
// mix step has full 8-bit-per-byte entropy instead of the 16 values of hex.
function fnv1a32(str) {
  let h = 0x811c9dc5;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

// Returns hex string — used for display and non-chain keying.
function fnv1a(str) {
  return fnv1a32(str).toString(16).padStart(8, '0');
}

// Mix a 32-bit hash value into a running chain hash. Uses the FNV-1a
// continuation pattern but processes the 4 bytes of the value, giving
// full 256-value per-byte entropy rather than the 16 values of hex-string
// concatenation.
function fnv1aMixNum(seed, hash) {
  let h = seed >>> 0;
  h ^= hash & 0xff;
  h = Math.imul(h, 0x01000193);
  h ^= (hash >>> 8) & 0xff;
  h = Math.imul(h, 0x01000193);
  h ^= (hash >>> 16) & 0xff;
  h = Math.imul(h, 0x01000193);
  h ^= (hash >>> 24) & 0xff;
  h = Math.imul(h, 0x01000193);
  return h >>> 0;
}

module.exports = { fnv1a, fnv1a32, fnv1aMixNum };
