'use strict';
require('./fetch-guard');

// Proves the hermetic guard is active: a fetch to any non-loopback host is
// rejected with TEST_FETCH_BLOCKED before any network attempt. Uses a .invalid
// host so that, even if the guard were somehow absent, no real provider could
// be reached (the TLD never resolves).

const { test } = require('node:test');
const assert = require('node:assert/strict');

test('fetch guard blocks non-loopback hosts so tests never reach the real upstream', async () => {
  await assert.rejects(
    () => fetch('https://umans-blocked.invalid/v1/usage'),
    (err) => err && err.code === 'TEST_FETCH_BLOCKED',
  );
});

test('fetch guard still allows loopback (mock upstreams)', async () => {
  // A loopback fetch to a closed port rejects with a connection error — NOT the
  // guard. Asserting the error is *not* TEST_FETCH_BLOCKED proves the guard let
  // the call through to the (failing) socket layer rather than blocking it.
  await assert.rejects(
    () => fetch('http://127.0.0.1:1/'),
    (err) => !err || err.code !== 'TEST_FETCH_BLOCKED',
  );
});
