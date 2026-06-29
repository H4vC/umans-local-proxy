'use strict';

// Hermetic test guard: the test suite must NEVER reach the real UMANS upstream
// (api.code.umans.ai) or web app (app.umans.ai). This wraps global.fetch to
// reject any call whose host is not loopback. Loopback (127.0.0.1, ::1,
// localhost) is allowed so mock upstreams — e.g. the real http.Server the
// chat-stream suite stands up on 127.0.0.1 — keep working.
//
// Tests that need a scripted response replace global.fetch wholesale; on
// restore they reinstate this guarded fetch (they capture it AFTER this module
// runs), so the guard is always in effect between mocks.
//
// Loaded two ways for defense in depth:
//   - `node --require ./test/fetch-guard.js` (the package.json test script)
//   - `require('./fetch-guard')` at the top of every test file (bare `node --test`)
// require() caches the module, so the install runs exactly once per process.

if (!global.fetch || !global.fetch.__testFetchGuard) {
  const realFetch = global.fetch;
  function guardedFetch(input, init) {
    let host = '';
    try {
      let u = null;
      if (typeof input === 'string') u = new URL(input);
      else if (input instanceof URL) u = input;
      else if (input && typeof input === 'object' && typeof input.url === 'string') u = new URL(input.url);
      host = u ? u.hostname.replace(/^\[|\]$/g, '') : '';
    } catch {
      host = '';
    }
    const loopback = host === '127.0.0.1' || host === '::1' || host === 'localhost';
    if (!loopback) {
      const err = new Error(
        `Blocked non-loopback fetch in tests (host: "${host || '?'}"). Tests must never reach the real UMANS upstream — mock global.fetch, or point state.config.upstreamBaseURL at a loopback mock upstream.`,
      );
      err.code = 'TEST_FETCH_BLOCKED';
      return Promise.reject(err);
    }
    return realFetch(input, init);
  }
  Object.defineProperty(guardedFetch, '__testFetchGuard', { value: true, enumerable: false });
  global.fetch = guardedFetch;
}

module.exports = {};
