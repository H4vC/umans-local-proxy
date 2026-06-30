'use strict';

const fs = require('fs');
const path = require('path');

const CONFIG_DIR = path.join(__dirname, '..', '.config');
const CONFIG_FILE = path.join(CONFIG_DIR, 'config.json');
const DEFAULT_UPSTREAM = 'https://api.code.umans.ai/v1';

function readJSON(file) {
  try { return JSON.parse(fs.readFileSync(file, 'utf8')); }
  catch (err) {
    if (err.code === 'ENOENT') return {};
    // Corrupted config (bad JSON): return empty rather than killing the
    // process. At boot this means defaults; mid-session the caller handles
    // the missing values gracefully.
    console.error(`Failed to load ${file}: ${err.message}`);
    return {};
  }
}

// Strict config read used at boot: a missing file is fine (defaults apply), but
// a corrupt (unparseable) existing file throws so auth is never silently
// disabled by falling back to empty defaults. Tolerant callers (handleConfig,
// the launcher) still use readJSON.
function readConfigFile(file) {
  let text;
  try { text = fs.readFileSync(file, 'utf8'); }
  catch (err) { if (err.code === 'ENOENT') return {}; throw err; }
  try { return JSON.parse(text); }
  catch (err) { throw new Error(`config file ${file} is corrupt: ${err.message}`); }
}

function cleanKeys(values) {
  return [...new Set(values.map((key) => String(key || '').trim()).filter(Boolean))];
}

function fileProxyApiKeys(raw) {
  return cleanKeys(Array.isArray(raw.API_KEYS) ? raw.API_KEYS.map((entry) => typeof entry === 'string' ? entry : entry?.key) : []);
}

function envProxyApiKeys() {
  return cleanKeys((process.env.API_KEYS || '').split(','));
}

function parseDuration(value) {
  const raw = String(value || '15m').trim().toLowerCase();
  const match = raw.match(/^(\d+)(ms|s|m|h)?$/);
  if (!match) throw new Error('REQUEST_TIMEOUT must be like 30000ms, 30s, 15m, or 1h');
  const n = Number(match[1]);
  const unit = match[2] || 'ms';
  const scale = unit === 'h' ? 3600000 : unit === 'm' ? 60000 : unit === 's' ? 1000 : 1;
  const ms = n * scale;
  // Clamp to a 1s floor: a unitless value (e.g. REQUEST_TIMEOUT=30) parses as
  // 30ms, which would abort every request instantly. Long timeouts are fine.
  return Math.max(ms, 1000);
}
const WEBSEARCH_PROVIDERS = new Set(['native', 'exa', 'none']);
// Selects the value sent as `X-Umans-Websearch-Provider` on chat upstream
// requests. Empty/missing → 'none' (no web search). Throws on an unknown
// value so a typo can't silently send garbage to the upstream.
function parseWebsearchProvider(value) {
  const v = String(value || 'none').trim().toLowerCase();
  if (!WEBSEARCH_PROVIDERS.has(v)) throw new Error(`WEBSEARCH_PROVIDER must be one of native, exa, none (got ${v ? JSON.stringify(v) : '""'})`);
  return v;
}


const REQUEST_LOGGING_LEVELS = new Set(['off', 'basic', 'verbose']);
// Selects how much lifecycle logging the proxy emits for chat requests.
// Empty/missing → 'off'. Throws on an unknown value so a typo can't silently
// change log volume.
function parseRequestLogging(value) {
  const v = String(value || 'off').trim().toLowerCase();
  if (!REQUEST_LOGGING_LEVELS.has(v)) throw new Error(`REQUEST_LOGGING must be one of off, basic, verbose (got ${v ? JSON.stringify(v) : '""'})`);
  return v;
}
// Accept either the bare session token (the __Secure-authjs.session-token JWT
// value) or a full `name=value` Cookie header, and return a ready-to-send
// `Cookie:` value — so users can paste just the token from DevTools instead of
// the whole header. A bare JWT has no '=' (base64url has no padding), so a
// leading `name=` reliably distinguishes a header from a bare token.
const SESSION_COOKIE_NAME = '__Secure-authjs.session-token';
function normalizeSessionCookie(raw) {
  let v = (raw == null ? '' : String(raw)).trim();
  if (!v) return '';
  // Strip a leading "Cookie:" if someone copied the whole request header line.
  v = v.replace(/^cookie\s*:\s*/i, '').trim();
  // Already "name=value" (or a full multi-cookie header)? Keep as-is.
  if (/^[^\s=;]+=/.test(v)) return v;
  return `${SESSION_COOKIE_NAME}=${v}`;
}


function loadConfig() {
  const raw = readConfigFile(CONFIG_FILE);
  const envApiKey = process.env.UMANS_API_KEY || '';
  const fileApiKey = raw.API_KEY || '';
  const envSessionCookie = process.env.UMANS_SESSION_COOKIE || '';
  const fileSessionCookie = raw.SESSION_COOKIE || '';
  const fileKeys = fileProxyApiKeys(raw);
  const requestTimeoutRaw = process.env.REQUEST_TIMEOUT || raw.REQUEST_TIMEOUT || '15m';
  const websearchProvider = parseWebsearchProvider(process.env.WEBSEARCH_PROVIDER || raw.WEBSEARCH_PROVIDER || 'none');
  const requestLogging = parseRequestLogging(process.env.REQUEST_LOGGING || raw.REQUEST_LOGGING || 'off');
  return {
    listenAddr: process.env.LISTEN_ADDR || raw.LISTEN_ADDR || '127.0.0.1:8084',
    upstreamBaseURL: DEFAULT_UPSTREAM,
    apiKey: envApiKey || fileApiKey,
    fileApiKey,
    sessionCookie: normalizeSessionCookie(envSessionCookie || fileSessionCookie),
    fileSessionCookie,
    enabledModels: Array.isArray(raw.ENABLED_MODELS) ? raw.ENABLED_MODELS.map(String).map((x) => x.trim()).filter(Boolean) : [],
    requestTimeout: parseDuration(requestTimeoutRaw),
    requestTimeoutRaw,
    requestLogging,
    requestLoggingRaw: requestLogging,
    overrideConcurrency: Math.max(0, Number(process.env.OVERRIDE_CONCURRENCY || raw.OVERRIDE_CONCURRENCY || 0) || 0),
    websearchProvider,
    proxyApiKeys: cleanKeys([...envProxyApiKeys(), ...fileKeys]),
    fileProxyApiKeys: fileKeys,
  };
}

function saveConfig(next) {
  if (!fs.existsSync(CONFIG_DIR)) fs.mkdirSync(CONFIG_DIR, { recursive: true, mode: 0o700 });
  fs.chmodSync(CONFIG_DIR, 0o700);
  fs.writeFileSync(CONFIG_FILE, JSON.stringify({
    LISTEN_ADDR: next.listenAddr,
    API_KEY: next.fileApiKey,
    SESSION_COOKIE: next.fileSessionCookie,
    ENABLED_MODELS: next.enabledModels,
    API_KEYS: next.fileProxyApiKeys,
    REQUEST_TIMEOUT: next.requestTimeoutRaw,
    REQUEST_LOGGING: next.requestLoggingRaw || next.requestLogging || 'off',
    OVERRIDE_CONCURRENCY: next.overrideConcurrency,
    WEBSEARCH_PROVIDER: next.websearchProvider,
  }, null, 2) + '\n', { mode: 0o600 });
  fs.chmodSync(CONFIG_FILE, 0o600);
}

function parseListenAddr(value) {
  const raw = String(value || '').trim();
  if (!raw) return { host: '127.0.0.1', port: 8084 };

  const match = raw.match(/^(.+):(\d+)$/);
  const portText = match ? match[2] : raw;
  if (!match && raw.includes(':')) throw new Error('LISTEN_ADDR must be a port or host:port');

  const port = Number(portText);
  if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error('LISTEN_ADDR port must be an integer from 1 to 65535');
  let host = match ? match[1] : '127.0.0.1';
  if (host.startsWith('[') && host.endsWith(']')) host = host.slice(1, -1); // strip [::1] brackets for IPv6
  return { host, port };
}

// True for loopback listen hosts. Used by the boot guard (B3) to refuse a
// non-loopback bind without proxy auth. Includes the IPv4-mapped form.
function isLoopbackHost(host) {
  return host === '127.0.0.1' || host === '::1' || host === 'localhost' || host === '::ffff:127.0.0.1';
}

module.exports = {
  CONFIG_DIR,
  CONFIG_FILE,
  DEFAULT_UPSTREAM,
  readJSON,
  readConfigFile,
  cleanKeys,
  fileProxyApiKeys,
  envProxyApiKeys,
  parseDuration,
  parseWebsearchProvider,
  parseRequestLogging,
  loadConfig,
  saveConfig,
  parseListenAddr,
  isLoopbackHost,
  normalizeSessionCookie,
};
