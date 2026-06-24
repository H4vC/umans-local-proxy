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
  return n * scale;
}

function loadConfig() {
  const raw = readJSON(CONFIG_FILE);
  const envApiKey = process.env.UMANS_API_KEY || '';
  const fileApiKey = raw.API_KEY || '';
  const fileKeys = fileProxyApiKeys(raw);
  const requestTimeoutRaw = process.env.REQUEST_TIMEOUT || raw.REQUEST_TIMEOUT || '15m';
  return {
    listenAddr: process.env.LISTEN_ADDR || raw.LISTEN_ADDR || '127.0.0.1:8084',
    upstreamBaseURL: DEFAULT_UPSTREAM,
    apiKey: envApiKey || fileApiKey,
    fileApiKey,
    enabledModels: Array.isArray(raw.ENABLED_MODELS) ? raw.ENABLED_MODELS.map(String).map((x) => x.trim()).filter(Boolean) : [],
    requestTimeout: parseDuration(requestTimeoutRaw),
    requestTimeoutRaw,
    overrideConcurrency: Math.max(0, Number(process.env.OVERRIDE_CONCURRENCY || raw.OVERRIDE_CONCURRENCY || 0) || 0),
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
    ENABLED_MODELS: next.enabledModels,
    API_KEYS: next.fileProxyApiKeys,
    REQUEST_TIMEOUT: next.requestTimeoutRaw,
    OVERRIDE_CONCURRENCY: next.overrideConcurrency,
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
  return { host: match ? match[1] : '127.0.0.1', port };
}

module.exports = {
  CONFIG_DIR,
  CONFIG_FILE,
  DEFAULT_UPSTREAM,
  readJSON,
  cleanKeys,
  fileProxyApiKeys,
  envProxyApiKeys,
  parseDuration,
  loadConfig,
  saveConfig,
  parseListenAddr,
};
