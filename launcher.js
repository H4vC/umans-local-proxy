const fs = require('fs');
const path = require('path');
const readline = require('readline');
const { spawn } = require('child_process');
const { SCALING_DEFAULTS, SCALING_FIELDS } = require('./lib/config');
const SCALING_DEFAULT_FIELDS = Object.fromEntries(Object.entries(SCALING_FIELDS).map(([key, field]) => [field, SCALING_DEFAULTS[key]]));

const CONFIG_DIR = path.join(__dirname, '.config');
const CONFIG_FILE = path.join(CONFIG_DIR, 'config.json');
const DEFAULTS = {
  LISTEN_ADDR: '127.0.0.1:8084',
  API_KEY: 'sk_dummy',
  SESSION_COOKIE: '',
  ENABLED_MODELS: [],
  API_KEYS: [],
  REQUEST_TIMEOUT: '15m',
  REQUEST_LOGGING: 'off',
  OVERRIDE_CONCURRENCY: 0,
  RELEASE_COOLDOWN_MS: '2s',
  WEBSEARCH_PROVIDER: 'none',
};

function loadScaling(raw) {
  const out = {};
  for (const [field, fallback] of Object.entries(SCALING_DEFAULT_FIELDS)) {
    const value = Number(process.env[field] !== undefined ? process.env[field] : raw[field]);
    out[field] = Number.isSafeInteger(value) && value >= 0 ? value : fallback;
  }
  return out;
}

function readJSON(file) {
  try { return JSON.parse(fs.readFileSync(file, 'utf8')); }
  catch { return {}; }
}

// Normalize a config list value. Accepts an array (of strings or {key} objects,
// matching the shape proxy.js fileProxyApiKeys reads) or a comma-separated
// string. Without the object handling, {key:'secret'} entries would stringify
// to "[object Object]" and silently destroy the saved proxy keys.
function cleanList(value) {
  if (Array.isArray(value)) return value.map((entry) => (typeof entry === 'string' ? entry : entry?.key)).map((x) => String(x || '').trim()).filter(Boolean);
  return String(value || '').split(',').map((x) => x.trim()).filter(Boolean);
}
function loadConfig() {
  const raw = readJSON(CONFIG_FILE);
  return {
    LISTEN_ADDR: raw.LISTEN_ADDR || DEFAULTS.LISTEN_ADDR,
    API_KEY: process.env.UMANS_API_KEY || raw.API_KEY || DEFAULTS.API_KEY,
    SESSION_COOKIE: process.env.UMANS_SESSION_COOKIE || raw.SESSION_COOKIE || DEFAULTS.SESSION_COOKIE,
    ENABLED_MODELS: cleanList(raw.ENABLED_MODELS),
    API_KEYS: cleanList(raw.API_KEYS),
    REQUEST_TIMEOUT: raw.REQUEST_TIMEOUT || DEFAULTS.REQUEST_TIMEOUT,
    REQUEST_LOGGING: raw.REQUEST_LOGGING || DEFAULTS.REQUEST_LOGGING,
    OVERRIDE_CONCURRENCY: Math.max(0, Number(raw.OVERRIDE_CONCURRENCY || DEFAULTS.OVERRIDE_CONCURRENCY) || 0),
    RELEASE_COOLDOWN_MS: raw.RELEASE_COOLDOWN_MS || DEFAULTS.RELEASE_COOLDOWN_MS,
    WEBSEARCH_PROVIDER: raw.WEBSEARCH_PROVIDER || DEFAULTS.WEBSEARCH_PROVIDER,
    ...loadScaling(raw),
  };
}

function saveConfig(config) {
  if (!fs.existsSync(CONFIG_DIR)) fs.mkdirSync(CONFIG_DIR, { recursive: true, mode: 0o700 });
  fs.chmodSync(CONFIG_DIR, 0o700);
  fs.writeFileSync(CONFIG_FILE, JSON.stringify({ ...config, ...loadScaling(config) }, null, 2) + '\n', { mode: 0o600 });
  fs.chmodSync(CONFIG_FILE, 0o600);
}

function mask(value) {
  if (!value) return '(empty)';
  if (value.length <= 10) return '(set)';
  return `${value.slice(0, 5)}...${value.slice(-4)}`;
}

function parseArg(name) {
  const prefix = `--${name}=`;
  const hit = process.argv.find((arg) => arg.startsWith(prefix));
  return hit ? hit.slice(prefix.length) : null;
}

function applyArgs(config) {
  const map = {
    listen: 'LISTEN_ADDR',
    key: 'API_KEY',
    sessionCookie: 'SESSION_COOKIE',
    models: 'ENABLED_MODELS',
    proxyKeys: 'API_KEYS',
    timeout: 'REQUEST_TIMEOUT',
    logging: 'REQUEST_LOGGING',
    concurrency: 'OVERRIDE_CONCURRENCY',
    releaseCooldown: 'RELEASE_COOLDOWN_MS',
    websearch: 'WEBSEARCH_PROVIDER',
  };
  for (const [arg, field] of Object.entries(map)) {
    const value = parseArg(arg);
    if (value == null) continue;
    config[field] = Array.isArray(DEFAULTS[field]) ? cleanList(value) : (typeof DEFAULTS[field] === 'number' ? Math.max(0, Number(value) || 0) : value.trim());
  }
  return config;
}

function question(rl, prompt) {
  return new Promise((resolve) => rl.question(prompt, resolve));
}
async function promptSettings(config) {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  try {
    console.log('\nUMANS Proxy settings. Press Enter to keep current value.');
    console.log('Comma-separate model IDs and proxy API keys.');
    console.log('Leave UMANS API key blank to keep the existing saved/env key.\n');

    const listen = await question(rl, `Listen address [${config.LISTEN_ADDR}]: `);
    if (listen.trim()) config.LISTEN_ADDR = listen.trim();

    const timeout = await question(rl, `Request timeout [${config.REQUEST_TIMEOUT}]: `);
    if (timeout.trim()) config.REQUEST_TIMEOUT = timeout.trim();

    const logging = await question(rl, `Request logging (off|basic|verbose) [${config.REQUEST_LOGGING}]: `);
    if (logging.trim()) config.REQUEST_LOGGING = logging.trim();

    const concurrency = await question(rl, `Override concurrency, 0 = use UMANS limits [${config.OVERRIDE_CONCURRENCY}]: `);
    if (concurrency.trim()) config.OVERRIDE_CONCURRENCY = Math.max(0, Number(concurrency) || 0);

    const releaseCooldown = await question(rl, `Slot release cooldown, e.g. 2s or 1500ms (rest before a freed permit is reusable) [${config.RELEASE_COOLDOWN_MS}]: `);
    if (releaseCooldown.trim()) config.RELEASE_COOLDOWN_MS = releaseCooldown.trim();

    const websearch = await question(rl, `Websearch provider (native|exa|none) [${config.WEBSEARCH_PROVIDER}]: `);
    if (websearch.trim()) config.WEBSEARCH_PROVIDER = websearch.trim();

    const apiKey = await question(rl, `UMANS API key [${mask(config.API_KEY)}]: `);
    if (apiKey.trim()) config.API_KEY = apiKey.trim();

    const sessionCookie = await question(rl, `UMANS session cookie (for app.umans.ai account endpoints) [${mask(config.SESSION_COOKIE)}]: `);
    if (sessionCookie.trim()) config.SESSION_COOKIE = sessionCookie.trim();
    const models = await question(rl, `Enabled models [${config.ENABLED_MODELS.join(', ')}]: `);
    if (models.trim()) config.ENABLED_MODELS = cleanList(models);

    const proxyKeys = await question(rl, `Proxy API keys [${config.API_KEYS.map(mask).join(', ')}]: `);
    if (proxyKeys.trim()) config.API_KEYS = cleanList(proxyKeys);

    return config;
  } finally {
    rl.close();
  }
}
function printSettings(config) {
  console.log('\nCurrent settings:');
  console.log(`  LISTEN_ADDR:       ${config.LISTEN_ADDR}`);
  console.log(`  REQUEST_TIMEOUT:   ${config.REQUEST_TIMEOUT}`);
  console.log(`  REQUEST_LOGGING:   ${config.REQUEST_LOGGING}`);
  console.log(`  OVERRIDE_CONCURRENCY: ${config.OVERRIDE_CONCURRENCY}`);
  console.log(`  RELEASE_COOLDOWN_MS:  ${config.RELEASE_COOLDOWN_MS}`);
  console.log(`  WEBSEARCH_PROVIDER:   ${config.WEBSEARCH_PROVIDER}`);
  console.log(`  API_KEY:           ${mask(config.API_KEY)}`);
  console.log(`  SESSION_COOKIE:    ${mask(config.SESSION_COOKIE)}`);
  console.log(`  ENABLED_MODELS:    ${config.ENABLED_MODELS.join(', ') || '(all upstream models)'}`);
  console.log(`  API_KEYS:          ${config.API_KEYS.map(mask).join(', ') || '(proxy auth disabled)'}`);
}

function startProxy() {
  const child = spawn(process.execPath, ['proxy.js'], { cwd: __dirname, stdio: 'inherit' });
  child.on('exit', (code, signal) => process.exit(signal ? 1 : (code || 0)));
}

function hasValidSettings() {
  const raw = readJSON(CONFIG_FILE);
  const key = raw.API_KEY;
  return !!key && key !== DEFAULTS.API_KEY;
}

async function main() {
  let config = applyArgs(loadConfig());
  const nonInteractive = process.argv.includes('--yes') || process.argv.includes('--start') || process.argv.some((arg) => arg.startsWith('--')) || hasValidSettings();
  if (!nonInteractive) config = await promptSettings(config);
  saveConfig(config);
  printSettings(config);
  if (!process.argv.includes('--no-start')) startProxy();
}

main().catch((err) => {
  console.error(err.message);
  process.exit(1);
});
