'use strict';

const state = require('./state');

function requestLoggingMode() {
  return state.config?.requestLogging || 'off';
}

function requestLoggingEnabled(verbose = false) {
  const mode = requestLoggingMode();
  return mode === 'verbose' || (!verbose && mode === 'basic');
}

function formatRequestLogData(data) {
  if (data == null || data === '') return '';
  if (typeof data === 'string') return data;
  try { return JSON.stringify(data); }
  catch { return String(data); }
}

function logRequest(tag, message, data, { verbose = false } = {}) {
  if (!requestLoggingEnabled(verbose)) return;
  const suffix = data == null ? '' : ` ${formatRequestLogData(data)}`;
  console.log(`${tag}: ${message}${suffix}`);
}

module.exports = {
  requestLoggingMode,
  requestLoggingEnabled,
  formatRequestLogData,
  logRequest,
};
