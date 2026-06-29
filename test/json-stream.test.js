'use strict';
require('./fetch-guard');

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { JsonStream } = require('../lib/json-stream');

function parse(json, chunkSize = 1) {
  const values = [];
  const parser = new JsonStream({ onValue: (path, value) => values.push({ path, value }) });
  for (let i = 0; i < json.length; i += chunkSize) {
    parser.push(json.slice(i, i + chunkSize));
  }
  parser.flush();
  return values;
}

function find(values, pathStr) {
  const target = pathStr.split('.');
  return values.find((v) => v.path.length === target.length && v.path.every((p, i) => String(p) === target[i]));
}

test('flat object', () => {
  const v = parse('{"a":1,"b":"hello","c":true,"d":null}');
  assert.strictEqual(find(v, 'a').value, 1);
  assert.strictEqual(find(v, 'b').value, 'hello');
  assert.strictEqual(find(v, 'c').value, true);
  assert.strictEqual(find(v, 'd').value, null);
});

test('nested object', () => {
  const v = parse('{"usage":{"completion_tokens":42,"prompt_tokens":100}}');
  assert.strictEqual(find(v, 'usage.completion_tokens').value, 42);
  assert.strictEqual(find(v, 'usage.prompt_tokens').value, 100);
});

test('array of objects', () => {
  const v = parse('[{"content":"hi"},{"content":"bye"}]');
  assert.strictEqual(find(v, '0.content').value, 'hi');
  assert.strictEqual(find(v, '1.content').value, 'bye');
});

test('choices array (OpenAI shape)', () => {
  const v = parse('{"choices":[{"message":{"content":"hello world"}}],"usage":{"completion_tokens":5}}');
  assert.strictEqual(find(v, 'choices.0.message.content').value, 'hello world');
  assert.strictEqual(find(v, 'usage.completion_tokens').value, 5);
});

test('string with escapes', () => {
  const v = parse('{"text":"line\\nbreak\\ttab\\\\back\\"quote"}');
  assert.strictEqual(find(v, 'text').value, 'line\nbreak\ttab\\back"quote');
});

test('unicode escapes', () => {
  const v = parse('{"emoji":"\\u0041"}');
  assert.strictEqual(find(v, 'emoji').value, 'A');
});

test('chunk boundary splits a string value', () => {
  // Feed char by char — the string "hello" gets split across pushes
  const v = parse('{"x":"hello"}', 1);
  assert.strictEqual(find(v, 'x').value, 'hello');
});

test('chunk boundary splits a number', () => {
  const v = parse('{"n":12345}', 2);
  assert.strictEqual(find(v, 'n').value, 12345);
});

test('chunk boundary splits a key', () => {
  const v = parse('{"longkey":"v"}', 1);
  assert.strictEqual(find(v, 'longkey').value, 'v');
});

test('chunk boundary in escape sequence', () => {
  // \n split across boundary: \ at end of chunk, n at start of next
  const v = parse('{"x":"a\\nb"}', 3);
  assert.strictEqual(find(v, 'x').value, 'a\nb');
});

test('deeply nested', () => {
  const v = parse('{"a":{"b":{"c":{"d":42}}}}');
  assert.strictEqual(find(v, 'a.b.c.d').value, 42);
});

test('empty array', () => {
  const v = parse('{"arr":[]}');
  assert.strictEqual(v.length, 0);
});

test('empty object', () => {
  const v = parse('{"obj":{}}');
  assert.strictEqual(v.length, 0);
});

test('float numbers', () => {
  const v = parse('{"f":3.14,"e":1e5,"neg":-2.5}');
  assert.strictEqual(find(v, 'f').value, 3.14);
  assert.strictEqual(find(v, 'e').value, 100000);
  assert.strictEqual(find(v, 'neg').value, -2.5);
});

test('array of mixed types', () => {
  const v = parse('[1,"two",true,null,4]');
  assert.strictEqual(v[0].value, 1);
  assert.strictEqual(v[1].value, 'two');
  assert.strictEqual(v[2].value, true);
  assert.strictEqual(v[3].value, null);
  assert.strictEqual(v[4].value, 4);
});

test('large chunk (full document at once)', () => {
  const json = '{"choices":[{"message":{"content":"hello"}}],"usage":{"completion_tokens":1,"prompt_tokens":2}}';
  const v = parse(json, 1000);
  assert.strictEqual(find(v, 'choices.0.message.content').value, 'hello');
  assert.strictEqual(find(v, 'usage.completion_tokens').value, 1);
});

test('Anthropic content blocks', () => {
  const v = parse('{"content":[{"type":"text","text":"hello"},{"type":"thinking","thinking":"deep"}],"usage":{"output_tokens":10}}');
  assert.strictEqual(find(v, 'content.0.text').value, 'hello');
  assert.strictEqual(find(v, 'content.1.thinking').value, 'deep');
  assert.strictEqual(find(v, 'usage.output_tokens').value, 10);
});

test('string with nested quotes', () => {
  const v = parse('{"content":"He said \\"hi\\" to her"}');
  assert.strictEqual(find(v, 'content').value, 'He said "hi" to her');
});

test('early termination: stop after finding target field', () => {
  const json = '{"usage":{"completion_tokens":42},"leftover":"data that should never be parsed"}';
  let seen = null;
  const parser = new JsonStream({
    onValue: (path, value) => {
      if (path.join('.') === 'usage.completion_tokens') {
        seen = value;
        parser.done = true;
      }
    },
  });
  parser.push(json);
  assert.strictEqual(seen, 42);
  // After done, push is a no-op
  parser.push('{"more":"data"}');
  assert.strictEqual(seen, 42);
});

test('early termination: stop after content string (OpenAI non-stream)', () => {
  // Simulates the tap: extract usage + content, then stop
  const json = '{"choices":[{"message":{"content":"hello world"}}],"usage":{"completion_tokens":5,"prompt_tokens":10}}';
  const got = {};
  const parser = new JsonStream({
    onValue: (path, value) => {
      const p = path.join('.');
      if (p === 'choices.0.message.content') got.content = value;
      if (p === 'usage.completion_tokens') got.completionTokens = value;
      if (p === 'usage.prompt_tokens') got.promptTokens = value;
      if (got.content && got.completionTokens != null && got.promptTokens != null) parser.done = true;
    },
  });
  parser.push(json);
  assert.strictEqual(got.content, 'hello world');
  assert.strictEqual(got.completionTokens, 5);
  assert.strictEqual(got.promptTokens, 10);
});
