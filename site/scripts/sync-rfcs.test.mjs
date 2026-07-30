import assert from 'node:assert/strict';
import { test } from 'node:test';
import { isRfcFile, transformRfc } from './sync-rfcs.mjs';

const source = [
  '# RFC 0016: Equality and range indexes',
  '',
  '- **Date:** 2026-07-10',
  '',
  '## Summary',
  '',
  'Verbs live on the [RFC 0003](0003-public-api-shape.md) surface, retried',
  'under [RFC 0004](0004-commit-protocol.md#retry). Encoding details are in',
  '[RFC 0002](0002-slatedb-key-encoding.md). Start from the',
  '[template](0000-template.md) or the [core crate](../../crates/moraine).',
  'See [SlateDB](https://slatedb.io) and [below](#summary).',
].join('\n');

const result = transformRfc('0016-equality-and-range-indexes.md', source);

test('frontmatter carries the H1 title, sidebar label, and numeric order', () => {
  assert.match(result, /^---\ntitle: "RFC 0016: Equality and range indexes"\n/);
  assert.match(
    result,
    /sidebar: \{ label: "0016: Equality and range indexes", order: 16 \}\n---\n/,
  );
});

test('the H1 is stripped from the body', () => {
  assert.ok(!result.includes('# RFC 0016:'));
  assert.ok(result.includes('- **Date:** 2026-07-10'));
});

test('sibling RFC links become relative page links, anchors preserved', () => {
  assert.ok(result.includes('](../0003-public-api-shape/)'));
  assert.ok(result.includes('](../0004-commit-protocol/#retry)'));
  assert.ok(result.includes('](../0002-slatedb-key-encoding/)'));
});

test('excluded and out-of-dir targets fall back to GitHub URLs', () => {
  assert.ok(
    result.includes(
      '](https://github.com/morainedb/moraine/blob/main/docs/rfcs/0000-template.md)',
    ),
  );
  assert.ok(
    result.includes('](https://github.com/morainedb/moraine/blob/main/crates/moraine)'),
  );
});

test('external links and pure anchors are untouched', () => {
  assert.ok(result.includes('](https://slatedb.io)'));
  assert.ok(result.includes('](#summary)'));
});

test('a source with no H1 is an error', () => {
  assert.throws(() => transformRfc('0099-broken.md', 'no title here'), /no H1/);
});

test('only numbered RFC files are synced', () => {
  assert.ok(isRfcFile('0022-commit-log-and-leader-role.md'));
  assert.ok(!isRfcFile('tasks.md'));
  assert.ok(!isRfcFile('README.md'));
  assert.ok(!isRfcFile('0000-template.md'));
});
