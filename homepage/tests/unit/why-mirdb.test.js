/**
 * Unit tests for the "Why MirDB?" differentiation section.
 *
 * Owner: Scenario 7 — "Why MirDB?" differentiation (REQ-8).
 *
 * Tests map 1:1 onto the six cases in .something/scenario.json:
 *   1. #why-mirdb section + h2 matching /why mirdb/i
 *   2. li[data-competitor='memcached'] mentions 'persistence' or 'persistent'
 *   3. li[data-competitor='redis']     mentions 'Memcached protocol' or 'simplicity'
 *   4. li[data-competitor='rocksdb']   mentions 'network protocol' or 'out of the box'
 *   5. Aggregated section textContent mentions Rust, LSM (or
 *      "Log-Structured Merge"), and compaction (case-insensitive)
 *   6. content.json#whyMirDB is an array with ≥3 well-formed entries
 *
 * Tests assert against the assembled public/index.html (the file users
 * actually load) and also run a standalone-partial sanity check, matching
 * the convention used by hero.test.js / roadmap.test.js on this project.
 */

const fs = require('fs');
const path = require('path');

const { loadFullPage, loadPartial } = require('../helpers/dom');

function loadContent() {
  const contentPath = path.resolve(
    __dirname,
    '..',
    '..',
    'src',
    'data',
    'content.json'
  );
  return JSON.parse(fs.readFileSync(contentPath, 'utf8'));
}

describe('Why MirDB? section', () => {
  let doc;

  beforeEach(() => {
    doc = loadFullPage();
  });

  test('test_case_1: section element is present with an h2 matching /why mirdb/i', () => {
    const section = doc.getElementById('why-mirdb');
    expect(section).not.toBeNull();
    expect(section.tagName.toLowerCase()).toBe('section');

    const heading = section.querySelector('h2');
    expect(heading).not.toBeNull();
    expect(heading.textContent).toMatch(/why mirdb/i);
  });

  test('test_case_2: memcached comparison mentions persistence/persistent', () => {
    const items = doc.querySelectorAll(
      "#why-mirdb li[data-competitor='memcached']"
    );
    expect(items.length).toBe(1);

    const text = items[0].textContent.toLowerCase();
    expect(/(persistence|persistent)/i.test(text)).toBe(true);
  });

  test('test_case_3: redis comparison mentions Memcached protocol or simplicity', () => {
    const items = doc.querySelectorAll(
      "#why-mirdb li[data-competitor='redis']"
    );
    expect(items.length).toBe(1);

    const text = items[0].textContent;
    expect(/memcached protocol|simplicity/i.test(text)).toBe(true);
  });

  test('test_case_4: rocksdb comparison mentions network protocol or out of the box', () => {
    const items = doc.querySelectorAll(
      "#why-mirdb li[data-competitor='rocksdb']"
    );
    expect(items.length).toBe(1);

    const text = items[0].textContent;
    expect(/network protocol|out of the box/i.test(text)).toBe(true);
  });

  test('test_case_5: aggregated section text mentions Rust, LSM, and compaction', () => {
    const section = doc.getElementById('why-mirdb');
    const text = section.textContent;

    expect(/rust/i.test(text)).toBe(true);
    expect(/lsm|log-structured merge/i.test(text)).toBe(true);
    expect(/compaction/i.test(text)).toBe(true);
  });

  test('test_case_6: content.json whyMirDB array shape is valid', () => {
    const content = loadContent();
    expect(Array.isArray(content.whyMirDB)).toBe(true);
    expect(content.whyMirDB.length).toBeGreaterThanOrEqual(3);

    const competitors = content.whyMirDB.map((entry) =>
      String(entry.competitor || '').toLowerCase()
    );
    expect(competitors).toEqual(
      expect.arrayContaining(['memcached', 'redis', 'rocksdb'])
    );

    content.whyMirDB.forEach((entry) => {
      expect(typeof entry.competitor).toBe('string');
      expect(entry.competitor.length).toBeGreaterThan(0);
      expect(typeof entry.difference).toBe('string');
      expect(entry.difference.length).toBeGreaterThan(0);
    });
  });

  test('regression: why-mirdb.html partial loads standalone', () => {
    const partial = loadPartial('src/components/why-mirdb/why-mirdb.html');
    const section = partial.getElementById('why-mirdb');
    expect(section).not.toBeNull();

    expect(
      partial.querySelectorAll("#why-mirdb li[data-competitor='memcached']").length
    ).toBe(1);
    expect(
      partial.querySelectorAll("#why-mirdb li[data-competitor='redis']").length
    ).toBe(1);
    expect(
      partial.querySelectorAll("#why-mirdb li[data-competitor='rocksdb']").length
    ).toBe(1);
  });
});
