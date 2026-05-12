/**
 * Unit tests for the Features section.
 *
 * Owner: Scenario 2 — Features grid (REQ-3, US-2).
 *
 * The tests assemble the full public/index.html into the jsdom Document
 * via the shared loadFullPage helper and then assert:
 *   - the section + grid markup exists
 *   - card count is in the required 6-8 range
 *   - every REQ-3 capability has its own card keyed by data-feature
 *   - each card has icon / title / description
 *   - card titles surface the canonical capability names
 *   - content.json is the source of truth for the cards
 */

const fs = require('fs');
const path = require('path');

const { loadFullPage, loadPartial } = require('../helpers/dom');
const { REQUIRED_FEATURE_IDS } = require('../helpers/fixtures');

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

describe('Features section', () => {
  let doc;

  beforeEach(() => {
    doc = loadFullPage();
  });

  test('test_case_1: features section and grid exist', () => {
    const section = doc.getElementById('features');
    expect(section).not.toBeNull();
    expect(section.tagName.toLowerCase()).toBe('section');

    const grid = section.querySelector('.features-grid');
    expect(grid).not.toBeNull();
    expect(grid.tagName.toLowerCase()).toBe('ul');
  });

  test('test_case_2: grid has between 6 and 8 cards inclusive', () => {
    const grid = doc.querySelector('#features .features-grid');
    const cards = grid.children;
    expect(cards.length).toBeGreaterThanOrEqual(6);
    expect(cards.length).toBeLessThanOrEqual(8);
  });

  test('test_case_3: every required REQ-3 capability has a card', () => {
    for (const id of REQUIRED_FEATURE_IDS) {
      const matches = doc.querySelectorAll(`[data-feature="${id}"]`);
      expect(matches.length).toBe(1);
      expect(matches[0]).not.toBeNull();
    }
  });

  test('test_case_4: each card has one svg icon, one h3 title, one p description', () => {
    const cards = doc.querySelectorAll('#features .feature-card');
    expect(cards.length).toBeGreaterThan(0);

    cards.forEach((card) => {
      const svgs = card.querySelectorAll('svg');
      const headings = card.querySelectorAll('h3');
      const paragraphs = card.querySelectorAll('p');

      expect(svgs.length).toBe(1);
      expect(headings.length).toBe(1);
      expect(paragraphs.length).toBe(1);

      expect(svgs[0]).not.toBeNull();
      expect(headings[0].textContent.trim().length).toBeGreaterThan(0);
      expect(paragraphs[0].textContent.trim().length).toBeGreaterThan(0);
    });
  });

  test('test_case_5: memcached card title mentions Memcached', () => {
    const card = doc.querySelector('[data-feature="memcached"]');
    const heading = card.querySelector('h3');
    expect(heading.textContent.toLowerCase()).toContain('memcached');
  });

  test('test_case_6: lsm-tree card title mentions LSM', () => {
    const card = doc.querySelector('[data-feature="lsm-tree"]');
    const heading = card.querySelector('h3');
    expect(heading.textContent.toLowerCase()).toContain('lsm');
  });

  test('test_case_7: wal card title mentions WAL or Write-Ahead Log', () => {
    const card = doc.querySelector('[data-feature="wal"]');
    const heading = card.querySelector('h3');
    const lower = heading.textContent.toLowerCase();
    expect(lower.includes('wal') || lower.includes('write-ahead log')).toBe(true);
  });

  test('test_case_8: content.json features schema matches the required IDs', () => {
    const content = loadContent();
    expect(Array.isArray(content.features)).toBe(true);

    const ids = content.features.map((f) => f.id);
    for (const required of REQUIRED_FEATURE_IDS) {
      expect(ids).toContain(required);
    }

    content.features.forEach((feature) => {
      expect(typeof feature.id).toBe('string');
      expect(typeof feature.title).toBe('string');
      expect(typeof feature.description).toBe('string');
      expect(typeof feature.icon).toBe('string');
      expect(feature.id.length).toBeGreaterThan(0);
      expect(feature.title.length).toBeGreaterThan(0);
      expect(feature.description.length).toBeGreaterThan(0);
      expect(feature.icon.length).toBeGreaterThan(0);
    });
  });

  test('regression: features.html partial loads on its own', () => {
    // Defensive check so that if the build pipeline ever bypasses partial
    // inlining, this test still surfaces a broken features partial.
    const partial = loadPartial('src/components/features/features.html');
    expect(partial.querySelector('#features')).not.toBeNull();
    expect(partial.querySelectorAll('.feature-card').length).toBeGreaterThanOrEqual(6);
  });
});
