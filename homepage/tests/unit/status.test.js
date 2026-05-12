/**
 * Status section unit tests — Scenario 5.
 *
 * Validates REQ-6 and User Story US-5 by inspecting the DOM that
 * results from loading public/index.html. Each `test` corresponds to
 * a test_case id in .something/scenario.json.
 */

const fs = require('fs');
const path = require('path');
const { loadFullPage, loadPartial } = require('../helpers/dom');

const REPO_ROOT = path.resolve(__dirname, '..', '..');

function readContent() {
  const raw = fs.readFileSync(
    path.join(REPO_ROOT, 'src', 'data', 'content.json'),
    'utf8'
  );
  return JSON.parse(raw);
}

describe('Status section', () => {
  let doc;
  let content;

  beforeAll(() => {
    doc = loadFullPage();
    content = readContent();
  });

  test('test_case 1: <section id="status"> is present in the document', () => {
    const status = doc.getElementById('status');
    expect(status).not.toBeNull();
    expect(status.tagName.toLowerCase()).toBe('section');
    expect(status.id).toBe('status');
  });

  test('test_case 2: CI badge <img> has src matching content.json#status.ciBadgeUrl and meaningful alt', () => {
    const status = doc.getElementById('status');
    const badge = status.querySelector('img');
    expect(badge).not.toBeNull();

    const src = badge.getAttribute('src') || '';
    expect(src.length).toBeGreaterThan(0);
    expect(typeof content.status.ciBadgeUrl).toBe('string');
    expect(content.status.ciBadgeUrl.length).toBeGreaterThan(0);
    expect(src).toBe(content.status.ciBadgeUrl);

    const alt = badge.getAttribute('alt') || '';
    expect(alt.length).toBeGreaterThan(0);
    const altLower = alt.toLowerCase();
    expect(altLower.includes('build') || altLower.includes('ci')).toBe(true);
  });

  test("test_case 3: badge's parent anchor links to ciLinkUrl, opens in a new tab, includes rel=noopener", () => {
    const status = doc.getElementById('status');
    const badge = status.querySelector('img');
    expect(badge).not.toBeNull();

    const anchor = badge.closest('a');
    expect(anchor).not.toBeNull();
    expect(anchor.tagName.toLowerCase()).toBe('a');

    const href = anchor.getAttribute('href') || '';
    expect(href.length).toBeGreaterThan(0);
    expect(typeof content.status.ciLinkUrl).toBe('string');
    expect(content.status.ciLinkUrl.length).toBeGreaterThan(0);
    expect(href).toBe(content.status.ciLinkUrl);

    expect(anchor.getAttribute('target')).toBe('_blank');

    const rel = anchor.getAttribute('rel') || '';
    expect(rel).toContain('noopener');
  });

  test('test_case 4: .version text matches /^v?\\d+\\.\\d+\\.\\d+/ and matches content.json#status.version', () => {
    const status = doc.getElementById('status');
    const versionEl = status.querySelector('.version');
    expect(versionEl).not.toBeNull();

    const text = versionEl.textContent.trim();
    expect(text.length).toBeGreaterThan(0);
    expect(text).toMatch(/^v?\d+\.\d+\.\d+/);

    const expectedVersion = content.status.version;
    expect(typeof expectedVersion).toBe('string');
    expect(expectedVersion.length).toBeGreaterThan(0);

    const normalized = text.startsWith('v') ? text.slice(1) : text;
    const expectedNormalized = expectedVersion.startsWith('v')
      ? expectedVersion.slice(1)
      : expectedVersion;
    expect(normalized).toBe(expectedNormalized);
  });

  test('test_case 5: content.json has non-empty status.ciBadgeUrl, status.ciLinkUrl, status.version', () => {
    expect(content.status).toBeDefined();
    expect(typeof content.status).toBe('object');

    const { ciBadgeUrl, ciLinkUrl, version } = content.status;

    expect(typeof ciBadgeUrl).toBe('string');
    expect(ciBadgeUrl.length).toBeGreaterThan(0);

    expect(typeof ciLinkUrl).toBe('string');
    expect(ciLinkUrl.length).toBeGreaterThan(0);

    expect(typeof version).toBe('string');
    expect(version.length).toBeGreaterThan(0);
    expect(version).toMatch(/^\d+\.\d+\.\d+/);
  });

  test('partial-loader sanity: status.html partial parses on its own and contains a badge + version', () => {
    const partial = loadPartial('src/components/status/status.html');
    const status = partial.getElementById('status');
    expect(status).not.toBeNull();
    expect(status.querySelector('img')).not.toBeNull();
    expect(status.querySelector('.version')).not.toBeNull();
  });
});
