/**
 * Hero section unit tests — Scenario 1.
 *
 * Validates REQ-1, REQ-2, and User Story US-1 by inspecting the DOM
 * that results from loading public/index.html. Each `test` corresponds
 * to a test_case id in .something/scenario.json.
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

describe('Hero section', () => {
  let doc;
  let content;

  beforeAll(() => {
    doc = loadFullPage();
    content = readContent();
  });

  test('test_case 1: <section id="hero"> is present in the document', () => {
    const hero = doc.getElementById('hero');
    expect(hero).not.toBeNull();
    expect(hero.tagName.toLowerCase()).toBe('section');
    expect(hero.id).toBe('hero');
  });

  test('test_case 2: hero contains a logo <img> with non-empty src and descriptive alt', () => {
    const hero = doc.getElementById('hero');
    const logo = hero.querySelector('img');
    expect(logo).not.toBeNull();

    const src = logo.getAttribute('src') || '';
    expect(src.length).toBeGreaterThan(0);
    expect(src).toMatch(/logo\.svg$/);

    const alt = logo.getAttribute('alt') || '';
    expect(alt.length).toBeGreaterThan(0);
    expect(alt.toLowerCase()).toContain('mirdb');
  });

  test("test_case 3: hero <h1> contains the product name 'MirDB'", () => {
    const hero = doc.getElementById('hero');
    const h1 = hero.querySelector('h1');
    expect(h1).not.toBeNull();
    expect(h1.textContent.trim()).toBe('MirDB');
  });

  test("test_case 4: tagline mentions 'persistent key-value store' and 'Memcached'", () => {
    const hero = doc.getElementById('hero');
    const tagline =
      hero.querySelector('.tagline') ||
      hero.querySelector('.hero-tagline') ||
      hero.querySelector('p');
    expect(tagline).not.toBeNull();

    const text = tagline.textContent.toLowerCase();
    expect(text).toContain('persistent key-value store');
    expect(text).toContain('memcached');
  });

  test('test_case 5: primary CTA links to GitHub, opens in new tab, has rel=noopener', () => {
    const hero = doc.getElementById('hero');
    const primary =
      hero.querySelector('.cta-primary') ||
      hero.querySelector('[data-cta="primary"]');
    expect(primary).not.toBeNull();
    expect(primary.tagName.toLowerCase()).toBe('a');

    const label = primary.textContent.trim();
    expect(label).toMatch(/View on GitHub|Get Started/i);

    const href = primary.getAttribute('href') || '';
    expect(href).toContain('github.com');

    expect(primary.getAttribute('target')).toBe('_blank');

    const rel = primary.getAttribute('rel') || '';
    expect(rel).toContain('noopener');
  });

  test("test_case 6: secondary CTA labelled 'Read Documentation' with href matching content.json docs URL", () => {
    const hero = doc.getElementById('hero');
    const secondary =
      hero.querySelector('.cta-secondary') ||
      hero.querySelector('[data-cta="secondary"]');
    expect(secondary).not.toBeNull();
    expect(secondary.tagName.toLowerCase()).toBe('a');

    const label = secondary.textContent.trim();
    expect(label).toMatch(/Read Documentation|Documentation/i);

    const href = secondary.getAttribute('href') || '';
    expect(href.length).toBeGreaterThan(0);

    const docsUrl = content.links && content.links.docs;
    expect(typeof docsUrl).toBe('string');
    expect(docsUrl.length).toBeGreaterThan(0);
    expect(href).toBe(docsUrl);
  });

  test('partial-loader sanity: hero.html partial parses on its own', () => {
    const partial = loadPartial('src/components/hero/hero.html');
    expect(partial.getElementById('hero')).not.toBeNull();
  });
});
