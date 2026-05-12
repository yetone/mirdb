/**
 * Navigation unit tests — Scenario 4.
 *
 * Validates REQ-5 and User Story US-4 by inspecting the navigation DOM
 * and link attributes. Each `test` corresponds to a test_case id in
 * .something/scenario.json.
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

describe('Navigation', () => {
  let doc;
  let content;

  beforeAll(() => {
    doc = loadFullPage();
    content = readContent();
  });

  // test_case 1
  test('header.site-nav is present with navigation role and contains nav-menu', () => {
    const header = doc.querySelector('header.site-nav');
    expect(header).not.toBeNull();

    const role = header.getAttribute('role');
    expect(role).toBe('navigation');

    const navMenu = header.querySelector('ul#nav-menu');
    expect(navMenu).not.toBeNull();
  });

  // test_case 2
  test('nav-menu contains Features, Quick Start, and GitHub links', () => {
    const navMenu = doc.getElementById('nav-menu');
    expect(navMenu).not.toBeNull();

    const anchors = navMenu.querySelectorAll('a');
    const labels = Array.from(anchors).map((a) => a.textContent.trim());

    const lowerLabels = labels.map((l) => l.toLowerCase());
    expect(lowerLabels).toContain('features');
    expect(lowerLabels).toContain('quick start');
    expect(lowerLabels).toContain('github');
  });

  // test_case 3
  test('GitHub anchor has correct external link attributes', () => {
    const navMenu = doc.getElementById('nav-menu');
    const anchors = navMenu.querySelectorAll('a');

    const githubAnchor = Array.from(anchors).find((a) =>
      a.textContent.trim().toLowerCase().includes('github')
    );
    expect(githubAnchor).not.toBeNull();

    const href = githubAnchor.getAttribute('href') || '';
    expect(href).toContain('github.com');

    expect(githubAnchor.getAttribute('target')).toBe('_blank');

    const rel = githubAnchor.getAttribute('rel') || '';
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');
  });

  // test_case 4
  test('either nav or footer contains documentation and API reference links from content.json', () => {
    const navMenu = doc.getElementById('nav-menu');
    const footer = doc.querySelector('footer');

    const navAnchors = navMenu ? navMenu.querySelectorAll('a') : [];
    const footerAnchors = footer ? footer.querySelectorAll('a') : [];
    const allAnchors = Array.from(navAnchors).concat(Array.from(footerAnchors));

    const docsUrl = content.links && content.links.docs;
    const apiUrl = content.links && content.links.api;

    expect(typeof docsUrl).toBe('string');
    expect(typeof apiUrl).toBe('string');

    const docsFound = allAnchors.some((a) => {
      const href = a.getAttribute('href') || '';
      return href === docsUrl;
    });
    const apiFound = allAnchors.some((a) => {
      const href = a.getAttribute('href') || '';
      return href === apiUrl;
    });

    expect(docsFound).toBe(true);
    expect(apiFound).toBe(true);
  });

  test('partial-loader sanity: navigation.html partial parses on its own', () => {
    const partial = loadPartial('src/components/navigation/navigation.html');
    expect(partial.querySelector('header.site-nav')).not.toBeNull();
  });
});
