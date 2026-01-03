/**
 * Unit tests for Hero Section HTML Structure
 * Tests TC1-TC5: Hero section display and semantic HTML validation
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

describe('Hero Section HTML Structure', () => {
  let document;
  let heroSection;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
    heroSection = document.querySelector('.hero');
  });

  // Test Case 1: Hero section contains 'MirDB' text element
  test('TC1: hero section contains MirDB text element', () => {
    expect(heroSection).not.toBeNull();

    const h1 = heroSection.querySelector('h1');
    expect(h1).not.toBeNull();
    expect(h1.textContent).toBe('MirDB');
  });

  // Test Case 2: Tagline contains required keywords
  test('TC2: tagline contains required keywords', () => {
    expect(heroSection).not.toBeNull();

    const tagline = heroSection.querySelector('.tagline');
    expect(tagline).not.toBeNull();

    const taglineText = tagline.textContent.toLowerCase();
    expect(taglineText).toContain('persistent key-value store');
    expect(taglineText).toContain('memcached');
  });

  // Test Case 3: Get Started button links to quickstart section
  test('TC3: Get Started button has href to quickstart section', () => {
    expect(heroSection).not.toBeNull();

    const ctaButtons = heroSection.querySelector('.cta-buttons');
    expect(ctaButtons).not.toBeNull();

    const getStartedBtn = ctaButtons.querySelector('a.btn-primary');
    expect(getStartedBtn).not.toBeNull();
    expect(getStartedBtn.textContent).toBe('Get Started');

    const href = getStartedBtn.getAttribute('href');
    expect(href).toMatch(/#quick[-]?start/i);
  });

  // Test Case 4: View on GitHub button has correct attributes
  test('TC4: View on GitHub button has correct href and attributes', () => {
    expect(heroSection).not.toBeNull();

    const ctaButtons = heroSection.querySelector('.cta-buttons');
    expect(ctaButtons).not.toBeNull();

    const githubBtn = ctaButtons.querySelector('a.btn-secondary');
    expect(githubBtn).not.toBeNull();
    expect(githubBtn.textContent).toContain('GitHub');

    // Check href
    const href = githubBtn.getAttribute('href');
    expect(href).toContain('github.com/yetone/mirdb');

    // Check target="_blank"
    expect(githubBtn.getAttribute('target')).toBe('_blank');

    // Check rel="noopener"
    const rel = githubBtn.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  // Test Case 5: Hero section has semantic HTML elements
  test('TC5: hero section has semantic HTML structure with h1, p, a elements', () => {
    expect(heroSection).not.toBeNull();

    // Check for h1 element
    const h1 = heroSection.querySelector('h1');
    expect(h1).not.toBeNull();
    expect(h1.textContent).toBe('MirDB');

    // Check for paragraph elements (at least tagline and description)
    const paragraphs = heroSection.querySelectorAll('p');
    expect(paragraphs.length).toBeGreaterThanOrEqual(2);

    // Check for anchor/button elements in CTA section
    const ctaContainer = heroSection.querySelector('.cta-buttons');
    expect(ctaContainer).not.toBeNull();

    const ctaLinks = ctaContainer.querySelectorAll('a');
    expect(ctaLinks.length).toBe(2);

    // Verify Get Started is anchor
    const getStartedBtn = ctaContainer.querySelector('a:first-child');
    expect(getStartedBtn).not.toBeNull();

    // Verify GitHub button is anchor
    const githubBtn = ctaContainer.querySelector('a:last-child');
    expect(githubBtn).not.toBeNull();
  });

  test('Hero section has container structure', () => {
    expect(heroSection).not.toBeNull();

    const container = heroSection.querySelector('.container');
    expect(container).not.toBeNull();
  });

  test('CTA buttons have proper styling classes', () => {
    const ctaButtons = heroSection.querySelector('.cta-buttons');
    expect(ctaButtons).not.toBeNull();

    const primaryBtn = ctaButtons.querySelector('.btn.btn-primary');
    expect(primaryBtn).not.toBeNull();

    const secondaryBtn = ctaButtons.querySelector('.btn.btn-secondary');
    expect(secondaryBtn).not.toBeNull();
  });
});
