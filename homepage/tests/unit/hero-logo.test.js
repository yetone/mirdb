/**
 * Hero Logo Unit Tests
 * Owner: Scenario 1 - Hero Section Display (Test Case 3)
 *
 * Tests for:
 * - Logo image element exists with src pointing to assets/logo.gif
 * - Logo has appropriate alt text
 */

const { loadHomepageHTML } = require('../setup/test-utils');

describe('Hero Logo GIF Element', () => {
  beforeEach(() => {
    // Load the homepage HTML into JSDOM
    const html = loadHomepageHTML();
    document.body.innerHTML = html;
  });

  test('TC3: Logo image element exists with src pointing to assets/logo.gif', () => {
    // Find the hero logo element
    const logoElement = document.querySelector('.hero-logo');

    // Verify the element exists
    expect(logoElement).toBeTruthy();
    expect(logoElement.tagName.toLowerCase()).toBe('img');

    // Verify the src attribute points to assets/logo.gif
    const src = logoElement.getAttribute('src');
    expect(src).toBeTruthy();
    expect(src).toContain('assets/logo.gif');
  });

  test('Logo has appropriate alt text', () => {
    const logoElement = document.querySelector('.hero-logo');

    // Verify alt text exists
    const altText = logoElement.getAttribute('alt');
    expect(altText).toBeTruthy();

    // Verify alt text is descriptive (contains 'logo' or 'MirDB')
    const altLower = altText.toLowerCase();
    expect(altLower).toMatch(/logo|mirdb/);
  });

  test('Logo has correct ID for targeting', () => {
    const logoElement = document.querySelector('#hero-logo');

    // Verify the element can be found by ID
    expect(logoElement).toBeTruthy();
    expect(logoElement.classList.contains('hero-logo')).toBe(true);
  });

  test('Logo is inside the hero section', () => {
    const heroSection = document.querySelector('#hero');
    const logoElement = document.querySelector('.hero-logo');

    // Verify logo is a descendant of hero section
    expect(heroSection).toBeTruthy();
    expect(heroSection.contains(logoElement)).toBe(true);
  });
});
