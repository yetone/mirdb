/**
 * Usage GIF Display Unit Tests
 * Owner: Scenario 15 - Usage GIF Display
 *
 * Tests for:
 * - Usage GIF element existence
 * - Usage GIF alt text for accessibility
 */

const { loadHomepageHTML } = require('../setup/test-utils');

describe('Usage GIF Display - Unit Tests', () => {
  beforeEach(() => {
    const html = loadHomepageHTML();
    document.body.innerHTML = html;
  });

  describe('TC1: Check for usage GIF element', () => {
    test('Image element with src pointing to assets/usage.gif exists', () => {
      const usageGif = document.querySelector('img[src*="usage.gif"]');
      expect(usageGif).toBeTruthy();
    });

    test('Usage GIF has correct src attribute', () => {
      const usageGif = document.querySelector('img[src*="usage.gif"]');
      expect(usageGif).toBeTruthy();

      const src = usageGif.getAttribute('src');
      expect(src).toContain('assets/usage.gif');
    });

    test('Usage GIF is in a dedicated section', () => {
      const usageDemoSection = document.querySelector('#usage-demo');
      expect(usageDemoSection).toBeTruthy();

      const usageGif = usageDemoSection.querySelector('img[src*="usage.gif"]');
      expect(usageGif).toBeTruthy();
    });

    test('Usage GIF has appropriate ID for targeting', () => {
      const usageGif = document.querySelector('#usage-gif');
      expect(usageGif).toBeTruthy();
      expect(usageGif.tagName.toLowerCase()).toBe('img');
    });

    test('Usage GIF has appropriate class for styling', () => {
      const usageGif = document.querySelector('img[src*="usage.gif"]');
      expect(usageGif).toBeTruthy();
      expect(usageGif.classList.contains('usage-gif')).toBe(true);
    });
  });

  describe('TC2: Verify usage GIF has alt text', () => {
    test('Usage GIF has descriptive alt text for accessibility', () => {
      const usageGif = document.querySelector('img[src*="usage.gif"]');
      expect(usageGif).toBeTruthy();

      const altText = usageGif.getAttribute('alt');
      expect(altText).toBeTruthy();
      expect(altText.length).toBeGreaterThan(10);
    });

    test('Usage GIF alt text describes the demonstration', () => {
      const usageGif = document.querySelector('img[src*="usage.gif"]');
      expect(usageGif).toBeTruthy();

      const altText = usageGif.getAttribute('alt').toLowerCase();
      expect(
        altText.includes('usage') ||
        altText.includes('demonstration') ||
        altText.includes('demo') ||
        altText.includes('mirdb')
      ).toBe(true);
    });

    test('Usage GIF alt text is not empty or placeholder', () => {
      const usageGif = document.querySelector('img[src*="usage.gif"]');
      expect(usageGif).toBeTruthy();

      const altText = usageGif.getAttribute('alt');
      expect(altText).not.toBe('');
      expect(altText).not.toBe('image');
      expect(altText).not.toBe('gif');
      expect(altText).not.toBe('usage.gif');
    });
  });

  describe('Usage Demonstration Section Structure', () => {
    test('Usage demo section has semantic HTML structure', () => {
      const usageDemoSection = document.querySelector('#usage-demo');
      expect(usageDemoSection).toBeTruthy();
      expect(usageDemoSection.tagName.toLowerCase()).toBe('section');
    });

    test('Usage demo section has accessible title', () => {
      const usageDemoSection = document.querySelector('#usage-demo');
      expect(usageDemoSection).toBeTruthy();

      const titleId = usageDemoSection.getAttribute('aria-labelledby');
      expect(titleId).toBeTruthy();

      const titleElement = document.getElementById(titleId);
      expect(titleElement).toBeTruthy();
    });

    test('Usage demo section has a heading', () => {
      const usageDemoSection = document.querySelector('#usage-demo');
      expect(usageDemoSection).toBeTruthy();

      const heading = usageDemoSection.querySelector('h1, h2, h3');
      expect(heading).toBeTruthy();
    });

    test('Usage demo section has description text', () => {
      const usageDemoSection = document.querySelector('#usage-demo');
      expect(usageDemoSection).toBeTruthy();

      const description = usageDemoSection.querySelector('.usage-demo-description');
      expect(description).toBeTruthy();
      expect(description.textContent.trim().length).toBeGreaterThan(0);
    });
  });
});
