/**
 * Usage GIF Display Unit Tests
 * Owner: Scenario 15 - Usage GIF Display
 *
 * Tests for:
 * - Usage GIF element existence with correct src
 * - Accessibility alt text for usage GIF
 */

const { loadHomepageHTML } = require('../setup/test-utils');

describe('Usage GIF Display', () => {
  beforeEach(() => {
    const html = loadHomepageHTML();
    document.body.innerHTML = html;
  });

  describe('TC1: Check for usage GIF element', () => {
    test('Image element with src pointing to assets/usage.gif exists', () => {
      // Find image element with usage.gif in src
      const usageGif = document.querySelector('img[src*="usage.gif"]');
      expect(usageGif).toBeTruthy();
    });

    test('Usage GIF src attribute contains correct path', () => {
      const usageGif = document.querySelector('img[src*="usage.gif"]');
      expect(usageGif).toBeTruthy();

      const src = usageGif.getAttribute('src');
      expect(src).toContain('assets/usage.gif');
    });

    test('Usage GIF has an id for easy targeting', () => {
      const usageGif = document.querySelector('#usage-gif');
      expect(usageGif).toBeTruthy();
      expect(usageGif.tagName.toLowerCase()).toBe('img');
    });

    test('Usage GIF is contained within a section or container', () => {
      const usageGif = document.querySelector('img[src*="usage.gif"]');
      expect(usageGif).toBeTruthy();

      // Check that it has a parent container
      const container = usageGif.closest('.usage-demo, .hero-demo, section');
      expect(container).toBeTruthy();
    });
  });

  describe('TC2: Verify usage GIF has alt text', () => {
    test('Usage GIF has descriptive alt text for accessibility', () => {
      const usageGif = document.querySelector('img[src*="usage.gif"]');
      expect(usageGif).toBeTruthy();

      const altText = usageGif.getAttribute('alt');
      expect(altText).toBeTruthy();
      expect(altText.length).toBeGreaterThan(10); // Should be descriptive
    });

    test('Alt text describes the usage demonstration', () => {
      const usageGif = document.querySelector('img[src*="usage.gif"]');
      expect(usageGif).toBeTruthy();

      const altText = usageGif.getAttribute('alt').toLowerCase();
      // Alt text should mention usage, demonstration, or how MirDB works
      const isDescriptive =
        altText.includes('usage') ||
        altText.includes('demonstration') ||
        altText.includes('demo') ||
        altText.includes('example') ||
        altText.includes('mirdb') ||
        altText.includes('terminal') ||
        altText.includes('command');

      expect(isDescriptive).toBe(true);
    });

    test('Alt text is not just a filename or generic text', () => {
      const usageGif = document.querySelector('img[src*="usage.gif"]');
      expect(usageGif).toBeTruthy();

      const altText = usageGif.getAttribute('alt').toLowerCase();
      // Should not be generic placeholders
      expect(altText).not.toBe('image');
      expect(altText).not.toBe('gif');
      expect(altText).not.toBe('usage.gif');
      expect(altText).not.toBe('usage');
    });

    test('Usage GIF has loading attribute for performance', () => {
      const usageGif = document.querySelector('img[src*="usage.gif"]');
      expect(usageGif).toBeTruthy();

      // Should have loading attribute for lazy loading (optional but recommended)
      const loading = usageGif.getAttribute('loading');
      if (loading) {
        expect(['lazy', 'eager']).toContain(loading);
      }
    });
  });

  describe('Usage GIF placement and structure', () => {
    test('Usage GIF is in a demo or showcase section', () => {
      const usageGif = document.querySelector('img[src*="usage.gif"]');
      expect(usageGif).toBeTruthy();

      // Check for appropriate parent section or container
      const parentSection = usageGif.closest('section, .usage-demo, .hero-demo, .demo-section');
      expect(parentSection).toBeTruthy();
    });

    test('Usage GIF container has appropriate CSS class for styling', () => {
      const usageGif = document.querySelector('img[src*="usage.gif"]');
      expect(usageGif).toBeTruthy();

      // The image or its container should have a class for styling
      const hasClass = usageGif.classList.length > 0 ||
                       usageGif.closest('[class*="usage"], [class*="demo"], [class*="gif"]');
      expect(hasClass).toBeTruthy();
    });
  });
});
