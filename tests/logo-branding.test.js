/**
 * Tests for Logo and Branding Assets
 * Scenario: Verify the logo and branding assets are displayed correctly
 *
 * Test Cases:
 * 1. Check for logo image - Logo image present and loaded successfully
 * 2. Verify logo has alt text - Logo img has alt attribute with 'MirDB' or similar
 * 3. Check for animated usage demo - Animated GIF or terminal demo present showing usage
 */

const fs = require('fs');
const path = require('path');

describe('Logo and Branding Assets', () => {
  let htmlContent;
  let document;

  beforeAll(() => {
    // Read the HTML file
    const htmlPath = path.join(__dirname, '..', 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');

    // Parse HTML using jsdom
    const { JSDOM } = require('jsdom');
    const dom = new JSDOM(htmlContent);
    document = dom.window.document;
  });

  // Test Case 1: Check for logo image
  describe('Test Case 1: Logo Image Present', () => {
    test('Logo image should be present in the document', () => {
      // Check for logo image in hero section or header/nav
      const heroLogo = document.querySelector('.hero-logo');
      const navLogo = document.querySelector('.logo img, .logo-img, nav img');

      // At least one logo should be present
      const logoImage = heroLogo || navLogo;
      expect(logoImage).not.toBeNull();
    });

    test('Logo image should have valid src attribute pointing to logo.gif', () => {
      const heroLogo = document.querySelector('.hero-logo');
      const navLogo = document.querySelector('.logo img, .logo-img');
      const logoImage = heroLogo || navLogo;

      expect(logoImage).not.toBeNull();
      const src = logoImage.getAttribute('src');
      expect(src).toBeTruthy();
      expect(src).toContain('logo.gif');
    });

    test('Logo asset file should exist on disk', () => {
      const logoPath = path.join(__dirname, '..', 'assets', 'logo.gif');
      expect(fs.existsSync(logoPath)).toBe(true);
    });

    test('Logo file should have valid content (non-empty)', () => {
      const logoPath = path.join(__dirname, '..', 'assets', 'logo.gif');
      const stats = fs.statSync(logoPath);
      expect(stats.size).toBeGreaterThan(0);
    });
  });

  // Test Case 2: Verify logo has alt text
  describe('Test Case 2: Logo Alt Text', () => {
    test('Logo img should have alt attribute', () => {
      const heroLogo = document.querySelector('.hero-logo');
      const navLogo = document.querySelector('.logo img, .logo-img');
      const logoImage = heroLogo || navLogo;

      expect(logoImage).not.toBeNull();
      expect(logoImage.hasAttribute('alt')).toBe(true);
    });

    test('Logo alt text should contain MirDB or similar descriptive text', () => {
      const heroLogo = document.querySelector('.hero-logo');
      const navLogo = document.querySelector('.logo img, .logo-img');
      const logoImage = heroLogo || navLogo;

      expect(logoImage).not.toBeNull();
      const altText = logoImage.getAttribute('alt');
      expect(altText).toBeTruthy();
      expect(altText.toLowerCase()).toMatch(/mirdb|logo/i);
    });

    test('Logo alt text should be descriptive (not empty or generic)', () => {
      const heroLogo = document.querySelector('.hero-logo');
      const navLogo = document.querySelector('.logo img, .logo-img');
      const logoImage = heroLogo || navLogo;

      expect(logoImage).not.toBeNull();
      const altText = logoImage.getAttribute('alt');
      expect(altText).toBeTruthy();
      expect(altText.length).toBeGreaterThan(3); // More than just "img" or similar
    });
  });

  // Test Case 3: Check for animated usage demo
  describe('Test Case 3: Animated Usage Demo', () => {
    test('Usage demo GIF should be present in the document', () => {
      // Check for usage.gif image anywhere in the document
      const usageImage = document.querySelector('img[src*="usage.gif"]');
      expect(usageImage).not.toBeNull();
    });

    test('Usage demo GIF should have valid src attribute', () => {
      const usageImage = document.querySelector('img[src*="usage.gif"]');
      expect(usageImage).not.toBeNull();

      const src = usageImage.getAttribute('src');
      expect(src).toBeTruthy();
      expect(src).toContain('usage.gif');
    });

    test('Usage demo asset file should exist on disk', () => {
      const usagePath = path.join(__dirname, '..', 'assets', 'usage.gif');
      expect(fs.existsSync(usagePath)).toBe(true);
    });

    test('Usage demo GIF should have alt text for accessibility', () => {
      const usageImage = document.querySelector('img[src*="usage.gif"]');
      expect(usageImage).not.toBeNull();

      const altText = usageImage.getAttribute('alt');
      expect(altText).toBeTruthy();
      expect(altText.length).toBeGreaterThan(0);
    });

    test('Usage demo should be in an appropriate section (code example or usage section)', () => {
      const usageImage = document.querySelector('img[src*="usage.gif"]');
      expect(usageImage).not.toBeNull();

      // Check if usage GIF is within a code-example section or similar context
      const codeExampleSection = usageImage.closest('#code-example, .code-example, .usage-demo, #usage');
      expect(codeExampleSection).not.toBeNull();
    });
  });

  // Additional branding consistency tests
  describe('Branding Consistency', () => {
    test('Logo should be prominently positioned (hero or header area)', () => {
      const heroLogo = document.querySelector('.hero .hero-logo, .hero-logo');
      const navLogo = document.querySelector('nav .logo img, header .logo img');

      // Logo should be in hero or navigation
      expect(heroLogo || navLogo).not.toBeNull();
    });

    test('Brand name MirDB should appear in the page title', () => {
      const title = document.querySelector('title');
      expect(title).not.toBeNull();
      expect(title.textContent).toContain('MirDB');
    });

    test('Open Graph image should reference the logo if present', () => {
      const ogImage = document.querySelector('meta[property="og:image"]');
      // OG image is optional but if present should reference logo
      if (ogImage) {
        expect(ogImage.getAttribute('content')).toContain('logo');
      } else {
        // If no OG image, just verify the page has the logo displayed prominently
        const heroLogo = document.querySelector('.hero-logo');
        expect(heroLogo).not.toBeNull();
      }
    });
  });
});
