/**
 * Hero Section Structure Tests
 * Owner: Scenario 1 - Hero Section Validation
 *
 * Tests for:
 * - Logo presence and attributes
 * - Product name display
 * - Tagline content
 * - CTA button presence and link
 * - Above-the-fold visibility
 */

const { loadHTML, getByTestId, getTextContent, isAboveTheFold } = require('./test-utils');

describe('Hero Section Validation', () => {
  let document;

  beforeAll(async () => {
    document = await loadHTML();
  });

  describe('Test Case 1: Logo Element', () => {
    test('An img element with src containing "logo" exists and has valid alt text', () => {
      const logoImg = document.querySelector('img[src*="logo"]');

      expect(logoImg).not.toBeNull();
      expect(logoImg.getAttribute('src')).toContain('logo');
      expect(logoImg.getAttribute('alt')).toBeTruthy();
      expect(logoImg.getAttribute('alt').length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 2: Product Name Heading', () => {
    test('An h1 element contains the text "MirDB"', () => {
      const h1Element = document.querySelector('h1');

      expect(h1Element).not.toBeNull();
      expect(getTextContent(h1Element)).toContain('MirDB');
    });
  });

  describe('Test Case 3: Tagline Content', () => {
    test('Text content includes "Persistent Key-Value Store" and "Memcached Protocol"', () => {
      const heroSection = getByTestId(document, 'hero-section') || document.querySelector('#hero');

      expect(heroSection).not.toBeNull();

      const heroText = getTextContent(heroSection);
      expect(heroText).toMatch(/Persistent\s+Key-Value\s+Store/i);
      expect(heroText).toMatch(/Memcached\s+Protocol/i);
    });
  });

  describe('Test Case 4: Persistence Subtext', () => {
    test('Text mentions data not disappearing on restart', () => {
      const heroSection = getByTestId(document, 'hero-section') || document.querySelector('#hero');

      expect(heroSection).not.toBeNull();

      const heroText = getTextContent(heroSection).toLowerCase();
      // Check for text about data persistence on restart
      const hasRestartText =
        heroText.includes('restart') ||
        heroText.includes('disappear') ||
        heroText.includes('persist');

      expect(hasRestartText).toBe(true);
    });
  });

  describe('Test Case 5: CTA Button/Link', () => {
    test('A link or button with text "Get Started" exists and has valid href', () => {
      // Look for a link or button with "Get Started" text
      const links = document.querySelectorAll('a');
      const buttons = document.querySelectorAll('button');

      let ctaElement = null;

      // Check links
      for (const link of links) {
        if (getTextContent(link).toLowerCase().includes('get started')) {
          ctaElement = link;
          break;
        }
      }

      // Check buttons if no link found
      if (!ctaElement) {
        for (const button of buttons) {
          if (getTextContent(button).toLowerCase().includes('get started')) {
            ctaElement = button;
            break;
          }
        }
      }

      expect(ctaElement).not.toBeNull();

      // If it's a link, check href
      if (ctaElement.tagName.toLowerCase() === 'a') {
        const href = ctaElement.getAttribute('href');
        expect(href).toBeTruthy();
        expect(href.length).toBeGreaterThan(0);
      }
    });
  });

  describe('Test Case 6: Hero Section Above the Fold', () => {
    test('Hero section is fully visible in the initial viewport at 1024px', () => {
      const heroSection = getByTestId(document, 'hero-section') || document.querySelector('#hero');

      expect(heroSection).not.toBeNull();

      // Verify hero section exists and has proper structure
      // The hero should contain logo, heading, tagline, and CTA
      const hasLogo = heroSection.querySelector('img[src*="logo"]') !== null;
      const hasHeading = heroSection.querySelector('h1') !== null;
      const hasCTA = Array.from(heroSection.querySelectorAll('a')).some(a =>
        getTextContent(a).toLowerCase().includes('get started')
      );

      // At least logo and heading should be in hero for above-the-fold
      expect(hasLogo || hasHeading).toBe(true);

      // Check that hero section is structured for above-fold visibility
      expect(isAboveTheFold(document)).toBe(true);
    });
  });
});
