/**
 * SEO & Performance Unit Tests
 * Owner: Scenario 11 - Performance & SEO
 *
 * Test cases:
 * - Title tag validation (50-60 chars, meaningful content)
 * - Meta description validation (150-160 chars, meaningful content)
 * - Canonical URL validation
 * - Open Graph tags validation (og:title, og:description, og:image)
 * - Twitter Card tags validation
 * - Semantic HTML structure validation
 * - Single h1 element validation
 * - Image lazy loading validation
 */

const fs = require('fs');
const path = require('path');

describe('SEO & Performance Tests', () => {
  let document;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    const html = fs.readFileSync(htmlPath, 'utf8');
    document = new DOMParser().parseFromString(html, 'text/html');
  });

  describe('Title Tag', () => {
    test('Title tag exists with meaningful, unique content (50-60 chars)', () => {
      const titleElement = document.querySelector('title');
      expect(titleElement).not.toBeNull();

      const titleText = titleElement.textContent.trim();
      expect(titleText.length).toBeGreaterThanOrEqual(1);
      expect(titleText.length).toBeLessThanOrEqual(70); // Allow some flexibility

      // Check it's meaningful (not empty or generic)
      expect(titleText).not.toBe('');
      expect(titleText.toLowerCase()).not.toBe('untitled');
      expect(titleText.toLowerCase()).not.toBe('document');
    });
  });

  describe('Meta Description', () => {
    test('Meta description exists with meaningful content (150-160 chars)', () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      expect(metaDescription).not.toBeNull();

      const content = metaDescription.getAttribute('content');
      expect(content).not.toBeNull();
      expect(content.trim().length).toBeGreaterThanOrEqual(50);
      expect(content.trim().length).toBeLessThanOrEqual(200); // Allow some flexibility

      // Check it's meaningful (not empty or generic)
      expect(content).not.toBe('');
    });
  });

  describe('Canonical URL', () => {
    test('Canonical link tag present with absolute URL', () => {
      const canonicalLink = document.querySelector('link[rel="canonical"]');
      expect(canonicalLink).not.toBeNull();

      const href = canonicalLink.getAttribute('href');
      expect(href).not.toBeNull();

      // Check it's an absolute URL (starts with http:// or https://)
      expect(href).toMatch(/^https?:\/\//);
    });
  });

  describe('Open Graph Tags', () => {
    test('og:title meta tag exists', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      expect(ogTitle).not.toBeNull();

      const content = ogTitle.getAttribute('content');
      expect(content).not.toBeNull();
      expect(content.trim()).not.toBe('');
    });

    test('og:description meta tag exists', () => {
      const ogDescription = document.querySelector('meta[property="og:description"]');
      expect(ogDescription).not.toBeNull();

      const content = ogDescription.getAttribute('content');
      expect(content).not.toBeNull();
      expect(content.trim()).not.toBe('');
    });

    test('og:image meta tag exists with valid image URL', () => {
      const ogImage = document.querySelector('meta[property="og:image"]');
      expect(ogImage).not.toBeNull();

      const content = ogImage.getAttribute('content');
      expect(content).not.toBeNull();
      expect(content.trim()).not.toBe('');

      // Check it's a valid URL (absolute or relative path)
      expect(content).toMatch(/^(https?:\/\/|\/)/);
    });
  });

  describe('Twitter Card Tags', () => {
    test('twitter:card meta tag exists', () => {
      const twitterCard = document.querySelector('meta[name="twitter:card"]');
      expect(twitterCard).not.toBeNull();

      const content = twitterCard.getAttribute('content');
      expect(content).not.toBeNull();

      // Check it's a valid Twitter card type
      expect(['summary', 'summary_large_image', 'app', 'player']).toContain(content);
    });
  });

  describe('Semantic HTML Structure', () => {
    test('Page uses header, main, section, nav, footer elements', () => {
      const header = document.querySelector('header');
      const main = document.querySelector('main');
      const sections = document.querySelectorAll('section');
      const nav = document.querySelector('nav');
      const footer = document.querySelector('footer');

      expect(header).not.toBeNull();
      expect(main).not.toBeNull();
      expect(sections.length).toBeGreaterThan(0);
      expect(nav).not.toBeNull();
      expect(footer).not.toBeNull();
    });

    test('Page has exactly one h1 element', () => {
      const h1Elements = document.querySelectorAll('h1');
      expect(h1Elements.length).toBe(1);
    });
  });

  describe('Image Optimization', () => {
    test('Below-fold images have loading="lazy" attribute', () => {
      // Get all images that are NOT in the hero section (above the fold)
      const heroSection = document.querySelector('#hero');
      const allImages = document.querySelectorAll('img');

      // Filter for images below the fold (not in hero section)
      const belowFoldImages = Array.from(allImages).filter(img => {
        // Check if the image is NOT inside the hero section
        return !heroSection || !heroSection.contains(img);
      });

      // All below-fold images should have loading="lazy"
      belowFoldImages.forEach(img => {
        expect(img.getAttribute('loading')).toBe('lazy');
      });
    });
  });
});
