/**
 * Unit tests for Hero Section Semantic HTML Structure
 * Test Case ID: 3
 * Validates that the hero section uses proper semantic HTML with correct heading hierarchy
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

describe('Hero Section Semantic HTML Structure', () => {
  let document;

  beforeAll(() => {
    const htmlPath = path.resolve(__dirname, '../../index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
  });

  describe('Heading Hierarchy', () => {
    test('should have exactly one h1 element for the product name', () => {
      const h1Elements = document.querySelectorAll('h1');
      expect(h1Elements.length).toBe(1);
    });

    test('h1 should contain "MirDB" as the product name', () => {
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.textContent.trim()).toBe('MirDB');
    });

    test('h1 should have an id for accessibility anchoring', () => {
      const h1 = document.querySelector('h1');
      expect(h1.getAttribute('id')).toBeTruthy();
    });
  });

  describe('Semantic Structure', () => {
    test('should have a hero section element', () => {
      const heroSection = document.querySelector('.hero');
      expect(heroSection).not.toBeNull();
    });

    test('hero section should have appropriate role or be within main', () => {
      const heroSection = document.querySelector('.hero');
      const main = document.querySelector('main');

      // Hero should either have role="banner" or be within main
      const hasBannerRole = heroSection.getAttribute('role') === 'banner';
      const isWithinMain = main && main.contains(heroSection);

      expect(hasBannerRole || isWithinMain).toBe(true);
    });

    test('hero section should have aria-labelledby pointing to h1', () => {
      const heroSection = document.querySelector('.hero');
      const h1 = document.querySelector('h1');

      if (heroSection.getAttribute('aria-labelledby')) {
        const labelledById = heroSection.getAttribute('aria-labelledby');
        expect(document.getElementById(labelledById)).toBe(h1);
      } else {
        // If no aria-labelledby, the h1 should still be a direct descendant
        expect(heroSection.querySelector('h1')).not.toBeNull();
      }
    });

    test('should have main element wrapping content', () => {
      const main = document.querySelector('main');
      expect(main).not.toBeNull();
    });
  });

  describe('Content Elements', () => {
    test('should have tagline element with descriptive text', () => {
      const tagline = document.querySelector('.hero-tagline');
      expect(tagline).not.toBeNull();
      expect(tagline.textContent.length).toBeGreaterThan(0);
    });

    test('should have description/value proposition element', () => {
      const description = document.querySelector('.hero-description');
      expect(description).not.toBeNull();
      expect(description.textContent.length).toBeGreaterThan(0);
    });

    test('all images should have alt attributes', () => {
      const images = document.querySelectorAll('img');
      images.forEach((img) => {
        expect(img.hasAttribute('alt')).toBe(true);
      });
    });
  });

  describe('Link Accessibility', () => {
    test('all links should have meaningful text or aria-label', () => {
      const links = document.querySelectorAll('a');
      links.forEach((link) => {
        const hasText = link.textContent.trim().length > 0;
        const hasAriaLabel = link.hasAttribute('aria-label');
        expect(hasText || hasAriaLabel).toBe(true);
      });
    });

    test('external links should have appropriate rel attributes', () => {
      const externalLinks = document.querySelectorAll('a[target="_blank"]');
      externalLinks.forEach((link) => {
        const rel = link.getAttribute('rel');
        expect(rel).toContain('noopener');
      });
    });
  });

  describe('Document Structure', () => {
    test('should have proper lang attribute on html element', () => {
      const html = document.documentElement;
      expect(html.getAttribute('lang')).toBe('en');
    });

    test('should have meta viewport for responsive design', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
      expect(viewport.getAttribute('content')).toContain('width=device-width');
    });

    test('should have meta description for SEO', () => {
      const description = document.querySelector('meta[name="description"]');
      expect(description).not.toBeNull();
      expect(description.getAttribute('content').length).toBeGreaterThan(50);
    });
  });
});
