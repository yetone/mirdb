/**
 * Hero Section Display Tests
 * Scenario: Verify that the hero section displays MirDB's value proposition
 * with tagline, logo, and call-to-action buttons (REQ-1)
 */

const fs = require('fs');
const path = require('path');

describe('Hero Section Display', () => {
  let document;

  beforeAll(() => {
    // Load the index.html file
    const htmlPath = path.resolve(__dirname, '../index.html');
    const html = fs.readFileSync(htmlPath, 'utf8');
    document = new DOMParser().parseFromString(html, 'text/html');
  });

  describe('Test Case 1: Hero section element exists', () => {
    test('should have a hero section element with class or id identifying it as hero', () => {
      // Query for hero section element
      const heroSection = document.querySelector('#hero, .hero, [class*="hero"], section[id*="hero"]');

      expect(heroSection).not.toBeNull();
      expect(heroSection.tagName.toLowerCase()).toBe('section');
    });

    test('hero section should be the first major content section', () => {
      const sections = document.querySelectorAll('main section, body > section');
      const heroSection = document.querySelector('#hero, .hero, [class*="hero"]');

      // Hero should be among the first sections
      if (sections.length > 0) {
        const firstSection = sections[0];
        expect(firstSection).toBe(heroSection);
      } else {
        // If no sections, hero should exist somewhere
        expect(heroSection).not.toBeNull();
      }
    });
  });

  describe('Test Case 2: Tagline content verification', () => {
    test('should contain tagline with "persistent", "key-value", and "memcached"', () => {
      const heroSection = document.querySelector('#hero, .hero, [class*="hero"]');
      expect(heroSection).not.toBeNull();

      // Get all text content in the hero section
      const heroText = heroSection.textContent.toLowerCase();

      // Check for required keywords in tagline
      expect(heroText).toContain('persistent');
      expect(heroText).toContain('key-value');
      expect(heroText).toContain('memcached');
    });

    test('should have an h1 or prominent tagline element', () => {
      const heroSection = document.querySelector('#hero, .hero, [class*="hero"]');
      expect(heroSection).not.toBeNull();

      // Check for h1 or tagline element
      const h1 = heroSection.querySelector('h1');
      const tagline = heroSection.querySelector('.tagline, [class*="tagline"], p.lead, .subtitle');

      // Either h1 or tagline should exist
      expect(h1 !== null || tagline !== null).toBe(true);
    });
  });

  describe('Test Case 3: Primary CTA button verification', () => {
    test('should have a primary CTA with "Get Started" text', () => {
      const heroSection = document.querySelector('#hero, .hero, [class*="hero"]');
      expect(heroSection).not.toBeNull();

      // Query for primary CTA button or link
      const ctaElements = heroSection.querySelectorAll('a, button');
      let primaryCTA = null;

      for (const el of ctaElements) {
        const text = el.textContent.toLowerCase().trim();
        if (text.includes('get started') || text.includes('quick start') || text.includes('start')) {
          primaryCTA = el;
          break;
        }
      }

      expect(primaryCTA).not.toBeNull();
    });

    test('primary CTA should link to quick-start section', () => {
      const heroSection = document.querySelector('#hero, .hero, [class*="hero"]');
      expect(heroSection).not.toBeNull();

      const ctaElements = heroSection.querySelectorAll('a, button');
      let primaryCTA = null;

      for (const el of ctaElements) {
        const text = el.textContent.toLowerCase().trim();
        if (text.includes('get started') || text.includes('quick start') || text.includes('start')) {
          primaryCTA = el;
          break;
        }
      }

      expect(primaryCTA).not.toBeNull();

      // Check if it links to a quick-start section
      if (primaryCTA.tagName.toLowerCase() === 'a') {
        const href = primaryCTA.getAttribute('href');
        expect(href).toBeTruthy();
        // Should link to quick-start section (e.g., #quick-start, #getting-started)
        expect(href).toMatch(/(#quick-start|#getting-started|#start|quick-start)/i);
      }
    });
  });

  describe('Test Case 4: GitHub link verification', () => {
    test('should have a GitHub link in the hero section', () => {
      const heroSection = document.querySelector('#hero, .hero, [class*="hero"]');
      expect(heroSection).not.toBeNull();

      // Query for GitHub link
      const links = heroSection.querySelectorAll('a');
      let githubLink = null;

      for (const link of links) {
        const href = link.getAttribute('href') || '';
        const text = link.textContent.toLowerCase();
        if (href.includes('github.com') || text.includes('github') || text.includes('view on github')) {
          githubLink = link;
          break;
        }
      }

      expect(githubLink).not.toBeNull();
    });

    test('GitHub link should point to a valid repository URL', () => {
      const heroSection = document.querySelector('#hero, .hero, [class*="hero"]');
      expect(heroSection).not.toBeNull();

      const links = heroSection.querySelectorAll('a');
      let githubLink = null;

      for (const link of links) {
        const href = link.getAttribute('href') || '';
        if (href.includes('github.com')) {
          githubLink = link;
          break;
        }
      }

      expect(githubLink).not.toBeNull();

      const href = githubLink.getAttribute('href');
      // Check it's a valid GitHub URL pattern
      expect(href).toMatch(/^https?:\/\/(www\.)?github\.com\/[\w-]+\/[\w-]+/);
    });
  });
});
