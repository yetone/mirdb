/**
 * Navigation Unit Tests
 * Owner: Scenario 9 - Navigation and Internal Links
 *
 * Unit tests for validating internal link integrity
 * using JSDOM for DOM parsing.
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

// Load HTML content
const htmlPath = path.join(__dirname, '../../index.html');
const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

describe('Navigation and Internal Links Unit Tests', () => {
  let dom;
  let document;

  beforeAll(() => {
    dom = new JSDOM(htmlContent);
    document = dom.window.document;
  });

  describe('Internal Link Validation', () => {
    test('all internal anchor links point to existing section IDs', () => {
      // Test case 4: Check all internal links have valid hrefs
      // Expected: All anchor links point to existing section IDs

      // Get all anchor links that start with #
      const internalLinks = document.querySelectorAll('a[href^="#"]');

      expect(internalLinks.length).toBeGreaterThan(0);

      // Check each internal link
      internalLinks.forEach(link => {
        const href = link.getAttribute('href');
        const targetId = href.substring(1); // Remove the # prefix

        // Find target element
        const targetElement = document.getElementById(targetId);

        expect(targetElement).not.toBeNull();
        expect(targetElement.id).toBe(targetId);
      });
    });

    test('Get Started button links to quickstart section', () => {
      // Find the Get Started button
      const getStartedLink = document.querySelector('a[href="#quickstart"]');

      expect(getStartedLink).not.toBeNull();
      expect(getStartedLink.textContent.toLowerCase()).toContain('get started');

      // Verify quickstart section exists
      const quickstartSection = document.getElementById('quickstart');
      expect(quickstartSection).not.toBeNull();
    });

    test('required section IDs exist', () => {
      // Verify all required sections exist
      const requiredSections = ['features', 'demo', 'quickstart'];

      requiredSections.forEach(sectionId => {
        const section = document.getElementById(sectionId);
        expect(section).not.toBeNull();
      });
    });

    test('internal links have proper anchor format', () => {
      const internalLinks = document.querySelectorAll('a[href^="#"]');

      internalLinks.forEach(link => {
        const href = link.getAttribute('href');

        // Href should start with # followed by valid ID characters
        expect(href).toMatch(/^#[a-zA-Z][a-zA-Z0-9_-]*$/);
      });
    });

    test('no duplicate IDs in document', () => {
      const allElements = document.querySelectorAll('[id]');
      const ids = Array.from(allElements).map(el => el.id);
      const uniqueIds = new Set(ids);

      expect(ids.length).toBe(uniqueIds.size);
    });

    test('section IDs are lowercase and valid', () => {
      const sections = ['features', 'demo', 'quickstart'];

      sections.forEach(sectionId => {
        // ID should be lowercase
        expect(sectionId).toBe(sectionId.toLowerCase());

        // Verify element exists
        const element = document.getElementById(sectionId);
        expect(element).not.toBeNull();
      });
    });
  });

  describe('Hero Section Navigation Elements', () => {
    test('hero section contains CTA buttons', () => {
      const heroSection = document.querySelector('header.hero');
      expect(heroSection).not.toBeNull();

      const ctaButtons = heroSection.querySelectorAll('.cta-buttons a');
      expect(ctaButtons.length).toBeGreaterThanOrEqual(2);
    });

    test('Get Started button is in CTA buttons', () => {
      const ctaButtons = document.querySelector('.cta-buttons');
      expect(ctaButtons).not.toBeNull();

      const getStartedBtn = ctaButtons.querySelector('a[href="#quickstart"]');
      expect(getStartedBtn).not.toBeNull();
    });

    test('internal navigation button has correct class', () => {
      const getStartedBtn = document.querySelector('a[href="#quickstart"]');
      expect(getStartedBtn).not.toBeNull();
      expect(getStartedBtn.classList.contains('btn')).toBe(true);
    });
  });
});
