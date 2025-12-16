/**
 * Navigation Menu Tests
 * Scenario: Verify that navigation menu provides easy access to all sections (REQ-6)
 */

const fs = require('fs');
const path = require('path');

describe('Navigation Menu', () => {
  let document;

  beforeAll(() => {
    // Load the index.html file
    const htmlPath = path.resolve(__dirname, '../index.html');
    const html = fs.readFileSync(htmlPath, 'utf8');
    document = new DOMParser().parseFromString(html, 'text/html');
  });

  describe('Test Case 1: Navigation element exists', () => {
    test('should have a navigation element (nav, header nav, or navigation role)', () => {
      // Query for nav element
      const navElement = document.querySelector('nav, header nav, [role="navigation"]');

      expect(navElement).not.toBeNull();
    });

    test('navigation should be within header or be easily accessible', () => {
      const header = document.querySelector('header');
      const nav = document.querySelector('nav');

      // Navigation should exist and ideally be in header
      expect(nav).not.toBeNull();

      if (header) {
        const navInHeader = header.querySelector('nav');
        expect(navInHeader).not.toBeNull();
      }
    });
  });

  describe('Test Case 2: Navigation contains at least 4 links to page sections', () => {
    test('should have at least 4 navigation links', () => {
      const nav = document.querySelector('nav, header nav, [role="navigation"]');
      expect(nav).not.toBeNull();

      // Query for anchor links within navigation
      const navLinks = nav.querySelectorAll('a[href^="#"], a[href*="#"]');

      // Should have at least 4 internal links (features, quick-start, commands, configuration)
      expect(navLinks.length).toBeGreaterThanOrEqual(4);
    });

    test('navigation links should be properly structured (in list or grouped)', () => {
      const nav = document.querySelector('nav, header nav, [role="navigation"]');
      expect(nav).not.toBeNull();

      // Check for nav links in list structure or direct links
      const navList = nav.querySelector('ul, ol');
      const navLinks = nav.querySelectorAll('a');

      // Should have either a list of links or multiple direct links
      expect(navList !== null || navLinks.length >= 4).toBe(true);
    });
  });

  describe('Test Case 3: Link to features section exists', () => {
    test('should have a link pointing to features section', () => {
      const nav = document.querySelector('nav, header nav, [role="navigation"]');
      expect(nav).not.toBeNull();

      // Look for a link with href containing "features"
      const featuresLink = nav.querySelector('a[href="#features"], a[href*="features"]');

      expect(featuresLink).not.toBeNull();
    });

    test('features link should have appropriate text', () => {
      const nav = document.querySelector('nav, header nav, [role="navigation"]');
      expect(nav).not.toBeNull();

      const featuresLink = nav.querySelector('a[href="#features"], a[href*="features"]');
      expect(featuresLink).not.toBeNull();

      const linkText = featuresLink.textContent.toLowerCase();
      expect(linkText).toContain('feature');
    });
  });

  describe('Test Case 4: Link to quick-start section exists', () => {
    test('should have a link pointing to quick-start section', () => {
      const nav = document.querySelector('nav, header nav, [role="navigation"]');
      expect(nav).not.toBeNull();

      // Look for a link with href containing "quick-start" or "start" or "getting-started"
      const quickStartLink = nav.querySelector(
        'a[href="#quick-start"], a[href*="quick-start"], a[href*="getting-started"], a[href*="start"]'
      );

      expect(quickStartLink).not.toBeNull();
    });

    test('quick-start link should have appropriate text', () => {
      const nav = document.querySelector('nav, header nav, [role="navigation"]');
      expect(nav).not.toBeNull();

      const quickStartLink = nav.querySelector(
        'a[href="#quick-start"], a[href*="quick-start"], a[href*="getting-started"], a[href*="start"]'
      );
      expect(quickStartLink).not.toBeNull();

      const linkText = quickStartLink.textContent.toLowerCase();
      expect(linkText.includes('start') || linkText.includes('quick') || linkText.includes('getting')).toBe(true);
    });
  });

  describe('Test Case 5: Link to commands section exists', () => {
    test('should have a link pointing to commands section', () => {
      const nav = document.querySelector('nav, header nav, [role="navigation"]');
      expect(nav).not.toBeNull();

      // Look for a link with href containing "commands" or "reference"
      const commandsLink = nav.querySelector(
        'a[href="#commands"], a[href*="commands"], a[href*="reference"]'
      );

      expect(commandsLink).not.toBeNull();
    });

    test('commands link should have appropriate text', () => {
      const nav = document.querySelector('nav, header nav, [role="navigation"]');
      expect(nav).not.toBeNull();

      const commandsLink = nav.querySelector(
        'a[href="#commands"], a[href*="commands"], a[href*="reference"]'
      );
      expect(commandsLink).not.toBeNull();

      const linkText = commandsLink.textContent.toLowerCase();
      expect(linkText.includes('command') || linkText.includes('reference')).toBe(true);
    });
  });

  describe('Test Case 6: Anchor targets exist on page', () => {
    test('all navigation href targets should have corresponding id elements', () => {
      const nav = document.querySelector('nav, header nav, [role="navigation"]');
      expect(nav).not.toBeNull();

      // Get all internal links (starting with #)
      const internalLinks = nav.querySelectorAll('a[href^="#"]');
      expect(internalLinks.length).toBeGreaterThan(0);

      // Check each internal link has a corresponding element
      const missingTargets = [];

      internalLinks.forEach((link) => {
        const href = link.getAttribute('href');
        if (href && href.startsWith('#') && href.length > 1) {
          const targetId = href.substring(1);
          const targetElement = document.getElementById(targetId);

          if (!targetElement) {
            missingTargets.push(targetId);
          }
        }
      });

      // All targets should exist
      expect(missingTargets).toEqual([]);
    });

    test('key section targets (features, quick-start, commands, configuration) should exist', () => {
      // These are the expected section IDs based on navigation
      const expectedSections = ['features', 'quick-start', 'commands', 'configuration'];

      expectedSections.forEach((sectionId) => {
        const section = document.getElementById(sectionId);
        expect(section).not.toBeNull();
      });
    });

    test('sections should be proper section elements for semantic HTML', () => {
      const expectedSections = ['features', 'quick-start', 'commands', 'configuration'];

      expectedSections.forEach((sectionId) => {
        const element = document.getElementById(sectionId);
        expect(element).not.toBeNull();
        expect(element.tagName.toLowerCase()).toBe('section');
      });
    });
  });
});
