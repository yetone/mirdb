/**
 * Hero Section Tests
 * Owner: Scenario 1 - Hero Section Display
 *
 * Tests:
 * - MirDB branding present
 * - Tagline text content
 * - Logo image/animation
 * - Primary CTA buttons (View on GitHub, Get Started)
 * - Status badges (Scenario 5 shared)
 */

const { loadHTML, querySection } = require('../helpers/dom-utils');

describe('Hero Section Display', () => {
  beforeEach(() => {
    loadHTML('index.html');
  });

  describe('Test Case 1: Hero section contains h1 with MirDB text', () => {
    test('should have a hero section with h1 containing MirDB', () => {
      const heroSection = querySection('hero');
      expect(heroSection).toBeInTheDocument();

      const h1 = heroSection.querySelector('h1');
      expect(h1).toBeInTheDocument();
      expect(h1.textContent).toContain('MirDB');
    });
  });

  describe('Test Case 2: Tagline contains Persistent Key-Value Store with Memcached Protocol', () => {
    test('should have tagline with correct text', () => {
      const heroSection = querySection('hero');
      expect(heroSection).toBeInTheDocument();

      const tagline = heroSection.querySelector('#tagline');
      expect(tagline).toBeInTheDocument();
      expect(tagline.textContent).toContain('Persistent Key-Value Store with Memcached Protocol');
    });
  });

  describe('Test Case 3: Logo element exists with src pointing to logo.gif', () => {
    test('should have logo image with correct src', () => {
      const heroSection = querySection('hero');
      expect(heroSection).toBeInTheDocument();

      const logo = heroSection.querySelector('#logo');
      expect(logo).toBeInTheDocument();
      expect(logo.tagName.toLowerCase()).toBe('img');
      expect(logo.getAttribute('src')).toMatch(/logo\.gif/);
      expect(logo.getAttribute('alt')).toBeTruthy();
    });
  });

  describe('Test Case 4: Primary CTA buttons exist', () => {
    test('should have View on GitHub button', () => {
      const heroSection = querySection('hero');
      expect(heroSection).toBeInTheDocument();

      const githubCta = heroSection.querySelector('#github-cta');
      expect(githubCta).toBeInTheDocument();
      expect(githubCta.textContent).toContain('View on GitHub');
      expect(githubCta.tagName.toLowerCase()).toBe('a');
    });

    test('should have Get Started button', () => {
      const heroSection = querySection('hero');
      expect(heroSection).toBeInTheDocument();

      const getStartedCta = heroSection.querySelector('#get-started-cta');
      expect(getStartedCta).toBeInTheDocument();
      expect(getStartedCta.textContent).toContain('Get Started');
      expect(getStartedCta.tagName.toLowerCase()).toBe('a');
    });

    test('should have at least two CTA buttons', () => {
      const heroSection = querySection('hero');
      expect(heroSection).toBeInTheDocument();

      const ctaButtons = heroSection.querySelector('#cta-buttons');
      expect(ctaButtons).toBeInTheDocument();

      const buttons = ctaButtons.querySelectorAll('a');
      expect(buttons.length).toBeGreaterThanOrEqual(2);
    });
  });

  describe('Test Case 5: View on GitHub CTA links to correct URL', () => {
    test('should have GitHub link with correct href and target', () => {
      const heroSection = querySection('hero');
      expect(heroSection).toBeInTheDocument();

      const githubCta = heroSection.querySelector('#github-cta');
      expect(githubCta).toBeInTheDocument();
      expect(githubCta.getAttribute('href')).toBe('https://github.com/yetone/mirdb');
      expect(githubCta.getAttribute('target')).toBe('_blank');
      expect(githubCta.getAttribute('rel')).toContain('noopener');
    });
  });

  describe('Additional Hero Section Tests', () => {
    test('hero section should be present in the document', () => {
      const heroSection = querySection('hero');
      expect(heroSection).toBeInTheDocument();
    });

    test('hero section should have proper structure', () => {
      const heroSection = querySection('hero');
      expect(heroSection).toBeInTheDocument();

      // Check for main content container
      const container = heroSection.querySelector('.max-w-4xl');
      expect(container).toBeInTheDocument();
    });

    test('hero section should have description text', () => {
      const heroSection = querySection('hero');
      expect(heroSection).toBeInTheDocument();

      // Check for description mentioning Memcached
      const description = heroSection.textContent;
      expect(description).toContain('Memcached');
      expect(description).toContain('Rust');
    });
  });
});
