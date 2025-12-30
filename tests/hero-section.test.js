/**
 * Tests for Hero Section Display (REQ-1)
 * Verify the hero section displays product name, tagline, and CTA buttons correctly
 */

const fs = require('fs');
const path = require('path');

describe('Hero Section Display', () => {
  let document;

  beforeEach(() => {
    const html = fs.readFileSync(path.resolve(__dirname, '../index.html'), 'utf8');
    document = new DOMParser().parseFromString(html, 'text/html');
  });

  // Test Case 1: h1 element contains 'MirDB' product name
  describe('Test Case 1: Product Name Display', () => {
    test('should have h1 element containing "MirDB" product name', () => {
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.textContent).toContain('MirDB');
    });

    test('should have h1 in the hero section', () => {
      const heroSection = document.querySelector('.hero, #hero, [data-section="hero"], header.hero');
      expect(heroSection).not.toBeNull();
      const h1InHero = heroSection.querySelector('h1');
      expect(h1InHero).not.toBeNull();
      expect(h1InHero.textContent).toContain('MirDB');
    });
  });

  // Test Case 2: Tagline is displayed
  describe('Test Case 2: Tagline Display', () => {
    test('should display tagline about persistent key-value store with memcached protocol', () => {
      const heroSection = document.querySelector('.hero, #hero, [data-section="hero"], header.hero');
      expect(heroSection).not.toBeNull();

      const taglineText = heroSection.textContent.toLowerCase();
      expect(taglineText).toContain('persistent');
      expect(taglineText).toContain('key-value');
      expect(taglineText).toContain('memcached');
    });

    test('should have a tagline element with appropriate styling class', () => {
      const heroSection = document.querySelector('.hero, #hero, [data-section="hero"], header.hero');
      const tagline = heroSection.querySelector('.tagline, .subtitle, .hero-tagline, p');
      expect(tagline).not.toBeNull();
    });
  });

  // Test Case 3: Get Started button presence
  describe('Test Case 3: Primary CTA Button - Get Started', () => {
    test('should have a "Get Started" button', () => {
      const heroSection = document.querySelector('.hero, #hero, [data-section="hero"], header.hero');
      const links = heroSection.querySelectorAll('a');
      const getStartedBtn = Array.from(links).find(link =>
        link.textContent.toLowerCase().includes('get started')
      );
      expect(getStartedBtn).not.toBeNull();
    });

    test('should have appropriate styling class on Get Started button', () => {
      const heroSection = document.querySelector('.hero, #hero, [data-section="hero"], header.hero');
      const links = heroSection.querySelectorAll('a');
      const getStartedBtn = Array.from(links).find(link =>
        link.textContent.toLowerCase().includes('get started')
      );
      expect(getStartedBtn).not.toBeNull();
      const classes = getStartedBtn.className;
      expect(
        classes.includes('btn') ||
        classes.includes('button') ||
        classes.includes('cta') ||
        classes.includes('primary')
      ).toBe(true);
    });

    test('should have href attribute on Get Started button', () => {
      const heroSection = document.querySelector('.hero, #hero, [data-section="hero"], header.hero');
      const links = heroSection.querySelectorAll('a');
      const getStartedBtn = Array.from(links).find(link =>
        link.textContent.toLowerCase().includes('get started')
      );
      expect(getStartedBtn).not.toBeNull();
      expect(getStartedBtn.hasAttribute('href')).toBe(true);
    });
  });

  // Test Case 4: View on GitHub button presence
  describe('Test Case 4: Secondary CTA Button - View on GitHub', () => {
    test('should have a "View on GitHub" button', () => {
      const heroSection = document.querySelector('.hero, #hero, [data-section="hero"], header.hero');
      const links = heroSection.querySelectorAll('a');
      const githubBtn = Array.from(links).find(link =>
        link.textContent.toLowerCase().includes('github')
      );
      expect(githubBtn).not.toBeNull();
    });

    test('should have GitHub button linking to GitHub repository', () => {
      const heroSection = document.querySelector('.hero, #hero, [data-section="hero"], header.hero');
      const links = heroSection.querySelectorAll('a');
      const githubBtn = Array.from(links).find(link =>
        link.textContent.toLowerCase().includes('github')
      );
      expect(githubBtn).not.toBeNull();
      const href = githubBtn.getAttribute('href');
      expect(href).toContain('github.com');
    });
  });

  // Test Case 5: GitHub button security - rel="noopener"
  describe('Test Case 5: GitHub Button Security', () => {
    test('should have rel="noopener" on GitHub link for security', () => {
      const heroSection = document.querySelector('.hero, #hero, [data-section="hero"], header.hero');
      const links = heroSection.querySelectorAll('a');
      const githubBtn = Array.from(links).find(link =>
        link.textContent.toLowerCase().includes('github')
      );
      expect(githubBtn).not.toBeNull();
      const rel = githubBtn.getAttribute('rel');
      expect(rel).toContain('noopener');
    });

    test('should open GitHub link in new tab with target="_blank"', () => {
      const heroSection = document.querySelector('.hero, #hero, [data-section="hero"], header.hero');
      const links = heroSection.querySelectorAll('a');
      const githubBtn = Array.from(links).find(link =>
        link.textContent.toLowerCase().includes('github')
      );
      expect(githubBtn).not.toBeNull();
      expect(githubBtn.getAttribute('target')).toBe('_blank');
    });
  });
});
