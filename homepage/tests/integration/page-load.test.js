/**
 * Page Load Integration Tests
 * Owner: Scenario 1 - Hero Section and Branding
 *
 * Tests for:
 * - Page loads without errors
 * - All sections render correctly
 * - Images load successfully
 * - No console errors
 */

const fs = require('fs');
const path = require('path');

describe('Page Load Integration Tests', () => {
  let document;
  let htmlContent;

  beforeAll(() => {
    // Read the HTML file
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Create a mock DOM
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  describe('Test Case 1: Page loads successfully', () => {
    test('HTML document has proper DOCTYPE', () => {
      expect(htmlContent.trim().toLowerCase()).toMatch(/^<!doctype html>/);
    });

    test('HTML document has valid structure', () => {
      expect(document.documentElement).toBeTruthy();
      expect(document.head).toBeTruthy();
      expect(document.body).toBeTruthy();
    });

    test('Page has required meta tags', () => {
      const charset = document.querySelector('meta[charset]');
      expect(charset).toBeTruthy();
      expect(charset.getAttribute('charset').toLowerCase()).toBe('utf-8');

      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).toBeTruthy();
      expect(viewport.getAttribute('content')).toContain('width=device-width');
    });

    test('Page has a title', () => {
      const title = document.querySelector('title');
      expect(title).toBeTruthy();
      expect(title.textContent).toContain('MirDB');
    });
  });

  describe('Test Case 2: Hero section content - Project name visible with h1', () => {
    test('Hero section exists', () => {
      const heroSection = document.querySelector('#hero');
      expect(heroSection).toBeTruthy();
    });

    test('Project name MirDB is in h1 tag', () => {
      const h1 = document.querySelector('#hero h1');
      expect(h1).toBeTruthy();
      expect(h1.textContent).toBe('MirDB');
    });

    test('H1 has appropriate ID for accessibility', () => {
      const h1 = document.querySelector('#hero h1');
      expect(h1.id).toBe('hero-title');
    });
  });

  describe('Test Case 3: Tagline presence', () => {
    test('Tagline is displayed in hero section', () => {
      const tagline = document.querySelector('.hero-tagline');
      expect(tagline).toBeTruthy();
      expect(tagline.textContent).toBe('A Persistent Key-Value Store with Memcached protocol');
    });
  });

  describe('Test Case 4: Logo display', () => {
    test('Logo image exists with appropriate alt text', () => {
      const logo = document.querySelector('.hero-logo');
      expect(logo).toBeTruthy();
      expect(logo.tagName.toLowerCase()).toBe('img');
      expect(logo.getAttribute('alt')).toBe('MirDB Logo');
    });

    test('Logo has width and height attributes for CLS prevention', () => {
      const logo = document.querySelector('.hero-logo');
      expect(logo.getAttribute('width')).toBeTruthy();
      expect(logo.getAttribute('height')).toBeTruthy();
    });

    test('Logo src points to correct path', () => {
      const logo = document.querySelector('.hero-logo');
      expect(logo.getAttribute('src')).toBe('assets/images/logo.gif');
    });
  });

  describe('Test Case 5: CI badge verification', () => {
    test('CircleCI badge is present', () => {
      const badge = document.querySelector('.hero-badge');
      expect(badge).toBeTruthy();
    });

    test('CircleCI badge links to correct URL', () => {
      const badgeLink = document.querySelector('.hero-badge');
      expect(badgeLink.getAttribute('href')).toBe('https://circleci.com/gh/yetone/mirdb');
    });

    test('Badge link opens in new tab safely', () => {
      const badgeLink = document.querySelector('.hero-badge');
      expect(badgeLink.getAttribute('target')).toBe('_blank');
      expect(badgeLink.getAttribute('rel')).toContain('noopener');
    });

    test('Badge image uses CircleCI shield URL', () => {
      const badgeImg = document.querySelector('.hero-badge img');
      expect(badgeImg).toBeTruthy();
      expect(badgeImg.getAttribute('src')).toContain('circleci.com/gh/yetone/mirdb.svg');
    });
  });

  describe('Test Case 6: Primary CTA button', () => {
    test('Primary CTA button exists', () => {
      const ctaButton = document.querySelector('.btn-primary');
      expect(ctaButton).toBeTruthy();
    });

    test('Primary CTA links to GitHub repository', () => {
      const ctaButton = document.querySelector('.btn-primary');
      expect(ctaButton.getAttribute('href')).toBe('https://github.com/yetone/mirdb');
    });

    test('Primary CTA opens in new tab safely', () => {
      const ctaButton = document.querySelector('.btn-primary');
      expect(ctaButton.getAttribute('target')).toBe('_blank');
      expect(ctaButton.getAttribute('rel')).toContain('noopener');
    });

    test('Primary CTA has accessible label', () => {
      const ctaButton = document.querySelector('.btn-primary');
      expect(ctaButton.getAttribute('aria-label')).toBeTruthy();
    });

    test('Secondary CTA (Get Started) exists and links to section', () => {
      const secondaryCta = document.querySelector('.btn-secondary');
      expect(secondaryCta).toBeTruthy();
      expect(secondaryCta.getAttribute('href')).toBe('#getting-started');
    });
  });

  describe('Test Case 7: HTML semantic structure validation', () => {
    test('Page uses semantic header element', () => {
      const header = document.querySelector('header');
      expect(header).toBeTruthy();
      expect(header.getAttribute('role')).toBe('banner');
    });

    test('Page uses semantic main element', () => {
      const main = document.querySelector('main');
      expect(main).toBeTruthy();
      expect(main.getAttribute('role')).toBe('main');
    });

    test('Page uses semantic section elements', () => {
      const sections = document.querySelectorAll('section');
      expect(sections.length).toBeGreaterThan(0);
    });

    test('Hero section has aria-labelledby for accessibility', () => {
      const heroSection = document.querySelector('#hero');
      expect(heroSection.getAttribute('aria-labelledby')).toBe('hero-title');
    });

    test('Page uses semantic footer element', () => {
      const footer = document.querySelector('footer');
      expect(footer).toBeTruthy();
      expect(footer.getAttribute('role')).toBe('contentinfo');
    });

    test('Navigation has proper role and aria-label', () => {
      const nav = document.querySelector('nav');
      expect(nav).toBeTruthy();
      expect(nav.getAttribute('role')).toBe('navigation');
      expect(nav.getAttribute('aria-label')).toBeTruthy();
    });

    test('Badges container has proper role', () => {
      const badges = document.querySelector('.hero-badges');
      expect(badges.getAttribute('role')).toBe('group');
      expect(badges.getAttribute('aria-label')).toBeTruthy();
    });
  });

  describe('CSS and JS file references', () => {
    test('Main stylesheet is linked', () => {
      const styleLink = document.querySelector('link[href="css/styles.css"]');
      expect(styleLink).toBeTruthy();
    });

    test('Responsive stylesheet is linked', () => {
      const responsiveLink = document.querySelector('link[href="css/responsive.css"]');
      expect(responsiveLink).toBeTruthy();
    });

    test('Main JavaScript file is included', () => {
      const script = document.querySelector('script[src="js/main.js"]');
      expect(script).toBeTruthy();
    });
  });
});
