/**
 * Tests for Hero Section Display and Content
 * Verifies the hero section displays correctly with all required elements
 * including headline, tagline, description, and call-to-action buttons
 */

const fs = require('fs');
const path = require('path');

describe('Hero Section Display and Content', () => {
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

  describe('Test Case 1: Hero section visible with headline', () => {
    test('Hero section should be visible with headline containing MirDB and Persistent Key-Value Store', () => {
      // Check for hero section with id
      const heroSection = document.querySelector('#hero');
      expect(heroSection).not.toBeNull();
      expect(heroSection.classList.contains('hero')).toBe(true);

      // Verify headline contains required text
      const h1 = heroSection.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.textContent).toContain('MirDB');
      expect(h1.textContent).toContain('Persistent Key-Value Store');
    });
  });

  describe('Test Case 2: H1 tag present with correct headline text', () => {
    test('H1 tag should be present with correct headline text', () => {
      // Verify H1 tag exists in the document
      const h1Elements = document.querySelectorAll('h1');
      expect(h1Elements.length).toBe(1);

      // Verify exact headline text matches requirement
      const h1 = h1Elements[0];
      expect(h1.textContent).toBe('MirDB: Persistent Key-Value Store with Memcached Protocol');
    });
  });

  describe('Test Case 3: Primary CTA button - Get Started', () => {
    test('Primary CTA button with text Get Started or View Documentation should be clickable', () => {
      // Find the primary CTA button by id
      const primaryCta = document.querySelector('#cta-primary');
      expect(primaryCta).not.toBeNull();

      // Verify it's a link element
      expect(primaryCta.tagName.toLowerCase()).toBe('a');

      // Verify button text contains "Get Started" or "View Documentation"
      const buttonText = primaryCta.textContent.trim();
      const hasValidText = buttonText.includes('Get Started') || buttonText.includes('View Documentation');
      expect(hasValidText).toBe(true);

      // Verify button has href attribute (is clickable)
      expect(primaryCta.hasAttribute('href')).toBe(true);
      expect(primaryCta.getAttribute('href').length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 4: Secondary CTA button - View on GitHub', () => {
    test('Secondary CTA button/link with text View on GitHub should link to correct URL', () => {
      // Find the GitHub CTA button by id
      const githubCta = document.querySelector('#cta-github');
      expect(githubCta).not.toBeNull();

      // Verify button text
      expect(githubCta.textContent.trim()).toBe('View on GitHub');

      // Verify correct GitHub link
      expect(githubCta.getAttribute('href')).toBe('https://github.com/yetone/mirdb');
    });
  });

  describe('Additional Hero Section Validations', () => {
    test('Hero section should display logo', () => {
      const logo = document.querySelector('.hero-logo');
      expect(logo).not.toBeNull();
      // Accept both optimized SVG and original GIF formats
      expect(logo.getAttribute('src')).toMatch(/assets\/logo\.(gif|svg|png|webp)/);
      expect(logo.getAttribute('alt')).toContain('MirDB');
    });

    test('Hero section should have tagline with painless messaging', () => {
      const heroSection = document.querySelector('#hero');
      expect(heroSection).not.toBeNull();

      // Check for the tagline paragraph
      const tagline = heroSection.querySelector('.hero-tagline');
      expect(tagline).not.toBeNull();
      expect(tagline.textContent.toLowerCase()).toContain('painless');
    });

    test('CTA buttons should be properly styled with btn classes', () => {
      const primaryCta = document.querySelector('#cta-primary');
      const githubCta = document.querySelector('#cta-github');

      expect(primaryCta.classList.contains('btn')).toBe(true);
      expect(primaryCta.classList.contains('btn-primary')).toBe(true);

      expect(githubCta.classList.contains('btn')).toBe(true);
      expect(githubCta.classList.contains('btn-secondary')).toBe(true);
    });

    test('Hero section should be within the first visible area', () => {
      const bodyContent = document.body.innerHTML;
      const heroIndex = bodyContent.indexOf('id="hero"');
      const mainIndex = bodyContent.indexOf('<main');
      const footerIndex = bodyContent.indexOf('<footer');

      // Hero should appear before footer
      if (footerIndex > -1) {
        expect(heroIndex).toBeLessThan(footerIndex);
      }
    });
  });
});
