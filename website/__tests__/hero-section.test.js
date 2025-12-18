/**
 * Hero Section Display Tests
 * Tests for REQ-1 and REQ-4 of MirDB Landing Page
 */

const fs = require('fs');
const path = require('path');

describe('Hero Section Display', () => {
  let document;
  let heroSection;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../src/index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf8');
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
    heroSection = document.querySelector('.hero-section');
  });

  // Test Case 1: Product name 'MirDB' is displayed prominently
  describe('TC-1: Product Name Display', () => {
    test('should display product name "MirDB" prominently', () => {
      const productName = heroSection.querySelector('.product-name, h1');
      expect(productName).not.toBeNull();
      expect(productName.textContent).toContain('MirDB');
    });
  });

  // Test Case 2: Tagline containing 'persistent key-value store' and 'memcached'
  describe('TC-2: Tagline Display', () => {
    test('should display tagline containing "persistent key-value store" and "memcached"', () => {
      const tagline = heroSection.querySelector('.tagline, .hero-tagline');
      expect(tagline).not.toBeNull();
      const taglineText = tagline.textContent.toLowerCase();
      expect(taglineText).toContain('persistent');
      expect(taglineText).toContain('key-value');
      expect(taglineText).toMatch(/memcache/i);
    });
  });

  // Test Case 3: Value proposition text (2-3 sentences) explaining MirDB benefits
  describe('TC-3: Value Proposition Display', () => {
    test('should display value proposition text with 2-3 sentences explaining MirDB benefits', () => {
      const valueProposition = heroSection.querySelector('.value-proposition, .hero-description');
      expect(valueProposition).not.toBeNull();
      const text = valueProposition.textContent.trim();
      // Check that there is substantial content (at least 50 characters)
      expect(text.length).toBeGreaterThan(50);
      // Value proposition should mention key benefits
      const lowerText = text.toLowerCase();
      expect(
        lowerText.includes('persist') ||
        lowerText.includes('data') ||
        lowerText.includes('storage') ||
        lowerText.includes('durable') ||
        lowerText.includes('performance') ||
        lowerText.includes('compatible')
      ).toBe(true);
    });
  });

  // Test Case 4: Primary CTA button with text 'Get Started' or 'View Documentation'
  describe('TC-4: Primary CTA Button', () => {
    test('should display primary CTA button with text "Get Started" or "View Documentation"', () => {
      const primaryCTA = heroSection.querySelector('.cta-primary, .btn-primary, a[href*="doc"]');
      expect(primaryCTA).not.toBeNull();
      const ctaText = primaryCTA.textContent.toLowerCase();
      expect(
        ctaText.includes('get started') ||
        ctaText.includes('documentation') ||
        ctaText.includes('docs')
      ).toBe(true);
    });
  });

  // Test Case 5: Secondary CTA button linking to GitHub repository
  describe('TC-5: Secondary CTA Button (GitHub)', () => {
    test('should display secondary CTA button linking to GitHub repository', () => {
      const githubCTA = heroSection.querySelector('a[href*="github"]');
      expect(githubCTA).not.toBeNull();
      expect(githubCTA.getAttribute('href')).toMatch(/github\.com.*mirdb/i);
      const ctaText = githubCTA.textContent.toLowerCase();
      expect(ctaText.includes('github') || ctaText.includes('source') || ctaText.includes('repository')).toBe(true);
    });
  });

  // Test Case 6: Primary CTA navigation to documentation page (integration)
  describe('TC-6: Primary CTA Navigation', () => {
    test('should have primary CTA that links to documentation page', () => {
      const primaryCTA = heroSection.querySelector('.cta-primary, .btn-primary, a[href*="doc"]');
      expect(primaryCTA).not.toBeNull();
      const href = primaryCTA.getAttribute('href');
      expect(href).toBeTruthy();
      // Should link to documentation (internal or external)
      expect(
        href.includes('doc') ||
        href.includes('#documentation') ||
        href.includes('readme') ||
        href.includes('getting-started')
      ).toBe(true);
    });
  });

  // Test Case 7: GitHub CTA opens in new tab
  describe('TC-7: GitHub CTA New Tab Navigation', () => {
    test('should have GitHub CTA that opens in a new tab', () => {
      const githubCTA = heroSection.querySelector('a[href*="github"]');
      expect(githubCTA).not.toBeNull();
      expect(githubCTA.getAttribute('target')).toBe('_blank');
      // Should also have rel="noopener" for security
      const rel = githubCTA.getAttribute('rel');
      expect(rel).toContain('noopener');
    });
  });
});

describe('Hero Section Accessibility', () => {
  let document;
  let heroSection;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../src/index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf8');
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
    heroSection = document.querySelector('.hero-section');
  });

  test('hero section should be visible and have proper semantic structure', () => {
    expect(heroSection).not.toBeNull();
    // Should have proper heading hierarchy
    const heading = heroSection.querySelector('h1');
    expect(heading).not.toBeNull();
  });

  test('CTA buttons should have appropriate aria labels or text', () => {
    const buttons = heroSection.querySelectorAll('a.cta-primary, a.cta-secondary, a[href*="github"], a[href*="doc"]');
    buttons.forEach(button => {
      const hasText = button.textContent.trim().length > 0;
      const hasAriaLabel = button.getAttribute('aria-label');
      expect(hasText || hasAriaLabel).toBe(true);
    });
  });
});
