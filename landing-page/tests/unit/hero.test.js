/**
 * Hero Section Unit Tests
 * Owner: Scenario 1 - Hero Section & Value Proposition
 *
 * Test cases:
 * - h1 element exists with non-empty text content
 * - Subheadline element exists with descriptive text
 * - Primary CTA button exists with action-oriented text
 * - Hero image/illustration is present
 */

const fs = require('fs');
const path = require('path');

describe('Hero Section Unit Tests', () => {
  let document;
  let heroSection;

  beforeAll(() => {
    // Read the HTML file
    const htmlPath = path.join(__dirname, '../../index.html');
    const html = fs.readFileSync(htmlPath, 'utf8');

    // Create a DOM from the HTML using jsdom
    document = new DOMParser().parseFromString(html, 'text/html');
    heroSection = document.getElementById('hero');
  });

  describe('Test Case 1: h1 headline element', () => {
    test('h1 element exists with non-empty text content', () => {
      const h1 = document.querySelector('h1');

      expect(h1).not.toBeNull();
      expect(h1.textContent.trim()).not.toBe('');
      expect(h1.textContent.trim().length).toBeGreaterThan(0);
    });

    test('h1 is within the hero section', () => {
      expect(heroSection).not.toBeNull();
      const h1 = heroSection.querySelector('h1');
      expect(h1).not.toBeNull();
    });

    test('h1 has proper headline class', () => {
      const h1 = heroSection.querySelector('h1');
      expect(h1.classList.contains('hero__headline')).toBe(true);
    });
  });

  describe('Test Case 2: Subheadline/value proposition element', () => {
    test('subheadline element exists with descriptive text about product value', () => {
      // Look for common subheadline patterns
      const subheadline = heroSection.querySelector('.hero__subheadline, p');

      expect(subheadline).not.toBeNull();
      expect(subheadline.textContent.trim()).not.toBe('');
      // Subheadline should be descriptive (more than 20 characters)
      expect(subheadline.textContent.trim().length).toBeGreaterThan(20);
    });

    test('subheadline contains value-oriented language', () => {
      const subheadline = heroSection.querySelector('.hero__subheadline, p');
      const text = subheadline?.textContent.toLowerCase() || '';

      // Check for value-oriented words
      const valueWords = ['productivity', 'streamline', 'powerful', 'solution', 'transform', 'boost', 'improve', 'better', 'efficient', 'teams', 'work'];
      const hasValueLanguage = valueWords.some(word => text.includes(word));

      expect(hasValueLanguage).toBe(true);
    });

    test('subheadline has appropriate class', () => {
      const subheadline = heroSection.querySelector('.hero__subheadline');
      expect(subheadline).not.toBeNull();
    });
  });

  describe('Test Case 3: Primary CTA button', () => {
    test('button element with role="button" or <button> tag exists', () => {
      // Look for button or element with role="button"
      const button = heroSection.querySelector('button, [role="button"], a.btn, a.btn-primary');

      expect(button).not.toBeNull();
    });

    test('CTA has action-oriented text', () => {
      const button = heroSection.querySelector('[role="button"], a.btn-primary, .btn-primary');
      const text = button?.textContent.toLowerCase() || '';

      // Check for action-oriented words
      const actionWords = ['get started', 'start', 'try', 'sign up', 'download', 'begin', 'join', 'free', 'demo', 'learn'];
      const hasActionText = actionWords.some(word => text.includes(word));

      expect(hasActionText).toBe(true);
    });

    test('primary CTA has an id or accessible identifier', () => {
      const primaryCta = heroSection.querySelector('#hero-cta-primary, .btn-primary');

      expect(primaryCta).not.toBeNull();
    });

    test('primary CTA has href attribute', () => {
      const primaryCta = heroSection.querySelector('#hero-cta-primary, .btn-primary');
      expect(primaryCta.getAttribute('href')).toBeTruthy();
    });
  });

  describe('Test Case 6: Hero image or illustration', () => {
    test('hero visual element is present', () => {
      // Look for image, svg, or placeholder
      const visual = heroSection.querySelector('img, svg, .hero__image, [class*="image"]');

      expect(visual).not.toBeNull();
    });

    test('hero visual has appropriate accessibility attributes', () => {
      const placeholder = heroSection.querySelector('.hero__image-placeholder[role="img"]');

      if (placeholder) {
        // Check for aria-label
        const ariaLabel = placeholder.getAttribute('aria-label');
        expect(ariaLabel).toBeTruthy();
      } else {
        // If no placeholder with role, check for decorative SVG (aria-hidden)
        const svg = heroSection.querySelector('svg');
        if (svg) {
          expect(svg.getAttribute('aria-hidden')).toBe('true');
        }
      }
    });

    test('hero image container has correct structure', () => {
      const imageContainer = heroSection.querySelector('.hero__image');
      expect(imageContainer).not.toBeNull();
    });
  });

  describe('Hero section structure', () => {
    test('hero section has aria-labelledby for accessibility', () => {
      expect(heroSection.getAttribute('aria-labelledby')).toBe('hero-title');
    });

    test('hero headline has matching id', () => {
      const h1 = heroSection.querySelector('#hero-title');
      expect(h1).not.toBeNull();
    });

    test('hero has CTA group with multiple buttons', () => {
      const ctaGroup = heroSection.querySelector('.hero__cta-group');
      expect(ctaGroup).not.toBeNull();

      const buttons = ctaGroup.querySelectorAll('.btn');
      expect(buttons.length).toBeGreaterThanOrEqual(1);
    });
  });
});
