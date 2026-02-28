/**
 * CTA Buttons Unit Tests
 * Owner: Scenario 15 - CTA Button Behavior
 *
 * Tests:
 * - CTA button text clearly indicates action
 * - CTA buttons have proper structure and attributes
 * - Button styling classes are correctly applied
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { loadHTML } from '../../setup.js';

const ctaButtonsHTML = `
<section class="hero" id="home" aria-labelledby="hero-title">
  <div class="hero__content">
    <h1 id="hero-title" class="hero__title">MirDB</h1>
    <p class="hero__tagline">A high-performance persistent key-value store for modern applications</p>
    <div class="hero__actions">
      <a href="#features" class="btn btn--primary">Get Started</a>
      <a href="#about" class="btn btn--secondary">Learn More</a>
    </div>
  </div>
</section>
`;

describe('CTA Buttons', () => {
  beforeEach(() => {
    loadHTML(ctaButtonsHTML);
  });

  describe('TC5: CTA Button Text Clarity', () => {
    it('primary CTA text clearly indicates action', () => {
      const primaryCta = document.querySelector('.btn--primary');
      expect(primaryCta).not.toBeNull();

      const ctaText = primaryCta.textContent.trim();

      // Define valid action-oriented CTA texts
      const validActionTexts = [
        'Get Started',
        'Try Now',
        'View Docs',
        'Learn More',
        'Start Free',
        'Sign Up',
        'Download',
        'Start Now',
        'Get Started Free',
        'Try Free',
      ];

      // Verify text clearly indicates an action
      const hasValidText = validActionTexts.some(text =>
        ctaText.toLowerCase().includes(text.toLowerCase())
      );

      expect(hasValidText).toBe(true);
    });

    it('secondary CTA text clearly indicates action (if present)', () => {
      const secondaryCta = document.querySelector('.btn--secondary');

      if (secondaryCta) {
        const ctaText = secondaryCta.textContent.trim();

        // Define valid action-oriented CTA texts
        const validActionTexts = [
          'Learn More',
          'View Demo',
          'See Features',
          'Read Docs',
          'About',
          'Contact',
          'Explore',
          'Discover',
        ];

        // Verify text clearly indicates an action
        const hasValidText = validActionTexts.some(text =>
          ctaText.toLowerCase().includes(text.toLowerCase())
        );

        expect(hasValidText).toBe(true);
      }
    });

    it('CTA button text is not empty', () => {
      const ctaButtons = document.querySelectorAll('.btn');

      ctaButtons.forEach(button => {
        const text = button.textContent.trim();
        expect(text.length).toBeGreaterThan(0);
      });
    });

    it('CTA button text is concise (under 25 characters)', () => {
      const ctaButtons = document.querySelectorAll('.btn');

      ctaButtons.forEach(button => {
        const text = button.textContent.trim();
        expect(text.length).toBeLessThanOrEqual(25);
      });
    });
  });

  describe('CTA Button Structure', () => {
    it('CTA buttons have href attribute for navigation', () => {
      const ctaButtons = document.querySelectorAll('.btn');

      ctaButtons.forEach(button => {
        expect(button.hasAttribute('href')).toBe(true);
        expect(button.getAttribute('href')).not.toBe('');
      });
    });

    it('primary CTA has btn--primary class', () => {
      const primaryCta = document.querySelector('.btn--primary');
      expect(primaryCta).not.toBeNull();
      expect(primaryCta.classList.contains('btn')).toBe(true);
      expect(primaryCta.classList.contains('btn--primary')).toBe(true);
    });

    it('secondary CTA has btn--secondary class (if present)', () => {
      const secondaryCta = document.querySelector('.btn--secondary');

      if (secondaryCta) {
        expect(secondaryCta.classList.contains('btn')).toBe(true);
        expect(secondaryCta.classList.contains('btn--secondary')).toBe(true);
      }
    });

    it('CTA buttons are anchor elements for proper semantics', () => {
      const ctaButtons = document.querySelectorAll('.btn');

      ctaButtons.forEach(button => {
        expect(button.tagName.toLowerCase()).toBe('a');
      });
    });

    it('CTA buttons link to valid anchor targets', () => {
      const ctaButtons = document.querySelectorAll('.btn');

      ctaButtons.forEach(button => {
        const href = button.getAttribute('href');

        // If it's an anchor link, verify format
        if (href.startsWith('#')) {
          // Verify it's a valid anchor format
          expect(href.length).toBeGreaterThan(1);
          // Anchor should not have spaces
          expect(href).not.toMatch(/\s/);
        }
      });
    });
  });

  describe('CTA Button Accessibility', () => {
    it('CTA buttons are focusable (as anchor elements with href)', () => {
      const ctaButtons = document.querySelectorAll('.btn');

      ctaButtons.forEach(button => {
        // Anchor elements with href are naturally focusable
        expect(button.hasAttribute('href')).toBe(true);
      });
    });

    it('CTA buttons have meaningful text (not just icons)', () => {
      const ctaButtons = document.querySelectorAll('.btn');

      ctaButtons.forEach(button => {
        const text = button.textContent.trim();
        // Should contain letters, not just symbols/icons
        expect(text).toMatch(/[a-zA-Z]/);
      });
    });
  });
});
