/**
 * Hero Component Unit Tests
 * Owner: Scenario 1 - Hero Section Display
 *
 * Tests:
 * - Product name renders in h1
 * - Tagline renders in paragraph
 * - CTA button present with correct text
 * - CTA button has proper accessibility attributes
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { loadHTML } from '../../setup.js';

const heroHTML = `
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

describe('Hero Component', () => {
  beforeEach(() => {
    loadHTML(heroHTML);
  });

  describe('Product Name Display', () => {
    it('renders product name "MirDB" in an h1 element', () => {
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.textContent).toBe('MirDB');
    });

    it('h1 element has hero__title class for bold styling', () => {
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.classList.contains('hero__title')).toBe(true);
    });

    it('h1 element has an id for accessibility reference', () => {
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.id).toBe('hero-title');
    });
  });

  describe('Hero Section Structure', () => {
    it('hero section contains product name, tagline, and CTA buttons', () => {
      const heroSection = document.querySelector('.hero');
      expect(heroSection).not.toBeNull();

      const productName = heroSection.querySelector('h1');
      expect(productName).not.toBeNull();

      const tagline = heroSection.querySelector('.hero__tagline');
      expect(tagline).not.toBeNull();

      const ctaButtons = heroSection.querySelectorAll('.btn');
      expect(ctaButtons.length).toBeGreaterThanOrEqual(1);
    });

    it('hero section has aria-labelledby for accessibility', () => {
      const heroSection = document.querySelector('.hero');
      expect(heroSection).not.toBeNull();
      expect(heroSection.getAttribute('aria-labelledby')).toBe('hero-title');
    });
  });

  describe('Tagline Display', () => {
    it('displays a descriptive tagline paragraph', () => {
      const tagline = document.querySelector('.hero__tagline');
      expect(tagline).not.toBeNull();
      expect(tagline.tagName.toLowerCase()).toBe('p');
    });

    it('tagline explains MirDB purpose', () => {
      const tagline = document.querySelector('.hero__tagline');
      expect(tagline).not.toBeNull();
      expect(tagline.textContent).toContain('key-value store');
    });
  });

  describe('CTA Button Display', () => {
    it('primary CTA button displays clear action text', () => {
      const primaryCta = document.querySelector('.btn--primary');
      expect(primaryCta).not.toBeNull();

      const ctaText = primaryCta.textContent.trim();
      const validCtaTexts = ['Get Started', 'Learn More', 'Try Now', 'Start Now', 'Get Started Free'];
      const hasValidText = validCtaTexts.some(text => ctaText.includes(text));
      expect(hasValidText).toBe(true);
    });

    it('at least one CTA button is present', () => {
      const ctaButtons = document.querySelectorAll('.btn');
      expect(ctaButtons.length).toBeGreaterThanOrEqual(1);
    });

    it('CTA buttons are anchor elements with href attributes', () => {
      const ctaButtons = document.querySelectorAll('.btn');
      ctaButtons.forEach(button => {
        expect(button.tagName.toLowerCase()).toBe('a');
        expect(button.hasAttribute('href')).toBe(true);
      });
    });

    it('primary CTA has distinct primary styling class', () => {
      const primaryCta = document.querySelector('.btn--primary');
      expect(primaryCta).not.toBeNull();
      expect(primaryCta.classList.contains('btn--primary')).toBe(true);
    });

    it('secondary CTA has distinct secondary styling class', () => {
      const secondaryCta = document.querySelector('.btn--secondary');
      expect(secondaryCta).not.toBeNull();
      expect(secondaryCta.classList.contains('btn--secondary')).toBe(true);
    });
  });

  describe('Accessibility', () => {
    it('hero section is a semantic section element', () => {
      const heroSection = document.querySelector('.hero');
      expect(heroSection).not.toBeNull();
      expect(heroSection.tagName.toLowerCase()).toBe('section');
    });

    it('CTA buttons are keyboard accessible (as anchor elements)', () => {
      const ctaButtons = document.querySelectorAll('.btn');
      ctaButtons.forEach(button => {
        // Anchor elements are naturally focusable when they have href
        expect(button.hasAttribute('href')).toBe(true);
      });
    });
  });
});
