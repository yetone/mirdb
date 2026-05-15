/**
 * Hero section tests for MirDB homepage.
 * Owner: Scenario 1 - Hero Section
 *
 * Test framework: Vitest + jsdom
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'node:fs';
import path from 'node:path';

function loadDOM() {
  const html = fs.readFileSync(
    path.resolve(__dirname, '../index.html'),
    'utf-8'
  );
  const dom = new JSDOM(html, { url: 'http://localhost:8080' });
  return dom;
}

describe('Hero Section', () => {
  let dom;
  let document;
  let window;

  beforeEach(() => {
    dom = loadDOM();
    document = dom.window.document;
    window = dom.window;
  });

  describe('Test Case 1: Hero section visibility and content', () => {
    it('should have a hero section that exists and is visible', () => {
      const hero = document.getElementById('hero');
      expect(hero).not.toBeNull();
      expect(hero.tagName).toBe('SECTION');
    });

    it('should have a headline with 5-10 words describing MirDB as a persistent key-value store', () => {
      const headline = document.querySelector('#hero h1');
      expect(headline).not.toBeNull();

      const text = headline.textContent.trim();
      const wordCount = text.split(/\s+/).length;
      expect(wordCount).toBeGreaterThanOrEqual(5);
      expect(wordCount).toBeLessThanOrEqual(10);

      const lower = text.toLowerCase();
      expect(lower).toMatch(/key.value|persistent|store/);
    });

    it('should have a subheadline with 15-25 words mentioning memcached protocol', () => {
      const subheadline = document.querySelector('#hero .hero-subheadline');
      expect(subheadline).not.toBeNull();

      const text = subheadline.textContent.trim();
      const wordCount = text.split(/\s+/).length;
      expect(wordCount).toBeGreaterThanOrEqual(15);
      expect(wordCount).toBeLessThanOrEqual(25);

      expect(text.toLowerCase()).toMatch(/memcached protocol/);
    });
  });

  describe('Test Case 2: Headline is h1 and unique', () => {
    it('should have the hero headline as an h1 element', () => {
      const h1 = document.querySelector('#hero h1');
      expect(h1).not.toBeNull();
      expect(h1.tagName).toBe('H1');
    });

    it('should have only one h1 element on the page', () => {
      const h1s = document.querySelectorAll('h1');
      expect(h1s.length).toBe(1);
    });
  });

  describe('Test Case 3: Primary CTA button styling and clickability', () => {
    it('should have a primary CTA button with text "Get Started"', () => {
      const cta = document.querySelector('#hero .hero-cta-primary');
      expect(cta).not.toBeNull();
      expect(cta.textContent.trim()).toMatch(/Get Started/i);
    });

    it('should have a primary CTA that is keyboard-focusable', () => {
      const cta = document.querySelector('#hero .hero-cta-primary');
      expect(cta).not.toBeNull();

      // The element should be a link or button (naturally focusable)
      // or have tabindex
      const isNaturallyFocusable = ['A', 'BUTTON'].includes(cta.tagName);
      const hasTabIndex = cta.hasAttribute('tabindex');
      expect(isNaturallyFocusable || hasTabIndex).toBe(true);
    });

    it('should have hover state defined in CSS', () => {
      const cssFiles = ['hero.css'];
      let hasHover = false;

      for (const file of cssFiles) {
        const cssPath = path.resolve(__dirname, '../css', file);
        if (fs.existsSync(cssPath)) {
          const css = fs.readFileSync(cssPath, 'utf-8');
          if (css.includes('.hero-cta-primary:hover') || css.includes('.hero-cta-primary:focus')) {
            hasHover = true;
            break;
          }
        }
      }

      expect(hasHover).toBe(true);
    });
  });

  describe('Test Case 4: Primary CTA scrolls to Quick Start section', () => {
    it('should link to #quick-start section', () => {
      const cta = document.querySelector('#hero .hero-cta-primary');
      expect(cta).not.toBeNull();

      const href = cta.getAttribute('href');
      expect(href).toBe('#quick-start');
    });

    it('should have the quick-start section target on the page', () => {
      const quickStart = document.getElementById('quick-start');
      expect(quickStart).not.toBeNull();
    });
  });

  describe('Test Case 5: Secondary CTA "View on GitHub" link', () => {
    it('should have a secondary CTA link with text "View on GitHub"', () => {
      const cta = document.querySelector('#hero .hero-cta-secondary');
      expect(cta).not.toBeNull();
      expect(cta.textContent.trim()).toMatch(/View on GitHub/i);
    });

    it('should link to the MirDB GitHub repository', () => {
      const cta = document.querySelector('#hero .hero-cta-secondary');
      const href = cta.getAttribute('href');
      expect(href).toMatch(/github\.com\/.*mirdb/i);
    });

    it('should open in a new tab with rel="noopener noreferrer"', () => {
      const cta = document.querySelector('#hero .hero-cta-secondary');
      expect(cta.getAttribute('target')).toBe('_blank');
      expect(cta.getAttribute('rel')).toMatch(/noopener/);
      expect(cta.getAttribute('rel')).toMatch(/noreferrer/);
    });
  });

  describe('Test Case 6: Logo image element', () => {
    it('should have a logo img element in the header', () => {
      const logo = document.querySelector('header img');
      expect(logo).not.toBeNull();
    });

    it('should reference logo.webp as the source', () => {
      const logo = document.querySelector('header img');
      const src = logo.getAttribute('src');
      expect(src).toMatch(/logo\.webp/i);
    });

    it('should have descriptive alt text', () => {
      const logo = document.querySelector('header img');
      const alt = logo.getAttribute('alt');
      expect(alt).not.toBeNull();
      expect(alt.trim().length).toBeGreaterThan(0);
    });

    it('should have width and height attributes for proper sizing', () => {
      const logo = document.querySelector('header img');
      const width = logo.getAttribute('width');
      const height = logo.getAttribute('height');
      expect(width).not.toBeNull();
      expect(height).not.toBeNull();
    });
  });

  describe('Test Case 7: No console errors and assets load', () => {
    it('should reference the logo from an existing file path', () => {
      const logo = document.querySelector('header img');
      const src = logo.getAttribute('src');
      const logoPath = path.resolve(__dirname, '..', src);
      expect(fs.existsSync(logoPath)).toBe(true);
    });

    it('should have all CSS stylesheets referenced', () => {
      const stylesheets = document.querySelectorAll('link[rel="stylesheet"]');
      const hrefs = Array.from(stylesheets).map((l) => l.getAttribute('href'));

      for (const href of hrefs) {
        const cssPath = path.resolve(__dirname, '..', href);
        expect(fs.existsSync(cssPath)).toBe(true);
      }
    });

    it('should have no inline event handler attributes that could cause JS errors', () => {
      const allElements = document.querySelectorAll('*');
      for (const el of allElements) {
        // Skip link tags which legitimately use onload for async CSS loading
        if (el.tagName === 'LINK') continue;
        const attrs = Array.from(el.attributes).map((a) => a.name);
        for (const attr of attrs) {
          expect(attr).not.toMatch(/^on/);
        }
      }
    });
  });
});
