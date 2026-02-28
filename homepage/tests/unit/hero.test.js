/**
 * Hero Section Tests
 * Owner: Scenario 1 - Hero Section & Value Proposition
 *
 * Tests for:
 * - Hero section visibility and structure
 * - Logo presence and attributes
 * - Product name (H1) display
 * - Tagline/value proposition text
 * - Layout and styling (desktop viewport)
 *
 * Requirements: REQ-1, NFR-1
 * @jest-environment jsdom
 */

import { readFileSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

describe('Hero Section & Value Proposition', () => {
  beforeEach(() => {
    // Load the homepage HTML
    const htmlPath = resolve(__dirname, '../../index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    document.documentElement.innerHTML = html;
  });

  /**
   * Test Case 1: Hero section exists with proper ID
   * Input: Load homepage and query for hero section
   * Expected: Hero section exists with id='hero' or class containing 'hero'
   */
  describe('Test Case 1: Hero Section Existence', () => {
    test('hero section exists with id="hero"', () => {
      const heroSection = document.getElementById('hero');
      expect(heroSection).not.toBeNull();
      expect(heroSection).toBeInTheDocument();
    });

    test('hero section has class containing "hero"', () => {
      const heroSection = document.getElementById('hero');
      expect(heroSection.classList.contains('hero')).toBe(true);
    });

    test('hero section is a semantic section element', () => {
      const heroSection = document.getElementById('hero');
      expect(heroSection.tagName.toLowerCase()).toBe('section');
    });

    test('hero section has aria-labelledby for accessibility', () => {
      const heroSection = document.getElementById('hero');
      expect(heroSection.getAttribute('aria-labelledby')).toBe('hero-title');
    });
  });

  /**
   * Test Case 2: Logo element in hero section
   * Input: Query for logo element in hero section
   * Expected: Logo image exists with alt text containing 'MirDB' and src pointing to valid image
   */
  describe('Test Case 2: Logo Presence', () => {
    test('logo image exists in hero section', () => {
      const heroSection = document.getElementById('hero');
      const logo = heroSection.querySelector('.hero__logo img');
      expect(logo).not.toBeNull();
      expect(logo).toBeInTheDocument();
    });

    test('logo has alt text containing "MirDB"', () => {
      const heroSection = document.getElementById('hero');
      const logo = heroSection.querySelector('.hero__logo img');
      expect(logo.getAttribute('alt')).toContain('MirDB');
    });

    test('logo src points to valid image path', () => {
      const heroSection = document.getElementById('hero');
      const logo = heroSection.querySelector('.hero__logo img');
      const src = logo.getAttribute('src');
      expect(src).toBeTruthy();
      expect(src).toMatch(/\.(svg|png|jpg|jpeg|gif|webp)$/i);
    });

    test('logo has width and height attributes for layout stability', () => {
      const heroSection = document.getElementById('hero');
      const logo = heroSection.querySelector('.hero__logo img');
      expect(logo.getAttribute('width')).toBeTruthy();
      expect(logo.getAttribute('height')).toBeTruthy();
    });
  });

  /**
   * Test Case 3: Main heading in hero section
   * Input: Query for main heading in hero section
   * Expected: H1 element exists containing 'MirDB' text
   */
  describe('Test Case 3: Product Name Display', () => {
    test('H1 element exists in hero section', () => {
      const heroSection = document.getElementById('hero');
      const h1 = heroSection.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1).toBeInTheDocument();
    });

    test('H1 contains "MirDB" text', () => {
      const heroSection = document.getElementById('hero');
      const h1 = heroSection.querySelector('h1');
      expect(h1.textContent).toContain('MirDB');
    });

    test('H1 has id for accessibility reference', () => {
      const heroSection = document.getElementById('hero');
      const h1 = heroSection.querySelector('h1');
      expect(h1.id).toBe('hero-title');
    });

    test('H1 has proper typography class', () => {
      const heroSection = document.getElementById('hero');
      const h1 = heroSection.querySelector('h1');
      expect(h1.classList.contains('hero__title')).toBe(true);
    });
  });

  /**
   * Test Case 4: Tagline/description element
   * Input: Query for tagline/description element
   * Expected: Element contains text about 'persistent key-value store' and 'memcached'
   */
  describe('Test Case 4: Value Proposition Display', () => {
    test('tagline element exists in hero section', () => {
      const heroSection = document.getElementById('hero');
      const tagline = heroSection.querySelector('.hero__tagline');
      expect(tagline).not.toBeNull();
      expect(tagline).toBeInTheDocument();
    });

    test('tagline contains "persistent" keyword', () => {
      const heroSection = document.getElementById('hero');
      const tagline = heroSection.querySelector('.hero__tagline');
      expect(tagline.textContent.toLowerCase()).toContain('persistent');
    });

    test('tagline contains "key-value" keyword', () => {
      const heroSection = document.getElementById('hero');
      const tagline = heroSection.querySelector('.hero__tagline');
      expect(tagline.textContent.toLowerCase()).toContain('key-value');
    });

    test('tagline contains "memcached" keyword', () => {
      const heroSection = document.getElementById('hero');
      const tagline = heroSection.querySelector('.hero__tagline');
      expect(tagline.textContent.toLowerCase()).toContain('memcached');
    });

    test('tagline has the exact expected text', () => {
      const heroSection = document.getElementById('hero');
      const tagline = heroSection.querySelector('.hero__tagline');
      expect(tagline.textContent.trim()).toBe('A Persistent Key-Value Store with Memcached Protocol');
    });
  });

  /**
   * Test Case 5: Hero section layout on desktop viewport
   * Input: Check hero section layout on desktop viewport (1280px)
   * Expected: Hero section spans full width with centered content
   */
  describe('Test Case 5: Hero Section Layout (Desktop)', () => {
    test('hero section has full-width styling', () => {
      const heroSection = document.getElementById('hero');
      // Hero should not have constrained width at the section level
      expect(heroSection.classList.contains('hero')).toBe(true);
    });

    test('hero content is centered with container', () => {
      const heroSection = document.getElementById('hero');
      const content = heroSection.querySelector('.hero__content');
      expect(content).not.toBeNull();
    });

    test('hero section contains CTA buttons', () => {
      const heroSection = document.getElementById('hero');
      const actions = heroSection.querySelector('.hero__actions');
      expect(actions).not.toBeNull();
      const buttons = actions.querySelectorAll('a');
      expect(buttons.length).toBeGreaterThanOrEqual(1);
    });

    test('GitHub button links to correct repository', () => {
      const heroSection = document.getElementById('hero');
      const githubBtn = heroSection.querySelector('.hero__btn--github');
      expect(githubBtn).not.toBeNull();
      expect(githubBtn.getAttribute('href')).toContain('github.com');
      expect(githubBtn.getAttribute('href')).toContain('mirdb');
    });

    test('GitHub link opens in new tab safely', () => {
      const heroSection = document.getElementById('hero');
      const githubBtn = heroSection.querySelector('.hero__btn--github');
      expect(githubBtn.getAttribute('target')).toBe('_blank');
      expect(githubBtn.getAttribute('rel')).toContain('noopener');
    });
  });

  /**
   * Additional accessibility and structure tests
   */
  describe('Accessibility & Structure', () => {
    test('hero section is within main element', () => {
      const main = document.querySelector('main');
      const heroSection = document.getElementById('hero');
      expect(main).not.toBeNull();
      expect(main.contains(heroSection)).toBe(true);
    });

    test('skip link exists for keyboard navigation', () => {
      const skipLink = document.querySelector('.skip-link');
      expect(skipLink).not.toBeNull();
      expect(skipLink.getAttribute('href')).toBe('#main-content');
    });

    test('GitHub button has accessible label', () => {
      const githubBtn = document.querySelector('.hero__btn--github');
      const ariaLabel = githubBtn.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel.toLowerCase()).toContain('github');
    });

    test('GitHub icon is hidden from screen readers', () => {
      const githubIcon = document.querySelector('.hero__github-icon');
      expect(githubIcon.getAttribute('aria-hidden')).toBe('true');
    });

    test('feature badges have proper aria-label', () => {
      const badges = document.querySelector('.hero__badges');
      expect(badges).not.toBeNull();
      expect(badges.getAttribute('aria-label')).toBe('Key features');
    });
  });
});
