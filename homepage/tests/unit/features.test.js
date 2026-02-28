/**
 * Features Section Unit Tests
 * Owner: Scenario 2 - Feature Grid Display
 *
 * Tests for:
 * - Features section existence and structure
 * - Feature card count (3-4 cards)
 * - Feature content (Memcached, persistence, LSM, Tokio)
 * - Feature card icons
 * - Grid layout
 * - Hover effects (CSS verification)
 *
 * Requirements: REQ-2, NFR-1
 * @jest-environment jsdom
 */

import { readFileSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

describe('Feature Grid Display', () => {
  let featuresSection;

  beforeEach(() => {
    // Load the homepage HTML
    const htmlPath = resolve(__dirname, '../../index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    document.documentElement.innerHTML = html;
    featuresSection = document.getElementById('features');
  });

  /**
   * Test Case 1: Query for features section
   * Input: Query for features section
   * Expected: Section exists with id='features' or class containing 'features'
   */
  describe('Test Case 1: Features Section Existence', () => {
    test('features section exists with id="features"', () => {
      expect(featuresSection).not.toBeNull();
      expect(featuresSection).toBeInTheDocument();
    });

    test('features section has class containing "features"', () => {
      expect(featuresSection.classList.contains('features')).toBe(true);
    });

    test('features section is a semantic section element', () => {
      expect(featuresSection.tagName.toLowerCase()).toBe('section');
    });

    test('features section has aria-labelledby for accessibility', () => {
      expect(featuresSection.getAttribute('aria-labelledby')).toBe('features-heading');
    });
  });

  /**
   * Test Case 2: Count feature card elements
   * Input: Count feature card elements
   * Expected: Between 3 and 4 feature cards exist in the grid
   */
  describe('Test Case 2: Feature Card Count', () => {
    test('between 3 and 4 feature cards exist in the grid', () => {
      const featureCards = featuresSection.querySelectorAll('.feature-card');
      expect(featureCards.length).toBeGreaterThanOrEqual(3);
      expect(featureCards.length).toBeLessThanOrEqual(4);
    });

    test('feature cards are direct children of features-grid', () => {
      const featuresGrid = featuresSection.querySelector('.features-grid');
      expect(featuresGrid).not.toBeNull();
      const cardsInGrid = featuresGrid.querySelectorAll(':scope > .feature-card');
      expect(cardsInGrid.length).toBeGreaterThanOrEqual(3);
      expect(cardsInGrid.length).toBeLessThanOrEqual(4);
    });
  });

  /**
   * Test Case 3: Query feature cards for Memcached-related content
   * Input: Query feature cards for Memcached-related content
   * Expected: At least one card contains text about 'memcached' or 'protocol'
   */
  describe('Test Case 3: Memcached Compatibility Feature', () => {
    test('at least one card contains text about memcached or protocol', () => {
      const featureCards = featuresSection.querySelectorAll('.feature-card');
      const hasMemcachedContent = Array.from(featureCards).some(card => {
        const text = card.textContent.toLowerCase();
        return text.includes('memcached') || text.includes('protocol');
      });
      expect(hasMemcachedContent).toBe(true);
    });

    test('Memcached feature card has proper title', () => {
      const featureCards = featuresSection.querySelectorAll('.feature-card');
      const memcachedCard = Array.from(featureCards).find(card =>
        card.textContent.toLowerCase().includes('memcached')
      );
      expect(memcachedCard).not.toBeUndefined();
      const title = memcachedCard.querySelector('.feature-card__title');
      expect(title).not.toBeNull();
    });
  });

  /**
   * Test Case 4: Query feature cards for persistence-related content
   * Input: Query feature cards for persistence-related content
   * Expected: At least one card contains text about 'persistent' or 'disk' or 'storage'
   */
  describe('Test Case 4: Data Persistence Feature', () => {
    test('at least one card contains text about persistent, disk, or storage', () => {
      const featureCards = featuresSection.querySelectorAll('.feature-card');
      const hasPersistenceContent = Array.from(featureCards).some(card => {
        const text = card.textContent.toLowerCase();
        return text.includes('persistent') || text.includes('disk') || text.includes('storage');
      });
      expect(hasPersistenceContent).toBe(true);
    });

    test('persistence feature mentions durability or surviving restarts', () => {
      const featureCards = featuresSection.querySelectorAll('.feature-card');
      const persistenceCard = Array.from(featureCards).find(card =>
        card.textContent.toLowerCase().includes('persistent')
      );
      expect(persistenceCard).not.toBeUndefined();
      const description = persistenceCard.textContent.toLowerCase();
      const hasDurabilityText = description.includes('survives') ||
                                description.includes('durability') ||
                                description.includes('durable');
      expect(hasDurabilityText).toBe(true);
    });
  });

  /**
   * Test Case 5: Query feature cards for architecture-related content
   * Input: Query feature cards for architecture-related content
   * Expected: Cards mention 'LSM' or 'SSTable' or 'Tokio' or 'async'
   */
  describe('Test Case 5: Architecture Features', () => {
    test('cards mention LSM, SSTable, Tokio, or async', () => {
      const featureCards = featuresSection.querySelectorAll('.feature-card');
      const hasArchitectureContent = Array.from(featureCards).some(card => {
        const text = card.textContent.toLowerCase();
        return text.includes('lsm') ||
               text.includes('sstable') ||
               text.includes('tokio') ||
               text.includes('async');
      });
      expect(hasArchitectureContent).toBe(true);
    });

    test('LSM Tree feature card exists', () => {
      const featureCards = featuresSection.querySelectorAll('.feature-card');
      const lsmCard = Array.from(featureCards).find(card =>
        card.textContent.toLowerCase().includes('lsm')
      );
      expect(lsmCard).not.toBeUndefined();
    });

    test('Tokio/async networking feature card exists', () => {
      const featureCards = featuresSection.querySelectorAll('.feature-card');
      const tokioCard = Array.from(featureCards).find(card =>
        card.textContent.toLowerCase().includes('tokio') ||
        card.textContent.toLowerCase().includes('async')
      );
      expect(tokioCard).not.toBeUndefined();
    });
  });

  /**
   * Test Case 6: Check feature cards have icons
   * Input: Check feature cards have icons
   * Expected: Each feature card contains an icon/image element
   */
  describe('Test Case 6: Feature Card Icons', () => {
    test('each feature card contains an icon element', () => {
      const featureCards = featuresSection.querySelectorAll('.feature-card');
      featureCards.forEach((card) => {
        const icon = card.querySelector('.feature-card__icon');
        expect(icon).not.toBeNull();
      });
    });

    test('each icon contains SVG or img element', () => {
      const featureCards = featuresSection.querySelectorAll('.feature-card');
      featureCards.forEach((card) => {
        const icon = card.querySelector('.feature-card__icon');
        const svg = icon.querySelector('svg');
        const img = icon.querySelector('img');
        expect(svg || img).not.toBeNull();
      });
    });

    test('icons are hidden from screen readers', () => {
      const featureCards = featuresSection.querySelectorAll('.feature-card');
      featureCards.forEach((card) => {
        const icon = card.querySelector('.feature-card__icon');
        expect(icon.getAttribute('aria-hidden')).toBe('true');
      });
    });
  });

  /**
   * Test Case 7: Test feature grid layout on desktop
   * Input: Test feature grid layout on desktop
   * Expected: Features display in 3-4 column grid layout
   */
  describe('Test Case 7: Feature Grid Layout', () => {
    test('features-grid container exists', () => {
      const featuresGrid = featuresSection.querySelector('.features-grid');
      expect(featuresGrid).not.toBeNull();
      expect(featuresGrid).toBeInTheDocument();
    });

    test('features grid contains all feature cards', () => {
      const featuresGrid = featuresSection.querySelector('.features-grid');
      const cardsInGrid = featuresGrid.querySelectorAll('.feature-card');
      expect(cardsInGrid.length).toBeGreaterThanOrEqual(3);
      expect(cardsInGrid.length).toBeLessThanOrEqual(4);
    });

    test('features section has proper header structure', () => {
      const header = featuresSection.querySelector('.features__header');
      expect(header).not.toBeNull();
      const title = header.querySelector('.features__title');
      const description = header.querySelector('.features__description');
      expect(title).not.toBeNull();
      expect(description).not.toBeNull();
    });

    test('features heading is H2 element', () => {
      const heading = featuresSection.querySelector('#features-heading');
      expect(heading).not.toBeNull();
      expect(heading.tagName.toLowerCase()).toBe('h2');
    });
  });

  /**
   * Test Case 8: Test feature card hover effects (CSS verification)
   * Input: Test feature card hover effects
   * Expected: Cards have visible hover state with smooth transition
   * Note: This is a CSS verification test
   */
  describe('Test Case 8: Feature Card Styling & Hover Effects', () => {
    let css;

    beforeAll(() => {
      const cssPath = resolve(__dirname, '../../css/components/features.css');
      css = readFileSync(cssPath, 'utf-8');
    });

    test('CSS defines hover state with transform', () => {
      expect(css).toMatch(/\.feature-card:hover/);
      expect(css).toMatch(/transform.*translateY/);
    });

    test('CSS defines transition for smooth animation', () => {
      expect(css).toMatch(/transition/);
    });

    test('CSS defines shadow on hover', () => {
      expect(css).toMatch(/\.feature-card:hover[\s\S]*?box-shadow/);
    });

    test('CSS defines 4-column grid layout for desktop', () => {
      expect(css).toMatch(/grid-template-columns.*repeat\(4/);
    });

    test('CSS includes responsive breakpoints', () => {
      expect(css).toMatch(/@media/);
      expect(css).toMatch(/grid-template-columns.*repeat\(3/);
      expect(css).toMatch(/grid-template-columns.*repeat\(2/);
      expect(css).toMatch(/grid-template-columns.*1fr/);
    });
  });

  /**
   * Additional structural and accessibility tests
   */
  describe('Structure & Accessibility', () => {
    test('feature cards have proper structure with title and description', () => {
      const featureCards = featuresSection.querySelectorAll('.feature-card');
      featureCards.forEach((card) => {
        const title = card.querySelector('.feature-card__title');
        const description = card.querySelector('.feature-card__description');
        expect(title).not.toBeNull();
        expect(description).not.toBeNull();
        expect(title.textContent.trim().length).toBeGreaterThan(0);
        expect(description.textContent.trim().length).toBeGreaterThan(0);
      });
    });

    test('feature cards are article elements for semantics', () => {
      const featureCards = featuresSection.querySelectorAll('.feature-card');
      featureCards.forEach((card) => {
        expect(card.tagName.toLowerCase()).toBe('article');
      });
    });

    test('feature card titles are H3 elements', () => {
      const featureCards = featuresSection.querySelectorAll('.feature-card');
      featureCards.forEach((card) => {
        const title = card.querySelector('.feature-card__title');
        expect(title.tagName.toLowerCase()).toBe('h3');
      });
    });

    test('feature cards have aria-labelledby pointing to their titles', () => {
      const featureCards = featuresSection.querySelectorAll('.feature-card');
      featureCards.forEach((card) => {
        const labelledBy = card.getAttribute('aria-labelledby');
        expect(labelledBy).not.toBeNull();
        const title = card.querySelector(`#${labelledBy}`);
        expect(title).not.toBeNull();
      });
    });

    test('features section heading contains "feature" text', () => {
      const heading = featuresSection.querySelector('#features-heading');
      expect(heading.textContent.toLowerCase()).toContain('feature');
    });
  });
});
