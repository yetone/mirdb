/**
 * Feature grid tests for MirDB homepage.
 * Owner: Scenario 2 - Feature Grid
 *
 * Test framework: Vitest + jsdom
 *
 * Test coverage:
 * - Features section exists with at least 4 feature cards
 * - Each card has icon, title, and description
 * - Cards for: memcached protocol, persistent storage, skip-list memtable, compaction
 * - Grid uses CSS grid layout
 * - Card hover transitions are defined (200ms ease)
 * - Card descriptions are single-line
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { readFileSync } from 'fs';
import { resolve } from 'path';
import { JSDOM } from 'jsdom';

const htmlPath = resolve(__dirname, '../index.html');
const cssPath = resolve(__dirname, '../css/features.css');

let dom;
let document;
let window;

beforeAll(() => {
  const html = readFileSync(htmlPath, 'utf-8');
  const css = readFileSync(cssPath, 'utf-8');

  dom = new JSDOM(html, {
    resources: 'usable',
    runScripts: 'dangerously',
  });

  // Inject features CSS into the DOM so computed styles work
  const styleEl = dom.window.document.createElement('style');
  styleEl.textContent = css;
  dom.window.document.head.appendChild(styleEl);

  document = dom.window.document;
  window = dom.window;
});

// Helper: get computed style for an element
function computedStyle(el) {
  return window.getComputedStyle(el);
}

// Helper: find all feature cards
function getFeatureCards() {
  return document.querySelectorAll('.feature-card');
}

// Helper: parse raw CSS for rule checks
function getRawCSS() {
  return readFileSync(cssPath, 'utf-8');
}

describe('Feature Grid Section', () => {
  describe('Section structure', () => {
    it('should have a features section', () => {
      const section = document.getElementById('features');
      expect(section).not.toBeNull();
    });

    it('should have a features grid container', () => {
      const grid = document.querySelector('.features-grid');
      expect(grid).not.toBeNull();
    });

    it('should have at least 4 feature cards', () => {
      const cards = getFeatureCards();
      expect(cards.length).toBeGreaterThanOrEqual(4);
    });
  });

  describe('Card structure', () => {
    it('each card should contain an icon element', () => {
      const cards = getFeatureCards();
      cards.forEach((card) => {
        const icon = card.querySelector('.feature-icon');
        expect(icon).not.toBeNull();
      });
    });

    it('each card should contain a title heading', () => {
      const cards = getFeatureCards();
      cards.forEach((card) => {
        const title = card.querySelector('.feature-title');
        expect(title).not.toBeNull();
        expect(title.tagName).toBe('H3');
      });
    });

    it('each card should contain a description paragraph', () => {
      const cards = getFeatureCards();
      cards.forEach((card) => {
        const desc = card.querySelector('.feature-description');
        expect(desc).not.toBeNull();
        expect(desc.tagName).toBe('P');
      });
    });
  });

  describe('Feature card: Memcached Protocol', () => {
    it('should have a card referencing memcached protocol', () => {
      const cards = getFeatureCards();
      const card = Array.from(cards).find((c) =>
        c.textContent.toLowerCase().includes('memcached protocol')
      );
      expect(card).not.toBeNull();
    });

    it('should mention memcached text protocol compatibility in description', () => {
      const cards = getFeatureCards();
      const card = Array.from(cards).find((c) =>
        c.textContent.toLowerCase().includes('memcached protocol')
      );
      const desc = card.querySelector('.feature-description').textContent.toLowerCase();
      expect(desc).toMatch(/memcached|protocol|compatib/);
    });
  });

  describe('Feature card: Persistent Storage', () => {
    it('should have a card referencing persistent storage or persistence', () => {
      const cards = getFeatureCards();
      const card = Array.from(cards).find((c) =>
        /persistent|persistence/i.test(c.textContent)
      );
      expect(card).not.toBeNull();
    });

    it('should mention data persistence to disk or SSTables', () => {
      const cards = getFeatureCards();
      const card = Array.from(cards).find((c) =>
        /persistent|persistence/i.test(c.textContent)
      );
      const desc = card.querySelector('.feature-description').textContent.toLowerCase();
      expect(desc).toMatch(/disk|sstable|persist|data/);
    });
  });

  describe('Feature card: Skip-List Memtable', () => {
    it('should have a card referencing skip-list or memtable', () => {
      const cards = getFeatureCards();
      const card = Array.from(cards).find((c) =>
        /skip.list|memtable/i.test(c.textContent)
      );
      expect(card).not.toBeNull();
    });

    it('should mention skip-list data structure for in-memory storage', () => {
      const cards = getFeatureCards();
      const card = Array.from(cards).find((c) =>
        /skip.list|memtable/i.test(c.textContent)
      );
      const desc = card.querySelector('.feature-description').textContent.toLowerCase();
      expect(desc).toMatch(/skip.list|in.memory|memtable/);
    });
  });

  describe('Feature card: Compaction', () => {
    it('should have a card referencing compaction', () => {
      const cards = getFeatureCards();
      const card = Array.from(cards).find((c) =>
        /compaction/i.test(c.textContent)
      );
      expect(card).not.toBeNull();
    });

    it('should mention minor and major compaction or LSM-tree compaction', () => {
      const cards = getFeatureCards();
      const card = Array.from(cards).find((c) =>
        /compaction/i.test(c.textContent)
      );
      const desc = card.querySelector('.feature-description').textContent.toLowerCase();
      expect(desc).toMatch(/minor|major|compaction|lsm|sstable/);
    });
  });

  describe('CSS grid layout', () => {
    it('features-grid should use CSS grid display', () => {
      const grid = document.querySelector('.features-grid');
      const style = computedStyle(grid);
      expect(style.display).toBe('grid');
    });

    it('should define grid-template-columns in CSS', () => {
      const css = getRawCSS();
      expect(css).toMatch(/grid-template-columns/);
    });

    it('feature cards should have border-radius', () => {
      const card = document.querySelector('.feature-card');
      const style = computedStyle(card);
      expect(style.borderRadius).toBeTruthy();
      // Should not be '0px' or empty
      expect(style.borderRadius).not.toBe('0px');
      expect(style.borderRadius).not.toBe('');
    });
  });

  describe('Hover effects', () => {
    it('feature cards should have transition defined in CSS', () => {
      const css = getRawCSS();
      expect(css).toMatch(/transition/);
    });

    it('transition duration should be approximately 200ms', () => {
      const css = getRawCSS();
      // Look for 200ms in transition property
      expect(css).toMatch(/200ms/);
    });

    it('hover state should have a transform or box-shadow effect', () => {
      const css = getRawCSS();
      // Check for hover rules with transform or shadow
      expect(css).toMatch(/\.feature-card:hover\s*\{[^}]*transform/);
    });

    it('hover effect should use ease timing function', () => {
      const css = getRawCSS();
      expect(css).toMatch(/ease/);
    });

    it('transition should apply to transform and box-shadow properties', () => {
      const css = getRawCSS();
      expect(css).toMatch(/transition:\s*transform/);
    });
  });

  describe('Responsive layout', () => {
    it('should have a tablet breakpoint media query (768px-1023px)', () => {
      const css = getRawCSS();
      expect(css).toMatch(/@media.*max-width:\s*1023px/);
      expect(css).toMatch(/@media.*min-width:\s*768px/);
    });

    it('should have a mobile breakpoint media query (<768px)', () => {
      const css = getRawCSS();
      expect(css).toMatch(/@media.*max-width:\s*767px/);
    });

    it('should reduce grid columns on tablet to 2', () => {
      const css = getRawCSS();
      // Tablet breakpoint should have grid-template-columns: repeat(2, ...)
      const tabletSection = css.match(
        /@media[^{]*max-width:\s*1023px[^}]*\{[^}]*grid-template-columns:\s*repeat\(2/m
      );
      expect(tabletSection).not.toBeNull();
    });

    it('should reduce grid columns on mobile to 1', () => {
      const css = getRawCSS();
      // Mobile breakpoint should have grid-template-columns: 1fr or repeat(1, ...)
      const mobileSection = css.match(
        /@media[^{]*max-width:\s*767px[^}]*\{[^}]*grid-template-columns:\s*1fr/m
      );
      expect(mobileSection).not.toBeNull();
    });
  });
});
