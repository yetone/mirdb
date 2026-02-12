/**
 * HTML Structure Unit Tests
 * Owner: Scenario 1 - Hero Section Display (also used by Scenario 2)
 *
 * Unit tests for validating HTML structure and content
 * using JSDOM for DOM parsing.
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

// Load HTML content
const htmlPath = path.join(__dirname, '../../index.html');
const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

describe('HTML Structure Tests', () => {
  let dom;
  let document;

  beforeAll(() => {
    dom = new JSDOM(htmlContent);
    document = dom.window.document;
  });

  describe('Document Structure', () => {
    test('has valid HTML5 doctype', () => {
      expect(htmlContent.toLowerCase()).toMatch(/^<!doctype html>/);
    });

    test('html element has lang attribute', () => {
      const htmlElement = document.documentElement;
      expect(htmlElement.getAttribute('lang')).toBe('en');
    });

    test('has meta viewport for responsive design', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
      expect(viewport.getAttribute('content')).toContain('width=device-width');
    });

    test('has title element with MirDB', () => {
      const title = document.querySelector('title');
      expect(title).not.toBeNull();
      expect(title.textContent).toContain('MirDB');
    });
  });

  describe('Semantic Structure', () => {
    test('has header element', () => {
      const header = document.querySelector('header');
      expect(header).not.toBeNull();
    });

    test('has main element', () => {
      const main = document.querySelector('main');
      expect(main).not.toBeNull();
    });

    test('has footer element', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
    });
  });

  describe('Hero Section', () => {
    test('has h1 with MirDB', () => {
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.textContent).toContain('MirDB');
    });

    test('has logo image', () => {
      const logo = document.querySelector('.logo');
      expect(logo).not.toBeNull();
      expect(logo.tagName.toLowerCase()).toBe('img');
      expect(logo.getAttribute('src')).toContain('logo.gif');
    });

    test('has tagline paragraph', () => {
      const tagline = document.querySelector('.tagline');
      expect(tagline).not.toBeNull();
      expect(tagline.textContent.toLowerCase()).toContain('key-value');
    });
  });

  describe('Features Section (Scenario 2)', () => {
    let featuresSection;

    beforeAll(() => {
      featuresSection = document.querySelector('#features');
    });

    test('features section exists with id="features"', () => {
      // Test case 1: Check for features section element
      expect(featuresSection).not.toBeNull();
      expect(featuresSection.tagName.toLowerCase()).toBe('section');
    });

    test('features section has h2 heading', () => {
      const heading = featuresSection.querySelector('h2');
      expect(heading).not.toBeNull();
      expect(heading.textContent.toLowerCase()).toContain('features');
    });

    test('Memcached protocol feature is present', () => {
      // Test case 2: Search for Memcached protocol feature
      const text = featuresSection.textContent.toLowerCase();
      expect(text).toContain('memcached');
      expect(text).toContain('protocol');
    });

    test('persistent storage feature is present', () => {
      // Test case 3: Search for persistent storage feature
      const text = featuresSection.textContent.toLowerCase();
      expect(text).toContain('persistent');
      expect(text).toContain('storage');
    });

    test('skip-list memtable feature is present', () => {
      // Test case 4: Search for skip-list memtable feature
      const text = featuresSection.textContent.toLowerCase();
      const hasSkipList = text.includes('skip-list') || text.includes('skiplist');
      expect(hasSkipList).toBe(true);
      expect(text).toContain('memtable');
    });

    test('compaction feature is present', () => {
      // Test case 5: Search for compaction feature
      const text = featuresSection.textContent.toLowerCase();
      expect(text).toContain('compaction');
    });

    test('Rust implementation is mentioned', () => {
      // Test case 6: Search for Rust implementation mention
      const text = featuresSection.textContent;
      expect(text).toContain('Rust');
    });

    test('has feature cards with proper structure', () => {
      const featureCards = featuresSection.querySelectorAll('.feature-card');
      expect(featureCards.length).toBeGreaterThanOrEqual(5);

      featureCards.forEach(card => {
        const title = card.querySelector('h3');
        const description = card.querySelector('p');
        expect(title).not.toBeNull();
        expect(description).not.toBeNull();
        expect(title.textContent.length).toBeGreaterThan(0);
        expect(description.textContent.length).toBeGreaterThan(0);
      });
    });

    test('features grid container exists', () => {
      const grid = featuresSection.querySelector('.features-grid');
      expect(grid).not.toBeNull();
    });

    test('all five key features are listed', () => {
      const text = featuresSection.textContent;
      const lowerText = text.toLowerCase();

      // Check each required feature
      expect(lowerText.includes('memcached') && lowerText.includes('protocol')).toBe(true);
      expect(lowerText.includes('persistent') && lowerText.includes('storage')).toBe(true);
      expect(lowerText.includes('skip-list') || lowerText.includes('skiplist')).toBe(true);
      expect(lowerText).toContain('memtable');
      expect(lowerText).toContain('compaction');
      expect(text).toContain('Rust');
    });
  });

  describe('Other Sections', () => {
    test('has demo section', () => {
      const demo = document.querySelector('#demo');
      expect(demo).not.toBeNull();
    });

    test('has quickstart section', () => {
      const quickstart = document.querySelector('#quickstart');
      expect(quickstart).not.toBeNull();
    });
  });
});
