/**
 * Features Section Tests
 * Owner: Scenario 2 - Features Section Display
 *
 * Tests:
 * - Features section exists
 * - Memcached compatibility feature
 * - Persistent storage feature
 * - High performance feature
 * - Feature card structure (icon, heading, description)
 * - Architecture highlights (Scenario 17 shared)
 * - Roadmap section (Scenario 18 shared)
 */

const { loadHTML, querySection } = require('../helpers/dom-utils');

describe('Features Section Display', () => {
  beforeEach(() => {
    loadHTML('index.html');
  });

  describe('Test Case 1: Features section exists with at least 4 feature items', () => {
    test('features section exists', () => {
      const featuresSection = querySection('features');
      expect(featuresSection).toBeInTheDocument();
    });

    test('features section contains at least 4 feature cards', () => {
      const featuresSection = querySection('features');
      const featureCards = featuresSection.querySelectorAll('.feature-card');
      expect(featureCards.length).toBeGreaterThanOrEqual(4);
    });
  });

  describe('Test Case 2: Memcached compatibility feature exists', () => {
    test('feature card with Memcached in title or description exists', () => {
      const featuresSection = querySection('features');
      const featureCards = featuresSection.querySelectorAll('.feature-card');

      let hasMemcachedFeature = false;
      featureCards.forEach(card => {
        const cardText = card.textContent.toLowerCase();
        if (cardText.includes('memcached')) {
          hasMemcachedFeature = true;
        }
      });

      expect(hasMemcachedFeature).toBe(true);
    });
  });

  describe('Test Case 3: Persistent storage feature exists', () => {
    test('feature card mentioning persistent or LSM-tree exists', () => {
      const featuresSection = querySection('features');
      const featureCards = featuresSection.querySelectorAll('.feature-card');

      let hasPersistentFeature = false;
      featureCards.forEach(card => {
        const cardText = card.textContent.toLowerCase();
        if (cardText.includes('persistent') || cardText.includes('lsm-tree') || cardText.includes('lsm tree')) {
          hasPersistentFeature = true;
        }
      });

      expect(hasPersistentFeature).toBe(true);
    });
  });

  describe('Test Case 4: High performance feature exists', () => {
    test('feature card mentioning Tokio, async, or performance exists', () => {
      const featuresSection = querySection('features');
      const featureCards = featuresSection.querySelectorAll('.feature-card');

      let hasPerformanceFeature = false;
      featureCards.forEach(card => {
        const cardText = card.textContent.toLowerCase();
        if (cardText.includes('tokio') || cardText.includes('async') || cardText.includes('performance')) {
          hasPerformanceFeature = true;
        }
      });

      expect(hasPerformanceFeature).toBe(true);
    });
  });

  describe('Test Case 5: Feature card structure', () => {
    test('each feature card has icon/image, heading, and description', () => {
      const featuresSection = querySection('features');
      const featureCards = featuresSection.querySelectorAll('.feature-card');

      expect(featureCards.length).toBeGreaterThan(0);

      featureCards.forEach((card, index) => {
        // Check for icon (SVG or img)
        const icon = card.querySelector('svg, img, .feature-icon');
        expect(icon).toBeInTheDocument();

        // Check for heading (h3, h4, or element with font-semibold)
        const heading = card.querySelector('h3, h4, [class*="font-semibold"], [class*="font-bold"]');
        expect(heading).toBeInTheDocument();
        expect(heading.textContent.trim().length).toBeGreaterThan(0);

        // Check for description (p tag or text content after heading)
        const description = card.querySelector('p');
        expect(description).toBeInTheDocument();
        expect(description.textContent.trim().length).toBeGreaterThan(0);
      });
    });
  });
});
