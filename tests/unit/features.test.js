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

/**
 * Architecture Highlights Section Tests
 * Owner: Scenario 17 - Architecture Highlights Section
 *
 * Tests:
 * - Architecture section exists
 * - Tokio mention for async I/O
 * - LSM-tree mention for storage architecture
 */
describe('Architecture Highlights Section', () => {
  beforeEach(() => {
    loadHTML('index.html');
  });

  describe('Test Case 1: Architecture or technical details section exists', () => {
    test('architecture section or features section with architecture content exists', () => {
      // Check for dedicated architecture section or features section containing architecture highlights
      const architectureSection = document.getElementById('architecture');
      const featuresSection = querySection('features');

      // Either a dedicated architecture section exists, or features section contains architecture info
      const hasArchitectureContent = architectureSection !== null ||
        (featuresSection && (
          featuresSection.textContent.toLowerCase().includes('tokio') ||
          featuresSection.textContent.toLowerCase().includes('lsm') ||
          featuresSection.textContent.toLowerCase().includes('skiplist') ||
          featuresSection.textContent.toLowerCase().includes('skip list')
        ));

      expect(hasArchitectureContent).toBe(true);
    });
  });

  describe('Test Case 2: Page mentions Tokio for async I/O', () => {
    test('page contains Tokio mention for async functionality', () => {
      const pageContent = document.body.textContent.toLowerCase();
      const hasTokioMention = pageContent.includes('tokio');
      expect(hasTokioMention).toBe(true);
    });

    test('Tokio is mentioned in context of async or performance', () => {
      const pageContent = document.body.textContent.toLowerCase();
      // Tokio should be mentioned alongside async, I/O, or performance concepts
      const hasTokioWithContext =
        (pageContent.includes('tokio') && pageContent.includes('async')) ||
        (pageContent.includes('tokio') && pageContent.includes('i/o')) ||
        (pageContent.includes('tokio') && pageContent.includes('throughput'));
      expect(hasTokioWithContext).toBe(true);
    });
  });

  describe('Test Case 3: Page mentions LSM-tree storage architecture', () => {
    test('page contains LSM-tree mention', () => {
      const pageContent = document.body.textContent.toLowerCase();
      const hasLsmTreeMention =
        pageContent.includes('lsm-tree') ||
        pageContent.includes('lsm tree') ||
        pageContent.includes('log-structured merge');
      expect(hasLsmTreeMention).toBe(true);
    });

    test('LSM-tree is mentioned in context of storage or persistence', () => {
      const featuresSection = querySection('features');
      const architectureSection = document.getElementById('architecture');

      const sectionToCheck = architectureSection || featuresSection;
      expect(sectionToCheck).toBeInTheDocument();

      const sectionContent = sectionToCheck.textContent.toLowerCase();
      const hasLsmWithStorageContext =
        (sectionContent.includes('lsm') && sectionContent.includes('storage')) ||
        (sectionContent.includes('lsm') && sectionContent.includes('persist'));
      expect(hasLsmWithStorageContext).toBe(true);
    });
  });
});
