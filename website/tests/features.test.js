/**
 * Tests for the Features Section of MirDB Homepage
 *
 * Scenario: Features Section Display and Content
 * Verifies that the features section presents at least 5 key features with clear,
 * scannable descriptions including memcached compatibility, persistent storage,
 * LSM-tree architecture, async networking, background compaction, and write-ahead logging.
 *
 * @jest-environment jsdom
 */

const fs = require('fs');
const path = require('path');

// Load HTML content before tests
beforeEach(() => {
  // Load HTML for each test to ensure clean state
  const htmlPath = path.join(__dirname, '..', 'index.html');
  const html = fs.readFileSync(htmlPath, 'utf8');
  document.body.innerHTML = html;
});

describe('Features Section Display and Content', () => {

  /**
   * Test Case 1: Query DOM for feature card/item elements
   * Expected: At least 5 feature items found in features section
   */
  describe('Test Case 1: Feature Item Count', () => {
    test('should have at least 5 feature items in the features section', () => {
      const featuresSection = document.getElementById('features');
      expect(featuresSection).toBeInTheDocument();

      const featureCards = featuresSection.querySelectorAll('.feature-card');
      expect(featureCards.length).toBeGreaterThanOrEqual(5);
    });

    test('should have exactly 6 feature cards for all key features', () => {
      const featuresSection = document.getElementById('features');
      const featureCards = featuresSection.querySelectorAll('.feature-card');
      expect(featureCards.length).toBe(6);
    });

    test('features section should exist with proper ID', () => {
      const featuresSection = document.getElementById('features');
      expect(featuresSection).toBeInTheDocument();
      expect(featuresSection.tagName.toLowerCase()).toBe('section');
    });

    test('features should use a grid layout container', () => {
      const featuresGrid = document.querySelector('.features-grid');
      expect(featuresGrid).toBeInTheDocument();
    });
  });

  /**
   * Test Case 2: Extract text content from feature items
   * Expected: Features include all 6 key features
   */
  describe('Test Case 2: Feature Content Verification', () => {
    const expectedFeatures = [
      { key: 'memcached-protocol', title: 'Memcached Protocol Compatibility' },
      { key: 'persistent-storage', title: 'Persistent Storage' },
      { key: 'lsm-tree', title: 'LSM-Tree Architecture' },
      { key: 'async-networking', title: 'Async Networking' },
      { key: 'background-compaction', title: 'Background Compaction' },
      { key: 'write-ahead-logging', title: 'Write-Ahead Logging' }
    ];

    test('should include Memcached Protocol Compatibility feature', () => {
      const feature = document.querySelector('[data-feature="memcached-protocol"]');
      expect(feature).toBeInTheDocument();
      const title = feature.querySelector('.feature-title');
      expect(title.textContent).toContain('Memcached Protocol');
    });

    test('should include Persistent Storage feature', () => {
      const feature = document.querySelector('[data-feature="persistent-storage"]');
      expect(feature).toBeInTheDocument();
      const title = feature.querySelector('.feature-title');
      expect(title.textContent).toContain('Persistent Storage');
    });

    test('should include LSM-Tree Architecture feature', () => {
      const feature = document.querySelector('[data-feature="lsm-tree"]');
      expect(feature).toBeInTheDocument();
      const title = feature.querySelector('.feature-title');
      expect(title.textContent).toContain('LSM-Tree');
    });

    test('should include Async Networking (Tokio) feature', () => {
      const feature = document.querySelector('[data-feature="async-networking"]');
      expect(feature).toBeInTheDocument();
      const title = feature.querySelector('.feature-title');
      expect(title.textContent).toContain('Async Networking');
      expect(title.textContent).toContain('Tokio');
    });

    test('should include Background Compaction feature', () => {
      const feature = document.querySelector('[data-feature="background-compaction"]');
      expect(feature).toBeInTheDocument();
      const title = feature.querySelector('.feature-title');
      expect(title.textContent).toContain('Background Compaction');
    });

    test('should include Write-Ahead Logging feature', () => {
      const feature = document.querySelector('[data-feature="write-ahead-logging"]');
      expect(feature).toBeInTheDocument();
      const title = feature.querySelector('.feature-title');
      expect(title.textContent).toContain('Write-Ahead Logging');
    });

    test('all expected features should be present', () => {
      expectedFeatures.forEach(({ key, title }) => {
        const feature = document.querySelector(`[data-feature="${key}"]`);
        expect(feature).toBeInTheDocument();
      });
    });
  });

  /**
   * Test Case 3: Check each feature item for description element
   * Expected: Each feature has both a title and a description with benefit explanation
   */
  describe('Test Case 3: Feature Title and Description Structure', () => {
    test('each feature card should have a title element', () => {
      const featureCards = document.querySelectorAll('.feature-card');
      featureCards.forEach((card, index) => {
        const title = card.querySelector('.feature-title');
        expect(title).toBeInTheDocument();
        expect(title.textContent.trim().length).toBeGreaterThan(0);
      });
    });

    test('each feature card should have a description element', () => {
      const featureCards = document.querySelectorAll('.feature-card');
      featureCards.forEach((card, index) => {
        const description = card.querySelector('.feature-description');
        expect(description).toBeInTheDocument();
        expect(description.textContent.trim().length).toBeGreaterThan(0);
      });
    });

    test('feature descriptions should be benefit-focused (contain meaningful text)', () => {
      const featureCards = document.querySelectorAll('.feature-card');
      featureCards.forEach((card) => {
        const description = card.querySelector('.feature-description');
        // Descriptions should be substantial (at least 50 characters)
        expect(description.textContent.trim().length).toBeGreaterThan(50);
      });
    });

    test('feature titles should use h3 heading tags', () => {
      const featureCards = document.querySelectorAll('.feature-card');
      featureCards.forEach((card) => {
        const title = card.querySelector('.feature-title');
        expect(title.tagName.toLowerCase()).toBe('h3');
      });
    });

    test('feature descriptions should use paragraph tags', () => {
      const featureCards = document.querySelectorAll('.feature-card');
      featureCards.forEach((card) => {
        const description = card.querySelector('.feature-description');
        expect(description.tagName.toLowerCase()).toBe('p');
      });
    });

    test('feature cards should be article elements for semantic HTML', () => {
      const featureCards = document.querySelectorAll('.feature-card');
      featureCards.forEach((card) => {
        expect(card.tagName.toLowerCase()).toBe('article');
      });
    });
  });

  /**
   * Test Case 4: Verify features section has proper heading
   * Expected: Features section has h2 heading for proper document structure
   */
  describe('Test Case 4: Section Heading Structure', () => {
    test('features section should have an h2 heading', () => {
      const featuresSection = document.getElementById('features');
      const heading = featuresSection.querySelector('h2');
      expect(heading).toBeInTheDocument();
    });

    test('features section heading should have appropriate text', () => {
      const featuresSection = document.getElementById('features');
      const heading = featuresSection.querySelector('h2');
      expect(heading.textContent.toLowerCase()).toContain('feature');
    });

    test('features section should have aria-labelledby for accessibility', () => {
      const featuresSection = document.getElementById('features');
      const labelledBy = featuresSection.getAttribute('aria-labelledby');
      expect(labelledBy).toBeTruthy();

      // The heading should exist and be referenced
      const heading = document.getElementById(labelledBy);
      expect(heading).toBeInTheDocument();
    });

    test('features section heading should have an id attribute', () => {
      const featuresSection = document.getElementById('features');
      const heading = featuresSection.querySelector('h2');
      expect(heading.id).toBeTruthy();
    });

    test('document structure should have proper heading hierarchy', () => {
      // Check that h1 exists before h2
      const h1 = document.querySelector('h1');
      const featuresH2 = document.getElementById('features').querySelector('h2');

      expect(h1).toBeInTheDocument();
      expect(featuresH2).toBeInTheDocument();

      // h1 should come before h2 in document order
      const h1Position = h1.compareDocumentPosition(featuresH2);
      expect(h1Position & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    });
  });

  /**
   * Additional tests for comprehensive coverage
   */
  describe('Additional Feature Section Tests', () => {
    test('each feature card should have an icon', () => {
      const featureCards = document.querySelectorAll('.feature-card');
      featureCards.forEach((card) => {
        const icon = card.querySelector('.feature-icon');
        expect(icon).toBeInTheDocument();
      });
    });

    test('feature icons should be decorative (aria-hidden)', () => {
      const featureCards = document.querySelectorAll('.feature-card');
      featureCards.forEach((card) => {
        const iconContainer = card.querySelector('.feature-icon');
        expect(iconContainer.getAttribute('aria-hidden')).toBe('true');
      });
    });

    test('features section should be navigable from navigation', () => {
      const navLink = document.querySelector('a[href="#features"]');
      expect(navLink).toBeInTheDocument();
    });

    test('feature descriptions should mention benefits', () => {
      // Check that descriptions include benefit-related words
      const benefitWords = ['without', 'efficiently', 'safe', 'fast', 'automatic', 'existing', 'drop-in', 'recoverable'];
      const descriptions = document.querySelectorAll('.feature-description');

      let benefitCount = 0;
      descriptions.forEach((desc) => {
        const text = desc.textContent.toLowerCase();
        benefitWords.forEach((word) => {
          if (text.includes(word)) {
            benefitCount++;
          }
        });
      });

      // At least some descriptions should contain benefit-focused language
      expect(benefitCount).toBeGreaterThan(3);
    });

    test('features section should have a subtitle/description', () => {
      const featuresSection = document.getElementById('features');
      const subtitle = featuresSection.querySelector('.section-subtitle');
      expect(subtitle).toBeInTheDocument();
      expect(subtitle.textContent.trim().length).toBeGreaterThan(0);
    });
  });
});
