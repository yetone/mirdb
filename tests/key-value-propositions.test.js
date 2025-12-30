/**
 * E2E Tests for Key Value Propositions Display (Scenario 2, REQ-2)
 *
 * These tests verify that key value propositions (memcached compatibility,
 * persistence, performance) are clearly presented as specified in REQ-2
 */

const fs = require('fs');
const path = require('path');

describe('Key Value Propositions Display - REQ-2', () => {
  let document;
  let htmlContent;

  beforeAll(() => {
    // Load the HTML file
    const htmlPath = path.join(__dirname, '..', 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Parse HTML using jsdom
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  describe('Features Section Structure', () => {
    test('should have a features section with id "features"', () => {
      const featuresSection = document.getElementById('features');
      expect(featuresSection).toBeTruthy();
    });

    test('should have a section title for Key Features', () => {
      const featuresSection = document.getElementById('features');
      const sectionTitle = featuresSection.querySelector('.section-title');
      expect(sectionTitle).toBeTruthy();
      expect(sectionTitle.textContent).toContain('Key Features');
    });

    test('should have a features grid container', () => {
      const featuresSection = document.getElementById('features');
      const featuresGrid = featuresSection.querySelector('.features-grid');
      expect(featuresGrid).toBeTruthy();
    });
  });

  describe('Test Case 1: Memcached Compatibility Feature Display', () => {
    /**
     * Test Case ID: 1
     * Input: Inspect features section for memcached compatibility
     * Expected: Feature card/section clearly states memcached protocol support and client compatibility
     */
    test('should display memcached feature card with data-testid', () => {
      const memcachedFeature = document.querySelector('[data-testid="feature-memcached"]');
      expect(memcachedFeature).toBeTruthy();
    });

    test('should have memcached feature title mentioning protocol compatibility', () => {
      const memcachedFeature = document.querySelector('[data-testid="feature-memcached"]');
      const title = memcachedFeature.querySelector('h3');
      expect(title).toBeTruthy();
      expect(title.textContent.toLowerCase()).toContain('memcached');
      expect(title.textContent.toLowerCase()).toMatch(/protocol|compatible/);
    });

    test('should have memcached feature description mentioning client compatibility', () => {
      const memcachedFeature = document.querySelector('[data-testid="feature-memcached"]');
      const description = memcachedFeature.querySelector('p');
      expect(description).toBeTruthy();
      expect(description.textContent.toLowerCase()).toContain('memcached');
      expect(description.textContent.toLowerCase()).toContain('client');
    });

    test('should mention drop-in replacement for existing memcached clients', () => {
      const memcachedFeature = document.querySelector('[data-testid="feature-memcached"]');
      const description = memcachedFeature.querySelector('p');
      expect(description.textContent.toLowerCase()).toMatch(/drop-in replacement|existing.*client/);
    });

    test('should have an icon for memcached feature', () => {
      const memcachedFeature = document.querySelector('[data-testid="feature-memcached"]');
      const icon = memcachedFeature.querySelector('.feature-icon svg');
      expect(icon).toBeTruthy();
    });
  });

  describe('Test Case 2: Data Persistence with LSM Tree Feature Display', () => {
    /**
     * Test Case ID: 2
     * Input: Inspect features section for persistence
     * Expected: Feature card/section clearly states data persistence with LSM tree architecture
     */
    test('should display persistence feature card with data-testid', () => {
      const persistenceFeature = document.querySelector('[data-testid="feature-persistence"]');
      expect(persistenceFeature).toBeTruthy();
    });

    test('should have persistence feature title mentioning LSM', () => {
      const persistenceFeature = document.querySelector('[data-testid="feature-persistence"]');
      const title = persistenceFeature.querySelector('h3');
      expect(title).toBeTruthy();
      expect(title.textContent.toLowerCase()).toMatch(/persistent|storage/);
      expect(title.textContent).toContain('LSM');
    });

    test('should have persistence feature description explaining LSM tree architecture', () => {
      const persistenceFeature = document.querySelector('[data-testid="feature-persistence"]');
      const description = persistenceFeature.querySelector('p');
      expect(description).toBeTruthy();
      expect(description.textContent).toContain('LSM');
    });

    test('should mention durability features (WAL or survives restarts)', () => {
      const persistenceFeature = document.querySelector('[data-testid="feature-persistence"]');
      const description = persistenceFeature.querySelector('p');
      expect(description.textContent.toLowerCase()).toMatch(/durability|write-ahead|survives|wal/);
    });

    test('should have an icon for persistence feature', () => {
      const persistenceFeature = document.querySelector('[data-testid="feature-persistence"]');
      const icon = persistenceFeature.querySelector('.feature-icon svg');
      expect(icon).toBeTruthy();
    });
  });

  describe('Test Case 3: Performance Feature with Rust and Tokio Display', () => {
    /**
     * Test Case ID: 3
     * Input: Inspect features section for performance
     * Expected: Feature card/section highlights Rust implementation and Tokio async networking
     */
    test('should display performance feature card with data-testid', () => {
      const performanceFeature = document.querySelector('[data-testid="feature-performance"]');
      expect(performanceFeature).toBeTruthy();
    });

    test('should have performance feature title mentioning performance', () => {
      const performanceFeature = document.querySelector('[data-testid="feature-performance"]');
      const title = performanceFeature.querySelector('h3');
      expect(title).toBeTruthy();
      expect(title.textContent.toLowerCase()).toContain('performance');
    });

    test('should have performance feature description mentioning Rust', () => {
      const performanceFeature = document.querySelector('[data-testid="feature-performance"]');
      const description = performanceFeature.querySelector('p');
      expect(description).toBeTruthy();
      expect(description.textContent).toContain('Rust');
    });

    test('should have performance feature description mentioning Tokio async networking', () => {
      const performanceFeature = document.querySelector('[data-testid="feature-performance"]');
      const description = performanceFeature.querySelector('p');
      expect(description.textContent).toContain('Tokio');
      expect(description.textContent.toLowerCase()).toContain('async');
    });

    test('should have an icon for performance feature', () => {
      const performanceFeature = document.querySelector('[data-testid="feature-performance"]');
      const icon = performanceFeature.querySelector('.feature-icon svg');
      expect(icon).toBeTruthy();
    });
  });

  describe('Test Case 4: Features Grid Layout with Visual Indicators', () => {
    /**
     * Test Case ID: 4
     * Input: Verify features grid layout
     * Expected: Features are displayed in a visually appealing grid format with icons or visual indicators
     */
    test('should have features displayed in a grid layout', () => {
      const featuresGrid = document.querySelector('.features-grid');
      expect(featuresGrid).toBeTruthy();
    });

    test('should have multiple feature cards in the grid', () => {
      const featuresGrid = document.querySelector('.features-grid');
      const featureCards = featuresGrid.querySelectorAll('.feature-card');
      expect(featureCards.length).toBeGreaterThanOrEqual(3);
    });

    test('should have all three key value proposition features present', () => {
      const memcachedFeature = document.querySelector('[data-testid="feature-memcached"]');
      const persistenceFeature = document.querySelector('[data-testid="feature-persistence"]');
      const performanceFeature = document.querySelector('[data-testid="feature-performance"]');

      expect(memcachedFeature).toBeTruthy();
      expect(persistenceFeature).toBeTruthy();
      expect(performanceFeature).toBeTruthy();
    });

    test('should have icons (visual indicators) for each feature card', () => {
      const featuresGrid = document.querySelector('.features-grid');
      const featureCards = featuresGrid.querySelectorAll('.feature-card');

      featureCards.forEach((card) => {
        const icon = card.querySelector('.feature-icon svg');
        expect(icon).toBeTruthy();
      });
    });

    test('each feature card should have a title and description', () => {
      const featuresGrid = document.querySelector('.features-grid');
      const featureCards = featuresGrid.querySelectorAll('.feature-card');

      featureCards.forEach((card) => {
        const title = card.querySelector('h3');
        const description = card.querySelector('p');
        expect(title).toBeTruthy();
        expect(description).toBeTruthy();
        expect(title.textContent.length).toBeGreaterThan(0);
        expect(description.textContent.length).toBeGreaterThan(0);
      });
    });
  });

  describe('Navigation to Features Section', () => {
    test('should have a navigation link to features section', () => {
      const featuresLink = document.querySelector('nav a[href="#features"]');
      expect(featuresLink).toBeTruthy();
    });

    test('navigation link should contain "Features" text', () => {
      const featuresLink = document.querySelector('nav a[href="#features"]');
      expect(featuresLink.textContent).toContain('Features');
    });
  });
});
