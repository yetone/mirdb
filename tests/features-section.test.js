/**
 * Core Features Section Tests
 * Scenario: Verify that the features section highlights memcached compatibility,
 * persistence, and LSM tree architecture (REQ-2)
 */

const fs = require('fs');
const path = require('path');

describe('Core Features Section', () => {
  let document;

  beforeAll(() => {
    // Load the HTML file
    const html = fs.readFileSync(path.join(__dirname, '../index.html'), 'utf8');
    document = new DOMParser().parseFromString(html, 'text/html');
  });

  // Test Case 1: Features section exists with appropriate heading
  describe('Test Case 1: Features section element', () => {
    test('Features section exists with appropriate heading', () => {
      const featuresSection = document.getElementById('features');
      expect(featuresSection).not.toBeNull();
      expect(featuresSection.tagName.toLowerCase()).toBe('section');

      // Check for aria-labelledby attribute
      expect(featuresSection.getAttribute('aria-labelledby')).toBe('features-heading');

      // Check for heading
      const heading = document.getElementById('features-heading');
      expect(heading).not.toBeNull();
      expect(heading.tagName.toLowerCase()).toBe('h2');
      expect(heading.textContent.toLowerCase()).toContain('feature');
    });
  });

  // Test Case 2: Memcached compatibility feature
  describe('Test Case 2: Memcached compatibility feature', () => {
    test('Feature card/block exists mentioning memcached protocol compatibility', () => {
      const featuresSection = document.getElementById('features');
      expect(featuresSection).not.toBeNull();

      // Look for memcached feature card
      const memcachedFeature = featuresSection.querySelector('[data-feature="memcached"]');
      expect(memcachedFeature).not.toBeNull();

      // Check for title mentioning memcached
      const title = memcachedFeature.querySelector('h3');
      expect(title).not.toBeNull();
      expect(title.textContent.toLowerCase()).toContain('memcached');

      // Check for description mentioning protocol compatibility
      const description = memcachedFeature.querySelector('p');
      expect(description).not.toBeNull();
      const descText = description.textContent.toLowerCase();
      expect(descText).toMatch(/protocol|compatible|compatibility/);
    });
  });

  // Test Case 3: Persistence feature
  describe('Test Case 3: Persistence feature', () => {
    test('Feature card/block exists mentioning persistent storage, SSTables, or data durability', () => {
      const featuresSection = document.getElementById('features');
      expect(featuresSection).not.toBeNull();

      // Look for persistence feature card
      const persistenceFeature = featuresSection.querySelector('[data-feature="persistence"]');
      expect(persistenceFeature).not.toBeNull();

      // Check for title mentioning persistence/storage
      const title = persistenceFeature.querySelector('h3');
      expect(title).not.toBeNull();
      const titleText = title.textContent.toLowerCase();
      expect(titleText).toMatch(/persist|storage/);

      // Check for description mentioning SSTables or durability
      const description = persistenceFeature.querySelector('p');
      expect(description).not.toBeNull();
      const descText = description.textContent.toLowerCase();
      expect(descText).toMatch(/sstable|durability|persist|disk|survives/);
    });
  });

  // Test Case 4: Performance feature with LSM tree
  describe('Test Case 4: Performance feature', () => {
    test('Feature card/block exists mentioning LSM tree, high performance, or async I/O', () => {
      const featuresSection = document.getElementById('features');
      expect(featuresSection).not.toBeNull();

      // Look for performance feature card
      const performanceFeature = featuresSection.querySelector('[data-feature="performance"]');
      expect(performanceFeature).not.toBeNull();

      // Check for title mentioning performance
      const title = performanceFeature.querySelector('h3');
      expect(title).not.toBeNull();
      const titleText = title.textContent.toLowerCase();
      expect(titleText).toMatch(/performance|fast|speed/);

      // Check for description mentioning LSM tree or async I/O
      const description = performanceFeature.querySelector('p');
      expect(description).not.toBeNull();
      const descText = description.textContent.toLowerCase();
      expect(descText).toMatch(/lsm|async|tokio|merge-tree|merge tree/);
    });
  });

  // Test Case 5: Feature count
  describe('Test Case 5: Feature count in section', () => {
    test('At least 3 feature elements are present in the features section', () => {
      const featuresSection = document.getElementById('features');
      expect(featuresSection).not.toBeNull();

      // Count feature cards
      const featureCards = featuresSection.querySelectorAll('.feature-card');
      expect(featureCards.length).toBeGreaterThanOrEqual(3);

      // Alternative: count by data-feature attribute
      const dataFeatureElements = featuresSection.querySelectorAll('[data-feature]');
      expect(dataFeatureElements.length).toBeGreaterThanOrEqual(3);
    });
  });

  // Additional validation: Three-column layout classes exist
  describe('Additional: Layout verification', () => {
    test('Features grid has proper structure for three-column layout', () => {
      const featuresSection = document.getElementById('features');
      expect(featuresSection).not.toBeNull();

      // Check for features-grid container
      const featuresGrid = featuresSection.querySelector('.features-grid');
      expect(featuresGrid).not.toBeNull();

      // Verify all feature cards are within the grid
      const featureCards = featuresGrid.querySelectorAll('.feature-card');
      expect(featureCards.length).toBe(3);
    });
  });
});
