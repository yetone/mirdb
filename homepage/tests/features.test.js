/**
 * Tests for the Features Section Display
 *
 * This test suite verifies that the features section of the MirDB homepage:
 * - Displays 4-6 key product features
 * - Each feature has an icon/visual representation
 * - Each feature has a title and description
 * - Specific features are displayed correctly
 */

const { getByText, getAllByRole, queryByText, within } = require('@testing-library/dom');

describe('Features Section Display', () => {
  let document;
  let featuresSection;

  beforeEach(() => {
    // Load the actual HTML from the homepage
    const html = global.getIndexHTML();
    global.loadHTML(global.extractBodyContent(html));

    // Get the features section
    featuresSection = window.document.getElementById('features') ||
                      window.document.querySelector('.features-section') ||
                      window.document.querySelector('[data-testid="features-section"]');
  });

  // Test Case 1: Count feature items in features section (4-6 features)
  describe('Test Case 1: Feature Count', () => {
    test('should display between 4 and 6 feature items', () => {
      expect(featuresSection).toBeTruthy();

      // Find all feature cards/items within the features section
      const featureItems = featuresSection.querySelectorAll('.feature-card, .feature-item, [data-testid="feature-item"]');

      expect(featureItems.length).toBeGreaterThanOrEqual(4);
      expect(featureItems.length).toBeLessThanOrEqual(6);
    });
  });

  // Test Case 2: Check for 'Memcached Compatible' feature
  describe('Test Case 2: Memcached Compatible Feature', () => {
    test('should have feature card with title containing "Memcached" and description about drop-in replacement', () => {
      expect(featuresSection).toBeTruthy();

      // Find feature containing Memcached
      const memcachedFeature = Array.from(
        featuresSection.querySelectorAll('.feature-card, .feature-item, [data-testid="feature-item"]')
      ).find(el => el.textContent.toLowerCase().includes('memcached'));

      expect(memcachedFeature).toBeTruthy();

      // Check for title with Memcached
      const title = memcachedFeature.querySelector('.feature-title, h3, [data-testid="feature-title"]');
      expect(title).toBeTruthy();
      expect(title.textContent.toLowerCase()).toContain('memcached');

      // Check for description mentioning drop-in replacement
      const description = memcachedFeature.querySelector('.feature-description, p, [data-testid="feature-description"]');
      expect(description).toBeTruthy();
      expect(description.textContent.toLowerCase()).toMatch(/drop-in|replacement|compatible|clients/);
    });
  });

  // Test Case 3: Check for 'Persistent Storage' feature
  describe('Test Case 3: Persistent Storage Feature', () => {
    test('should have feature card mentioning SSTable-based disk persistence', () => {
      expect(featuresSection).toBeTruthy();

      // Find feature containing Persistent Storage
      const persistentFeature = Array.from(
        featuresSection.querySelectorAll('.feature-card, .feature-item, [data-testid="feature-item"]')
      ).find(el => el.textContent.toLowerCase().includes('persistent'));

      expect(persistentFeature).toBeTruthy();

      // Check for title with Persistent
      const title = persistentFeature.querySelector('.feature-title, h3, [data-testid="feature-title"]');
      expect(title).toBeTruthy();
      expect(title.textContent.toLowerCase()).toContain('persistent');

      // Check for description mentioning SSTable or disk persistence
      const description = persistentFeature.querySelector('.feature-description, p, [data-testid="feature-description"]');
      expect(description).toBeTruthy();
      expect(description.textContent.toLowerCase()).toMatch(/sstable|disk|persistence|durable/);
    });
  });

  // Test Case 4: Verify all features have icons
  describe('Test Case 4: Feature Icons', () => {
    test('each feature item should contain an img or svg element', () => {
      expect(featuresSection).toBeTruthy();

      const featureItems = featuresSection.querySelectorAll('.feature-card, .feature-item, [data-testid="feature-item"]');

      expect(featureItems.length).toBeGreaterThan(0);

      featureItems.forEach((item, index) => {
        const hasIcon = item.querySelector('img, svg, .feature-icon, [data-testid="feature-icon"]');
        expect(hasIcon).toBeTruthy();
      });
    });
  });

  // Test Case 5: Check for 'High Performance' feature
  describe('Test Case 5: High Performance Feature', () => {
    test('should have feature card mentioning Rust and/or Tokio async I/O', () => {
      expect(featuresSection).toBeTruthy();

      // Find feature containing Performance
      const performanceFeature = Array.from(
        featuresSection.querySelectorAll('.feature-card, .feature-item, [data-testid="feature-item"]')
      ).find(el => el.textContent.toLowerCase().includes('performance'));

      expect(performanceFeature).toBeTruthy();

      // Check for title with Performance
      const title = performanceFeature.querySelector('.feature-title, h3, [data-testid="feature-title"]');
      expect(title).toBeTruthy();
      expect(title.textContent.toLowerCase()).toContain('performance');

      // Check for description mentioning Rust or Tokio or async
      const description = performanceFeature.querySelector('.feature-description, p, [data-testid="feature-description"]');
      expect(description).toBeTruthy();
      expect(description.textContent.toLowerCase()).toMatch(/rust|tokio|async|i\/o|fast/i);
    });
  });

  // Additional test: Verify features section structure
  describe('Features Section Structure', () => {
    test('should have a features section with proper container', () => {
      expect(featuresSection).toBeTruthy();
      expect(featuresSection.tagName).toBe('SECTION');
    });

    test('each feature should have both title and description', () => {
      const featureItems = featuresSection.querySelectorAll('.feature-card, .feature-item, [data-testid="feature-item"]');

      featureItems.forEach((item) => {
        const title = item.querySelector('.feature-title, h3, [data-testid="feature-title"]');
        const description = item.querySelector('.feature-description, p, [data-testid="feature-description"]');

        expect(title).toBeTruthy();
        expect(title.textContent.trim().length).toBeGreaterThan(0);
        expect(description).toBeTruthy();
        expect(description.textContent.trim().length).toBeGreaterThan(0);
      });
    });
  });
});
