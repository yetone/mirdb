/**
 * DOM Structure Tests
 * Owner: Multiple scenarios - each adds tests for their section
 *
 * Test groups:
 * - Hero section elements (Scenario 1)
 * - Features grid elements (Scenario 2)
 * - Terminal demo elements (Scenario 3)
 * - Quick-start elements (Scenario 4)
 * - Navigation elements (Scenario 5)
 * - Architecture elements (Scenario 6)
 */

const fs = require('fs');
const path = require('path');

// Read the HTML file
const htmlPath = path.join(__dirname, '../../index.html');
const htmlContent = fs.readFileSync(htmlPath, 'utf8');

// Set up DOM
document.documentElement.innerHTML = htmlContent;

/**
 * Features Grid Section Tests (Scenario 2)
 */
describe('Features Grid Section', () => {
  let featuresSection;
  let featureCards;

  beforeAll(() => {
    featuresSection = document.getElementById('features');
    featureCards = document.querySelectorAll('.feature-card');
  });

  // Test Case 1: Exactly 4 feature cards are present in the DOM
  test('should have exactly 4 feature cards', () => {
    expect(featureCards.length).toBe(4);
  });

  // Test Case 2: Feature card with 'Tokio' or 'async' in title/description exists
  test('should have a Tokio async feature card', () => {
    const tokioCard = Array.from(featureCards).find(card => {
      const title = card.querySelector('.feature-card__title')?.textContent || '';
      const description = card.querySelector('.feature-card__description')?.textContent || '';
      return (
        title.toLowerCase().includes('tokio') ||
        title.toLowerCase().includes('async') ||
        description.toLowerCase().includes('tokio') ||
        description.toLowerCase().includes('async')
      );
    });
    expect(tokioCard).toBeTruthy();
  });

  // Test Case 3: Feature card describing Memcached protocol compatibility exists
  test('should have a Memcached protocol feature card', () => {
    const memcachedCard = Array.from(featureCards).find(card => {
      const title = card.querySelector('.feature-card__title')?.textContent || '';
      const description = card.querySelector('.feature-card__description')?.textContent || '';
      return (
        title.toLowerCase().includes('memcached') ||
        description.toLowerCase().includes('memcached')
      );
    });
    expect(memcachedCard).toBeTruthy();
  });

  // Test Case 4: Feature card describing Skip-list memtable exists
  test('should have a Skip-list memtable feature card', () => {
    const skipListCard = Array.from(featureCards).find(card => {
      const title = card.querySelector('.feature-card__title')?.textContent || '';
      const description = card.querySelector('.feature-card__description')?.textContent || '';
      return (
        title.toLowerCase().includes('skip-list') ||
        title.toLowerCase().includes('skiplist') ||
        description.toLowerCase().includes('skip-list') ||
        description.toLowerCase().includes('skiplist')
      );
    });
    expect(skipListCard).toBeTruthy();
  });

  // Test Case 5: Feature card describing compaction (minor/major) exists
  test('should have a Compaction feature card', () => {
    const compactionCard = Array.from(featureCards).find(card => {
      const title = card.querySelector('.feature-card__title')?.textContent || '';
      const description = card.querySelector('.feature-card__description')?.textContent || '';
      return (
        title.toLowerCase().includes('compaction') ||
        description.toLowerCase().includes('compaction') ||
        (description.toLowerCase().includes('minor') && description.toLowerCase().includes('major'))
      );
    });
    expect(compactionCard).toBeTruthy();
  });

  // Test Case 6: Each feature card contains an SVG icon element
  test('should have an SVG icon in each feature card', () => {
    featureCards.forEach((card, index) => {
      const iconContainer = card.querySelector('.feature-card__icon');
      expect(iconContainer).toBeTruthy();

      const svg = iconContainer?.querySelector('svg');
      expect(svg).toBeTruthy();
    });
  });

  // Additional structural tests
  test('features section should exist', () => {
    expect(featuresSection).toBeTruthy();
  });

  test('features section should have a title', () => {
    const title = featuresSection.querySelector('.section__title, h2');
    expect(title).toBeTruthy();
    expect(title.textContent.toLowerCase()).toContain('feature');
  });

  test('features grid container should exist', () => {
    const grid = featuresSection.querySelector('.features__grid');
    expect(grid).toBeTruthy();
  });

  test('each feature card should have a title', () => {
    featureCards.forEach(card => {
      const title = card.querySelector('.feature-card__title');
      expect(title).toBeTruthy();
      expect(title.textContent.trim().length).toBeGreaterThan(0);
    });
  });

  test('each feature card should have a description', () => {
    featureCards.forEach(card => {
      const description = card.querySelector('.feature-card__description');
      expect(description).toBeTruthy();
      expect(description.textContent.trim().length).toBeGreaterThan(0);
    });
  });
});
