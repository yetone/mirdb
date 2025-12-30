/**
 * Tests for Feature Comparison Section (REQ-7)
 * Verify feature comparison highlighting MirDB vs standard memcached
 */

const fs = require('fs');
const path = require('path');

describe('Feature Comparison Section', () => {
  let document;

  beforeEach(() => {
    const html = fs.readFileSync(path.resolve(__dirname, '../index.html'), 'utf8');
    document = new DOMParser().parseFromString(html, 'text/html');
  });

  // Test Case 1: Section exists comparing MirDB features to standard memcached
  test('should have a comparison section comparing MirDB features to standard memcached', () => {
    // Look for feature comparison section using various selectors
    const section = document.querySelector(
      '[data-section="feature-comparison"], ' +
      '#feature-comparison, ' +
      '.feature-comparison, ' +
      'section[id*="comparison"], ' +
      'section[class*="comparison"]'
    );

    expect(section).not.toBeNull();

    // Verify section contains comparison content (both MirDB and memcached mentioned)
    const sectionText = section.textContent.toLowerCase();
    const hasMirDB = sectionText.includes('mirdb');
    const hasMemcached = sectionText.includes('memcached');

    expect(hasMirDB).toBe(true);
    expect(hasMemcached).toBe(true);
  });

  // Test Case 2: Comparison highlights that MirDB has persistence while standard memcached does not
  test('should highlight persistence as a key differentiator between MirDB and memcached', () => {
    const section = document.querySelector(
      '[data-section="feature-comparison"], ' +
      '#feature-comparison, ' +
      '.feature-comparison, ' +
      'section[id*="comparison"], ' +
      'section[class*="comparison"]'
    );

    expect(section).not.toBeNull();

    const sectionText = section.textContent.toLowerCase();

    // Check for persistence being mentioned as a differentiator
    const hasPersistence = sectionText.includes('persistence') || sectionText.includes('persistent');
    expect(hasPersistence).toBe(true);

    // Verify the comparison table or content structure exists
    // Could be a table, or comparison items with data attributes
    const comparisonItems = section.querySelectorAll(
      '[data-feature], ' +
      '.comparison-row, ' +
      '.feature-row, ' +
      'tr, ' +
      '.comparison-item'
    );

    // Should have at least one comparison item/row
    expect(comparisonItems.length).toBeGreaterThan(0);

    // Find persistence-related comparison
    let foundPersistenceComparison = false;
    comparisonItems.forEach(item => {
      const text = item.textContent.toLowerCase();
      if (text.includes('persistence') || text.includes('persistent')) {
        foundPersistenceComparison = true;
      }
    });

    expect(foundPersistenceComparison).toBe(true);
  });

  // Test Case 3: Comparison indicates MirDB is compatible with memcached protocol/clients
  test('should indicate MirDB is compatible with memcached protocol/clients', () => {
    const section = document.querySelector(
      '[data-section="feature-comparison"], ' +
      '#feature-comparison, ' +
      '.feature-comparison, ' +
      'section[id*="comparison"], ' +
      'section[class*="comparison"]'
    );

    expect(section).not.toBeNull();

    const sectionText = section.textContent.toLowerCase();

    // Check for protocol compatibility claims
    const hasProtocolCompatibility =
      (sectionText.includes('protocol') && sectionText.includes('compatible')) ||
      (sectionText.includes('protocol') && sectionText.includes('compatibility')) ||
      sectionText.includes('memcached protocol') ||
      sectionText.includes('drop-in') ||
      (sectionText.includes('client') && sectionText.includes('compatible'));

    expect(hasProtocolCompatibility).toBe(true);
  });
});
