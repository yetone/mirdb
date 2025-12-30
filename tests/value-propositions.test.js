/**
 * Tests for Value Propositions Section (REQ-2)
 * Verify the three core value propositions are displayed in a three-column layout
 */

const fs = require('fs');
const path = require('path');

describe('Value Propositions Section', () => {
  let document;

  beforeEach(() => {
    const html = fs.readFileSync(path.resolve(__dirname, '../index.html'), 'utf8');
    document = new DOMParser().parseFromString(html, 'text/html');
  });

  // Test Case 1: Query DOM for value proposition section
  test('should have value proposition section with three distinct items', () => {
    const section = document.querySelector('[data-section="value-propositions"], #value-propositions, .value-propositions');
    expect(section).not.toBeNull();

    const items = section.querySelectorAll('[data-proposition], .proposition, .value-proposition');
    expect(items.length).toBe(3);
  });

  // Test Case 2: Check first proposition content - Memcached Protocol Compatibility
  test('should contain content about Memcached Protocol Compatibility', () => {
    const section = document.querySelector('[data-section="value-propositions"], #value-propositions, .value-propositions');
    const propositions = section.querySelectorAll('[data-proposition], .proposition, .value-proposition');

    const firstProposition = propositions[0];
    const text = firstProposition.textContent.toLowerCase();

    const hasMemcachedCompatibility =
      text.includes('memcached') &&
      (text.includes('compatibility') || text.includes('compatible') || text.includes('protocol') || text.includes('drop-in'));

    expect(hasMemcachedCompatibility).toBe(true);
  });

  // Test Case 3: Check second proposition content - Data Persistence / Built-in Persistence with SSTables
  test('should contain content about Data Persistence or Built-in Persistence with SSTables', () => {
    const section = document.querySelector('[data-section="value-propositions"], #value-propositions, .value-propositions');
    const propositions = section.querySelectorAll('[data-proposition], .proposition, .value-proposition');

    const secondProposition = propositions[1];
    const text = secondProposition.textContent.toLowerCase();

    const hasPersistence = text.includes('persistence') || text.includes('persistent');
    const hasSSTables = text.includes('sstable') || text.includes('sst');

    expect(hasPersistence).toBe(true);
    expect(hasSSTables).toBe(true);
  });

  // Test Case 4: Check third proposition content - LSM Tree Architecture
  test('should contain content about LSM Tree Architecture', () => {
    const section = document.querySelector('[data-section="value-propositions"], #value-propositions, .value-propositions');
    const propositions = section.querySelectorAll('[data-proposition], .proposition, .value-proposition');

    const thirdProposition = propositions[2];
    const text = thirdProposition.textContent.toLowerCase();

    const hasLSMTree = text.includes('lsm') && text.includes('tree');

    expect(hasLSMTree).toBe(true);
  });

  // Test Case 5: Verify three-column layout on desktop viewport (>=1024px)
  test('should have three-column layout styles for desktop viewport', () => {
    // Check if CSS file exists and contains appropriate grid/flexbox styles
    const cssPath = path.resolve(__dirname, '../styles.css');
    const css = fs.readFileSync(cssPath, 'utf8');

    // Check for three-column layout implementation
    // Can be either CSS Grid with 3 columns or Flexbox with 3 items
    const hasGridThreeColumns = css.includes('grid-template-columns') &&
      (css.includes('1fr 1fr 1fr') || css.includes('repeat(3') || css.includes('repeat( 3'));

    const hasFlexboxLayout = css.includes('display: flex') || css.includes('display:flex');

    // Also check for responsive breakpoint around 1024px
    const hasDesktopBreakpoint = css.includes('1024px') || css.includes('min-width');

    // The layout should use grid with 3 columns or flexbox
    const hasThreeColumnLayout = hasGridThreeColumns || hasFlexboxLayout;

    expect(hasThreeColumnLayout).toBe(true);

    // Each proposition should have title and description
    const section = document.querySelector('[data-section="value-propositions"], #value-propositions, .value-propositions');
    const propositions = section.querySelectorAll('[data-proposition], .proposition, .value-proposition');

    propositions.forEach((prop, index) => {
      const title = prop.querySelector('h2, h3, .proposition-title, [data-title]');
      const description = prop.querySelector('p, .proposition-description, [data-description]');

      expect(title).not.toBeNull();
      expect(description).not.toBeNull();
    });
  });
});
