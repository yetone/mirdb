/**
 * Tests for Responsive Design - Desktop (NFR-1)
 * Verify page displays correctly on desktop viewports (1280px+ width)
 */

const fs = require('fs');
const path = require('path');

describe('Responsive Design - Desktop', () => {
  let document;
  let css;

  beforeAll(() => {
    const html = fs.readFileSync(path.resolve(__dirname, '../index.html'), 'utf8');
    document = new DOMParser().parseFromString(html, 'text/html');
    css = fs.readFileSync(path.resolve(__dirname, '../styles.css'), 'utf8');
  });

  // Test Case 1: Load page at 1280px viewport width
  // Full desktop layout displays correctly with three-column value propositions
  describe('Test Case 1: Full desktop layout at 1280px viewport width', () => {
    test('should have viewport meta tag for proper rendering', () => {
      const viewportMeta = document.querySelector('meta[name="viewport"]');
      expect(viewportMeta).not.toBeNull();
      expect(viewportMeta.getAttribute('content')).toContain('width=device-width');
      expect(viewportMeta.getAttribute('content')).toContain('initial-scale=1');
    });

    test('should have container with max-width for desktop', () => {
      // Container should have a max-width to constrain content
      expect(css).toMatch(/\.container[\s\S]*?max-width/);
      // Check for 1200px max-width
      expect(css).toMatch(/\.container\s*\{[^}]*max-width:\s*1200px[^}]*\}/);
    });

    test('should have three-column grid layout for value propositions at desktop breakpoint', () => {
      // Check that 3-column grid is applied at min-width: 1024px (which includes 1280px)
      const hasDesktopGrid = css.match(/@media\s*\(\s*min-width:\s*1024px\s*\)\s*\{[\s\S]*?\.propositions-grid[\s\S]*?grid-template-columns:\s*1fr\s+1fr\s+1fr/);
      expect(hasDesktopGrid).toBeTruthy();
    });

    test('should have three value proposition cards', () => {
      const propositions = document.querySelectorAll('.proposition, [data-proposition]');
      expect(propositions.length).toBe(3);
    });

    test('should have value propositions section properly structured', () => {
      const section = document.querySelector('.value-propositions, #value-propositions, [data-section="value-propositions"]');
      expect(section).not.toBeNull();

      const grid = document.querySelector('.propositions-grid');
      expect(grid).not.toBeNull();
    });

    test('should have hero section displaying at full width', () => {
      const hero = document.querySelector('.hero, header.hero');
      expect(hero).not.toBeNull();

      // Hero should have text-align: center
      expect(css).toMatch(/\.hero\s*\{[^}]*text-align:\s*center[^}]*\}/);
    });

    test('should have all main sections visible', () => {
      // Check for key sections
      const heroSection = document.querySelector('.hero');
      const valuePropsSection = document.querySelector('.value-propositions');
      const commandsSection = document.querySelector('.commands-section, #commands');
      const comparisonSection = document.querySelector('.feature-comparison, #feature-comparison');
      const gettingStartedSection = document.querySelector('.getting-started, #getting-started');
      const clientConnectionSection = document.querySelector('.client-connection, #client-connection');
      const footer = document.querySelector('.footer, footer');

      expect(heroSection).not.toBeNull();
      expect(valuePropsSection).not.toBeNull();
      expect(commandsSection).not.toBeNull();
      expect(comparisonSection).not.toBeNull();
      expect(gettingStartedSection).not.toBeNull();
      expect(clientConnectionSection).not.toBeNull();
      expect(footer).not.toBeNull();
    });

    test('should have desktop hero font size', () => {
      // Hero h1 should have larger font size on desktop (3.5rem by default)
      expect(css).toMatch(/\.hero\s+h1\s*\{[^}]*font-size:\s*3\.5rem[^}]*\}/);
    });
  });

  // Test Case 2: Check max-width constraints
  // Content has appropriate max-width, not stretching across ultra-wide screens
  describe('Test Case 2: Max-width constraints for content', () => {
    test('should have container max-width of 1200px', () => {
      // Container should have a reasonable max-width
      expect(css).toMatch(/\.container\s*\{[^}]*max-width:\s*1200px[^}]*\}/);
    });

    test('should have container centered with auto margins', () => {
      // Container should have margin: 0 auto for centering
      expect(css).toMatch(/\.container\s*\{[^}]*margin:\s*0\s+auto[^}]*\}/);
    });

    test('should have section description with max-width for readability', () => {
      // Section descriptions should have max-width for optimal line length
      expect(css).toMatch(/\.section-description\s*\{[^}]*max-width/);
    });

    test('should have comparison table within wrapper', () => {
      const tableWrapper = document.querySelector('.comparison-table-wrapper');
      expect(tableWrapper).not.toBeNull();

      const table = tableWrapper.querySelector('.comparison-table');
      expect(table).not.toBeNull();
    });

    test('should have table with 100% width within container', () => {
      // Table should be 100% width of its container
      expect(css).toMatch(/\.comparison-table\s*\{[^}]*width:\s*100%[^}]*\}/);
    });

    test('should have proposition cards with equal widths in grid', () => {
      // At desktop, each proposition should be 1fr (equal width)
      const gridTemplate = css.match(/@media\s*\(\s*min-width:\s*1024px\s*\)[\s\S]*?grid-template-columns:\s*1fr\s+1fr\s+1fr/);
      expect(gridTemplate).toBeTruthy();
    });
  });

  // Test Case 3: Check at 1920px viewport
  // Layout remains centered and readable at large viewport
  describe('Test Case 3: Layout at 1920px (large) viewport', () => {
    test('should keep content centered at wide viewports via container max-width', () => {
      // Max-width of 1200px ensures content doesn't stretch beyond that
      expect(css).toMatch(/\.container\s*\{[^}]*max-width:\s*1200px[^}]*\}/);
      expect(css).toMatch(/\.container\s*\{[^}]*margin:\s*0\s+auto[^}]*\}/);
    });

    test('should have horizontal padding on container', () => {
      // Container should have horizontal padding
      expect(css).toMatch(/\.container\s*\{[^}]*padding:\s*0\s+1\.5rem[^}]*\}/);
    });

    test('should maintain readable line lengths for text content', () => {
      // Section description has max-width for readability
      const sectionDescMaxWidth = css.match(/\.section-description\s*\{[^}]*max-width:\s*\d+px[^}]*\}/);
      expect(sectionDescMaxWidth).toBeTruthy();
    });

    test('should have commands grid at 4-column layout on desktop', () => {
      // At 1024px+, commands grid should show 4 columns
      const has4ColGrid = css.match(/@media\s*\(\s*min-width:\s*1024px\s*\)[\s\S]*?\.commands-grid[\s\S]*?grid-template-columns:\s*repeat\s*\(\s*4/);
      expect(has4ColGrid).toBeTruthy();
    });

    test('should have client examples in 3-column layout on desktop', () => {
      // At 1024px+, client examples should show 3 columns
      const has3ColClients = css.match(/@media\s*\(\s*min-width:\s*1024px\s*\)[\s\S]*?\.client-examples-grid[\s\S]*?grid-template-columns:\s*repeat\s*\(\s*3/);
      expect(has3ColClients).toBeTruthy();
    });

    test('should have section padding appropriate for desktop', () => {
      // Sections should have generous vertical padding
      expect(css).toMatch(/section\s*\{[^}]*padding:\s*4rem\s+0[^}]*\}/);
    });

    test('should have hero section with appropriate padding for desktop', () => {
      // Hero should have generous padding on desktop
      expect(css).toMatch(/\.hero\s*\{[^}]*padding:\s*4rem\s+0[^}]*\}/);
    });
  });

  // Additional desktop layout tests
  describe('Additional Desktop Layout Verification', () => {
    test('should have footer with centered content', () => {
      const footer = document.querySelector('.footer, footer');
      expect(footer).not.toBeNull();

      // Footer should be centered
      expect(css).toMatch(/\.footer\s*\{[^}]*text-align:\s*center[^}]*\}/);
    });

    test('should have proper spacing between sections', () => {
      // Gap between grid items
      expect(css).toMatch(/\.propositions-grid[\s\S]*?gap:\s*2rem/);
    });

    test('should have consistent button sizing on desktop', () => {
      const buttons = document.querySelectorAll('.btn');
      expect(buttons.length).toBeGreaterThan(0);

      // Button padding
      expect(css).toMatch(/\.btn\s*\{[^}]*padding:\s*0\.75rem\s+1\.5rem[^}]*\}/);
    });

    test('should have step cards in getting-started section', () => {
      const steps = document.querySelectorAll('.step');
      expect(steps.length).toBeGreaterThanOrEqual(4);
    });

    test('should have feature comparison table fully visible on desktop', () => {
      const comparisonTable = document.querySelector('.comparison-table');
      expect(comparisonTable).not.toBeNull();

      const tableRows = comparisonTable.querySelectorAll('tbody tr');
      expect(tableRows.length).toBeGreaterThanOrEqual(5);
    });
  });
});
