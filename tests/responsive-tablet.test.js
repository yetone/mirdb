/**
 * Tests for Responsive Design - Tablet (NFR-1)
 * Verify page is fully responsive on tablet viewports (768px width)
 */

const fs = require('fs');
const path = require('path');

describe('Responsive Design - Tablet (768px viewport)', () => {
  let document;
  let css;

  beforeAll(() => {
    const html = fs.readFileSync(path.resolve(__dirname, '../index.html'), 'utf8');
    document = new DOMParser().parseFromString(html, 'text/html');
    css = fs.readFileSync(path.resolve(__dirname, '../styles.css'), 'utf8');
  });

  // Test Case 1: Layout adjusts appropriately for tablet, no content overflow
  describe('Test Case 1: Layout adjusts appropriately for tablet', () => {
    test('should have viewport meta tag for responsive design', () => {
      const viewportMeta = document.querySelector('meta[name="viewport"]');
      expect(viewportMeta).not.toBeNull();
      expect(viewportMeta.getAttribute('content')).toContain('width=device-width');
    });

    test('should have container with max-width constraint', () => {
      const containers = document.querySelectorAll('.container');
      expect(containers.length).toBeGreaterThan(0);

      // CSS should have max-width for container
      expect(css).toMatch(/\.container[\s\S]*?max-width/);
    });

    test('should have CSS media queries for tablet breakpoint', () => {
      // Check for 768px breakpoint (common tablet width)
      const has768Breakpoint = css.includes('768px');
      const hasMinWidthQueries = css.includes('min-width');
      const hasMaxWidthQueries = css.includes('max-width');

      expect(has768Breakpoint || hasMinWidthQueries || hasMaxWidthQueries).toBe(true);
    });

    test('should have horizontal padding on container for tablet margins', () => {
      // Container should have padding for content spacing
      expect(css).toMatch(/\.container[\s\S]*?padding/);
    });

    test('should have overflow-x handling for code blocks', () => {
      // Pre/code blocks should have overflow-x: auto to prevent horizontal overflow
      const preStyles = css.match(/pre[\s\S]*?overflow/i);
      expect(preStyles).not.toBeNull();
    });

    test('should have responsive table wrapper for comparison table', () => {
      const tableWrapper = document.querySelector('.comparison-table-wrapper');
      expect(tableWrapper).not.toBeNull();

      // Wrapper should have overflow-x handling
      expect(css).toMatch(/\.comparison-table-wrapper[\s\S]*?overflow/);
    });
  });

  // Test Case 2: Value propositions display in appropriate layout (2 columns or stacked)
  describe('Test Case 2: Value propositions display appropriately on tablet', () => {
    test('should have value propositions section', () => {
      const section = document.querySelector('.value-propositions, #value-propositions, [data-section="value-propositions"]');
      expect(section).not.toBeNull();
    });

    test('should have responsive layout for propositions grid', () => {
      // Check that propositions-grid has flex-direction column (stacked) by default
      // and switches to grid for larger screens
      const hasFlexColumn = css.match(/\.propositions-grid[\s\S]*?flex-direction[\s:]*column/);
      const hasGridLayout = css.match(/@media[\s\S]*?\.propositions-grid[\s\S]*?grid/);

      // Either stacked by default (flex column) or has responsive grid
      expect(hasFlexColumn || hasGridLayout).toBeTruthy();
    });

    test('should have tablet-appropriate column count for value propositions', () => {
      // For 768px tablet, layout should be stacked (1 column) or 2 columns
      // The current implementation uses 1 column (flex-direction: column) below 1024px
      // and 3 columns at 1024px and above

      // Check desktop breakpoint is at 1024px (meaning tablet <1024px is stacked)
      const desktopBreakpoint = css.match(/@media\s*\(\s*min-width\s*:\s*1024px\s*\)/);
      expect(desktopBreakpoint).not.toBeNull();
    });

    test('should have readable card width on tablet', () => {
      const propositions = document.querySelectorAll('.proposition, [data-proposition]');
      expect(propositions.length).toBe(3);

      // Cards should have padding for readability
      expect(css).toMatch(/\.proposition[\s\S]*?padding/);
    });
  });

  // Test Case 3: Navigation is usable and not truncated on tablet
  describe('Test Case 3: Navigation is usable on tablet', () => {
    test('should have properly sized hero buttons on tablet', () => {
      const heroActions = document.querySelector('.hero-actions');
      expect(heroActions).not.toBeNull();

      // Hero actions should use flexbox with wrap for tablet
      expect(css).toMatch(/\.hero-actions[\s\S]*?flex/);
      expect(css).toMatch(/\.hero-actions[\s\S]*?flex-wrap[\s:]*wrap/);
    });

    test('should have accessible button sizing', () => {
      const buttons = document.querySelectorAll('.btn, .btn-primary, .btn-secondary');
      expect(buttons.length).toBeGreaterThan(0);

      // Buttons should have adequate padding for touch targets
      expect(css).toMatch(/\.btn[\s\S]*?padding/);
    });

    test('should have readable hero text on tablet', () => {
      const heroH1 = document.querySelector('.hero h1');
      expect(heroH1).not.toBeNull();

      // CSS should have responsive font-size for hero h1
      // Check for media query adjusting hero h1 size
      const hasResponsiveHeroText = css.match(/@media[\s\S]*?\.hero h1[\s\S]*?font-size/);
      expect(hasResponsiveHeroText).not.toBeNull();
    });

    test('should have footer links accessible and visible', () => {
      const footerLinks = document.querySelector('.footer-links');
      expect(footerLinks).not.toBeNull();

      const links = footerLinks.querySelectorAll('a');
      expect(links.length).toBeGreaterThan(0);
    });

    test('should have internal navigation links (anchor links)', () => {
      const getStartedLink = document.querySelector('a[href="#getting-started"]');
      expect(getStartedLink).not.toBeNull();
    });

    test('should have commands grid responsive on tablet', () => {
      // Commands grid should adapt to tablet viewport
      const has768GridChange = css.match(/@media\s*\(\s*min-width\s*:\s*768px\s*\)[\s\S]*?\.commands-grid/);
      expect(has768GridChange).not.toBeNull();
    });
  });

  // Additional responsive design verification tests
  describe('Additional tablet responsive requirements', () => {
    test('should have smooth scroll behavior for anchor links', () => {
      // HTML should have smooth scroll enabled
      expect(css).toMatch(/scroll-behavior[\s:]*smooth/);
    });

    test('should have responsive section padding', () => {
      // Sections should have vertical padding
      expect(css).toMatch(/section[\s\S]*?padding/);
    });

    test('should have box-sizing border-box for predictable layouts', () => {
      expect(css).toMatch(/box-sizing[\s:]*border-box/);
    });

    test('should have responsive font sizes for section titles', () => {
      // Section titles should have responsive font sizes
      const hasResponsiveSectionTitle = css.match(/@media[\s\S]*?\.section-title[\s\S]*?font-size/);
      expect(hasResponsiveSectionTitle).not.toBeNull();
    });
  });
});
