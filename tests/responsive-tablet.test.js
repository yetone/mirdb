/**
 * Test Suite: Responsive Design - Tablet
 * Scenario: Verify the homepage displays correctly on tablet devices with
 * appropriate layout adjustments at 768px viewport width.
 *
 * These tests use JSDOM to parse and validate the HTML structure and CSS
 * for tablet responsive behavior.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import { readFileSync } from 'fs';
import { resolve } from 'path';

/**
 * Helper function to extract the content of 768px media query
 * @param {string} css - The CSS content
 * @returns {string} - The content inside the 768px media query
 */
function extract768MediaQuery(css) {
  // Find the start of the 768px media query
  const mediaStart = css.indexOf('@media (max-width: 768px)');
  if (mediaStart === -1) return '';

  // Find the opening brace after the media query declaration
  const braceStart = css.indexOf('{', mediaStart);
  if (braceStart === -1) return '';

  // Track braces to find the matching closing brace
  let braceCount = 1;
  let pos = braceStart + 1;

  while (braceCount > 0 && pos < css.length) {
    if (css[pos] === '{') braceCount++;
    if (css[pos] === '}') braceCount--;
    pos++;
  }

  return css.substring(braceStart + 1, pos - 1);
}

describe('Responsive Design - Tablet', () => {
  let dom;
  let document;
  let window;
  let cssContent;
  let mediaQueryContent;

  beforeEach(() => {
    const htmlPath = resolve(__dirname, '../index.html');
    const cssPath = resolve(__dirname, '../styles.css');
    const html = readFileSync(htmlPath, 'utf-8');
    cssContent = readFileSync(cssPath, 'utf-8');
    mediaQueryContent = extract768MediaQuery(cssContent);

    dom = new JSDOM(html, {
      runScripts: 'dangerously',
      resources: 'usable',
      pretendToBeVisual: true
    });
    document = dom.window.document;
    window = dom.window;

    // Inject CSS for style tests
    const styleElement = document.createElement('style');
    styleElement.textContent = cssContent;
    document.head.appendChild(styleElement);
  });

  afterEach(() => {
    if (dom) {
      dom.window.close();
    }
  });

  /**
   * Test Case 1: Load page at 768px width viewport
   * Expected: Page layout adapts appropriately for tablet width
   */
  describe('Test Case 1: Page layout adapts for tablet width (768px)', () => {
    it('should have viewport meta tag for responsive design', () => {
      const viewportMeta = document.querySelector('meta[name="viewport"]');
      expect(viewportMeta).not.toBeNull();

      const content = viewportMeta.getAttribute('content');
      expect(content).toContain('width=device-width');
      expect(content).toContain('initial-scale=1');
    });

    it('should have CSS media queries targeting tablet breakpoint', () => {
      // Check that CSS contains media queries for tablet/mobile breakpoints
      const hasTabletMediaQuery = cssContent.includes('@media') &&
        (cssContent.includes('768px') ||
         cssContent.includes('max-width') ||
         cssContent.includes('min-width'));
      expect(hasTabletMediaQuery).toBe(true);
    });

    it('should have media query for max-width: 768px', () => {
      // Check for specific tablet breakpoint
      const has768Query = cssContent.includes('max-width: 768px') ||
                          cssContent.includes('max-width:768px');
      expect(has768Query).toBe(true);
    });

    it('should have main content area that adapts to viewport', () => {
      const main = document.querySelector('main');
      expect(main).not.toBeNull();
    });

    it('should have sections with max-width constraints', () => {
      // Check that sections have max-width to prevent too-wide content
      const hasMaxWidth = cssContent.includes('max-width') && cssContent.includes('section');
      expect(hasMaxWidth).toBe(true);
    });

    it('should have padding adjustments in media queries', () => {
      // Media query should adjust padding for smaller viewports
      expect(mediaQueryContent.length).toBeGreaterThan(0);
      expect(mediaQueryContent).toContain('padding');
    });
  });

  /**
   * Test Case 2: Check feature cards layout at 768px
   * Expected: Feature cards display in 2 or 3 column grid
   */
  describe('Test Case 2: Feature cards display in 2 or 3 column grid at 768px', () => {
    it('should have feature-grid element with grid layout', () => {
      const featureGrid = document.querySelector('.feature-grid');
      expect(featureGrid).not.toBeNull();
    });

    it('should have CSS grid definition for feature-grid', () => {
      const hasGridLayout = cssContent.includes('.feature-grid') &&
        (cssContent.includes('grid-template-columns') || cssContent.includes('display: grid'));
      expect(hasGridLayout).toBe(true);
    });

    it('should use auto-fit or auto-fill for responsive grid', () => {
      // Check for responsive grid pattern
      const hasResponsiveGrid = cssContent.includes('auto-fit') ||
                                cssContent.includes('auto-fill') ||
                                cssContent.includes('minmax');
      expect(hasResponsiveGrid).toBe(true);
    });

    it('should have feature cards with minimum width for proper column sizing', () => {
      // Feature cards should have minmax sizing to control column count
      const hasMinMax = cssContent.includes('minmax');
      expect(hasMinMax).toBe(true);
    });

    it('should have at least 6 feature cards that can be arranged in 2-3 column grid', () => {
      const featureCards = document.querySelectorAll('.feature-card');
      expect(featureCards.length).toBeGreaterThanOrEqual(6);
    });

    it('feature cards should have consistent styling', () => {
      const featureCards = document.querySelectorAll('.feature-card');
      expect(featureCards.length).toBeGreaterThan(0);

      featureCards.forEach(card => {
        // Each card should have heading and description
        const heading = card.querySelector('h3');
        const description = card.querySelector('p');
        expect(heading).not.toBeNull();
        expect(description).not.toBeNull();
      });
    });

    it('should adjust grid columns in media query for tablet', () => {
      // At 768px, the grid should adjust to show 1 column or reduce columns
      // Check that media query affects feature-grid
      expect(mediaQueryContent.length).toBeGreaterThan(0);
      expect(mediaQueryContent).toContain('.feature-grid');

      // Verify feature-grid changes grid-template-columns in media query
      expect(mediaQueryContent).toContain('grid-template-columns');
    });
  });

  /**
   * Test Case 3: Verify hero section at tablet width
   * Expected: Hero section content is centered and readable
   */
  describe('Test Case 3: Hero section is centered and readable at tablet width', () => {
    it('should have hero section present', () => {
      const heroSection = document.querySelector('.hero, section.hero, #hero');
      expect(heroSection).not.toBeNull();
    });

    it('should have hero section with text-align center', () => {
      const hasCenteredHero = cssContent.includes('.hero') && cssContent.includes('text-align: center');
      expect(hasCenteredHero).toBe(true);
    });

    it('should have hero h1 heading', () => {
      const heroSection = document.querySelector('.hero, section.hero, #hero');
      expect(heroSection).not.toBeNull();

      const h1 = heroSection.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.textContent.trim().length).toBeGreaterThan(0);
    });

    it('hero heading should be styled with appropriate font size', () => {
      // Check that hero h1 has font-size defined
      const hasHeroH1Styling = cssContent.includes('.hero h1') && cssContent.includes('font-size');
      expect(hasHeroH1Styling).toBe(true);
    });

    it('should have responsive font sizes for hero in media query', () => {
      // Check that media query adjusts hero font sizes
      expect(mediaQueryContent.length).toBeGreaterThan(0);

      // Should have hero h1 styles with font-size in media query
      expect(mediaQueryContent).toContain('.hero h1');
      expect(mediaQueryContent).toContain('font-size');
    });

    it('should have tagline/subtitle element in hero', () => {
      const heroSection = document.querySelector('.hero, section.hero, #hero');
      expect(heroSection).not.toBeNull();

      const tagline = heroSection.querySelector('.tagline, .subtitle, p');
      expect(tagline).not.toBeNull();
      expect(tagline.textContent.trim().length).toBeGreaterThan(0);
    });

    it('should have CTA buttons centered in hero', () => {
      const heroSection = document.querySelector('.hero, section.hero, #hero');
      expect(heroSection).not.toBeNull();

      const ctaButtons = heroSection.querySelector('.cta-buttons, .buttons, .btn');
      expect(ctaButtons).not.toBeNull();
    });

    it('should have hero padding adjustments in media query', () => {
      expect(mediaQueryContent.length).toBeGreaterThan(0);

      // Should have hero padding adjustments
      expect(mediaQueryContent).toContain('.hero');
      expect(mediaQueryContent).toContain('padding');
    });

    it('should have max-width on tagline for readability', () => {
      // Tagline should have max-width to ensure readability
      const hasTaglineMaxWidth = cssContent.includes('.tagline') && cssContent.includes('max-width');
      expect(hasTaglineMaxWidth).toBe(true);
    });

    it('hero content should be semantic with proper structure', () => {
      const heroSection = document.querySelector('.hero, section.hero, #hero');
      expect(heroSection).not.toBeNull();

      // Check for proper heading hierarchy
      const h1 = heroSection.querySelector('h1');
      expect(h1).not.toBeNull();

      // Check for descriptive content
      const paragraphs = heroSection.querySelectorAll('p');
      expect(paragraphs.length).toBeGreaterThan(0);

      // Check for call-to-action
      const links = heroSection.querySelectorAll('a');
      expect(links.length).toBeGreaterThan(0);
    });
  });

  /**
   * Additional tablet-specific responsive tests
   */
  describe('Additional tablet responsive design checks', () => {
    it('should have navigation that adapts for tablet', () => {
      // Check for mobile menu toggle (visible at 768px)
      const mobileToggle = document.querySelector('.mobile-menu-toggle, .hamburger, .menu-toggle');
      expect(mobileToggle).not.toBeNull();
    });

    it('should have mobile menu toggle display rules in media query', () => {
      expect(mediaQueryContent.length).toBeGreaterThan(0);

      // Mobile menu toggle should be displayed
      expect(mediaQueryContent).toContain('.mobile-menu-toggle');
      expect(mediaQueryContent).toContain('display');
    });

    it('should hide nav-links by default in tablet view media query', () => {
      expect(mediaQueryContent.length).toBeGreaterThan(0);

      // Nav links should have display changes in media query
      expect(mediaQueryContent).toContain('.nav-links');
      expect(mediaQueryContent).toContain('display: none');
    });

    it('should have section padding adjustments for tablet', () => {
      expect(mediaQueryContent.length).toBeGreaterThan(0);

      // Sections should have reduced padding in tablet view
      expect(mediaQueryContent).toContain('section');
      expect(mediaQueryContent).toContain('padding');
    });

    it('should have footer responsive layout', () => {
      const footer = document.querySelector('footer, .footer');
      expect(footer).not.toBeNull();

      // Footer should have grid/flex layout for responsive content
      const hasFooterGrid = cssContent.includes('.footer-content') &&
        (cssContent.includes('grid') || cssContent.includes('flex'));
      expect(hasFooterGrid).toBe(true);
    });

    it('should have footer grid adjustments in media query', () => {
      expect(mediaQueryContent.length).toBeGreaterThan(0);

      // Footer content should have column adjustments
      expect(mediaQueryContent).toContain('.footer-content');
      expect(mediaQueryContent).toContain('grid-template-columns');
    });

    it('should have code blocks with horizontal overflow handling', () => {
      // Code blocks should handle overflow for narrow viewports
      const hasCodeOverflow = cssContent.includes('pre') && cssContent.includes('overflow');
      expect(hasCodeOverflow).toBe(true);
    });

    it('should have architecture diagram responsive handling', () => {
      expect(mediaQueryContent.length).toBeGreaterThan(0);

      // Architecture diagram should be responsive
      expect(mediaQueryContent).toContain('.architecture-diagram');
    });

    it('should have quick-start section padding adjustments for tablet', () => {
      expect(mediaQueryContent.length).toBeGreaterThan(0);

      // Quick start section should have responsive adjustments
      expect(mediaQueryContent).toContain('.quick-start');
    });
  });
});
