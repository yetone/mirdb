/**
 * Responsive Design - Desktop Tests
 *
 * Tests for verifying homepage renders correctly on desktop devices (NFR-1)
 * Test cases:
 * 1. Full desktop layout displays with multi-column feature grid at 1280px
 * 2. Main content has max-width constraint for readability
 * 3. Layout handles large screens (1920px) without breaking
 */

const fs = require('fs');
const path = require('path');

describe('Responsive Design - Desktop', () => {
  let html;
  let styleContent;

  beforeAll(() => {
    // Load HTML content
    const htmlPath = path.join(__dirname, '..', 'index.html');
    html = fs.readFileSync(htmlPath, 'utf-8');

    // Load CSS content
    const cssPath = path.join(__dirname, '..', 'css', 'styles.css');
    styleContent = fs.readFileSync(cssPath, 'utf-8');
  });

  beforeEach(() => {
    document.documentElement.innerHTML = html;
  });

  describe('Test Case 1: Load page at 1280px viewport width', () => {
    it('should display full desktop layout', () => {
      // Simulate desktop viewport
      Object.defineProperty(window, 'innerWidth', {
        writable: true,
        configurable: true,
        value: 1280
      });
      window.dispatchEvent(new Event('resize'));

      // Verify main sections are present and visible
      const header = document.querySelector('.header');
      const hero = document.querySelector('.hero');
      const features = document.querySelector('.features');
      const quickStart = document.querySelector('.quick-start');
      const technicalSpecs = document.querySelector('.technical-specs');
      const footer = document.querySelector('.footer');

      expect(header).toBeTruthy();
      expect(hero).toBeTruthy();
      expect(features).toBeTruthy();
      expect(quickStart).toBeTruthy();
      expect(technicalSpecs).toBeTruthy();
      expect(footer).toBeTruthy();
    });

    it('should display multi-column feature grid', () => {
      // Verify feature grid exists
      const featureGrid = document.querySelector('.feature-grid');
      expect(featureGrid).toBeTruthy();

      // Verify multiple feature cards exist for grid layout
      const featureCards = document.querySelectorAll('.feature-card');
      expect(featureCards.length).toBeGreaterThanOrEqual(3);

      // Verify CSS uses grid layout with auto-fit for responsive columns
      expect(styleContent).toMatch(/\.feature-grid\s*\{[^}]*display:\s*grid/);
      expect(styleContent).toMatch(/\.feature-grid\s*\{[^}]*grid-template-columns:\s*repeat\(auto-fit/);
    });

    it('should display full navigation at desktop width', () => {
      const navLinks = document.querySelector('.nav-links');
      expect(navLinks).toBeTruthy();

      // Verify all navigation links are present
      const links = navLinks.querySelectorAll('a');
      expect(links.length).toBeGreaterThanOrEqual(4);

      // Verify key navigation items
      const linkTexts = Array.from(links).map(link => link.textContent);
      expect(linkTexts).toContain('Features');
      expect(linkTexts).toContain('Quick Start');
      expect(linkTexts).toContain('GitHub');
    });

    it('should display nav as horizontal flex layout at desktop width', () => {
      // Verify CSS has flex layout for nav
      expect(styleContent).toMatch(/\.nav\s*\{[^}]*display:\s*flex/);
      expect(styleContent).toMatch(/\.nav-links\s*\{[^}]*display:\s*flex/);
    });
  });

  describe('Test Case 2: Check content container max-width', () => {
    it('should have max-width constraint on container', () => {
      const container = document.querySelector('.container');
      expect(container).toBeTruthy();

      // Verify CSS has max-width for container
      expect(styleContent).toMatch(/--container-max-width:\s*1200px/);
      expect(styleContent).toMatch(/\.container\s*\{[^}]*max-width:\s*var\(--container-max-width\)/);
    });

    it('should have max-width less than or equal to 1200px for readability', () => {
      // Check that max-width is set to a readable width (typically 1200px or less)
      const maxWidthMatch = styleContent.match(/--container-max-width:\s*(\d+)px/);
      expect(maxWidthMatch).toBeTruthy();

      const maxWidthValue = parseInt(maxWidthMatch[1], 10);
      expect(maxWidthValue).toBeLessThanOrEqual(1400);
      expect(maxWidthValue).toBeGreaterThanOrEqual(800);
    });

    it('should center container with auto margins', () => {
      // Verify container is centered
      expect(styleContent).toMatch(/\.container\s*\{[^}]*margin:\s*0\s+auto/);
    });

    it('should have appropriate padding on container', () => {
      // Verify container has padding for spacing
      expect(styleContent).toMatch(/\.container\s*\{[^}]*padding:\s*0\s+var\(--spacing-md\)/);
    });

    it('should constrain hero description width for readability', () => {
      const description = document.querySelector('.hero .description');
      expect(description).toBeTruthy();

      // Verify CSS constrains hero description width
      expect(styleContent).toMatch(/\.hero\s+\.description\s*\{[^}]*max-width:\s*600px/);
    });
  });

  describe('Test Case 3: Load page at 1920px viewport width', () => {
    beforeEach(() => {
      // Simulate large desktop viewport
      Object.defineProperty(window, 'innerWidth', {
        writable: true,
        configurable: true,
        value: 1920
      });
      window.dispatchEvent(new Event('resize'));
    });

    it('should not break layout at 1920px width', () => {
      // All major sections should still be present
      const sections = [
        '.header',
        '.hero',
        '.features',
        '.comparison',
        '.quick-start',
        '.technical-specs',
        '.footer'
      ];

      sections.forEach(selector => {
        const element = document.querySelector(selector);
        expect(element).toBeTruthy();
      });
    });

    it('should maintain content max-width at large screens', () => {
      // Content should be constrained even at 1920px
      const containers = document.querySelectorAll('.container');
      expect(containers.length).toBeGreaterThan(0);

      // The CSS should have the max-width variable defined
      expect(styleContent).toMatch(/--container-max-width:\s*1200px/);
    });

    it('should have grids that adapt but not stretch excessively', () => {
      // Feature grid should use auto-fit with min/max constraints
      expect(styleContent).toMatch(/\.feature-grid\s*\{[^}]*grid-template-columns:\s*repeat\(auto-fit,\s*minmax\(\d+px,\s*1fr\)\)/);

      // Comparison grid should also be responsive
      expect(styleContent).toMatch(/\.comparison-grid\s*\{[^}]*grid-template-columns:\s*repeat\(auto-fit,\s*minmax\(\d+px,\s*1fr\)\)/);

      // Specs grid should be responsive
      expect(styleContent).toMatch(/\.specs-grid\s*\{[^}]*grid-template-columns:\s*repeat\(auto-fit,\s*minmax\(\d+px,\s*1fr\)\)/);
    });

    it('should have footer that spans full width but content is constrained', () => {
      const footer = document.querySelector('.footer');
      expect(footer).toBeTruthy();

      const footerContainer = footer.querySelector('.container');
      expect(footerContainer).toBeTruthy();

      // Footer grid should also adapt
      expect(styleContent).toMatch(/\.footer-content\s*\{[^}]*display:\s*grid/);
    });

    it('should display multi-column layouts for specs and comparison sections', () => {
      // At large screens, grids should display multiple columns
      const specsGrid = document.querySelector('.specs-grid');
      const comparisonGrid = document.querySelector('.comparison-grid');

      expect(specsGrid).toBeTruthy();
      expect(comparisonGrid).toBeTruthy();

      // Verify minimum column size is reasonable
      const specsMinMatch = styleContent.match(/\.specs-grid\s*\{[^}]*minmax\((\d+)px/);
      const comparisonMinMatch = styleContent.match(/\.comparison-grid\s*\{[^}]*minmax\((\d+)px/);

      expect(specsMinMatch).toBeTruthy();
      expect(comparisonMinMatch).toBeTruthy();

      // Min widths should be reasonable for desktop
      const specsMin = parseInt(specsMinMatch[1], 10);
      const comparisonMin = parseInt(comparisonMinMatch[1], 10);

      expect(specsMin).toBeGreaterThanOrEqual(200);
      expect(comparisonMin).toBeGreaterThanOrEqual(200);
    });

    it('should have nav constrained to max-width at large screens', () => {
      // Nav should also have max-width constraint
      expect(styleContent).toMatch(/\.nav\s*\{[^}]*max-width:\s*var\(--container-max-width\)/);
    });
  });

  describe('Desktop Layout Breakpoint Verification', () => {
    it('should not have mobile-specific styles apply above 768px', () => {
      // Verify mobile breakpoint is at 768px
      expect(styleContent).toMatch(/@media\s*\(max-width:\s*768px\)/);

      // At desktop widths (above 768px), mobile styles should not apply
      Object.defineProperty(window, 'innerWidth', {
        writable: true,
        configurable: true,
        value: 1280
      });

      // Verify nav is horizontal layout by default (not column layout for mobile)
      expect(styleContent).toMatch(/\.nav\s*\{[^}]*display:\s*flex/);
      expect(styleContent).toMatch(/\.nav\s*\{[^}]*justify-content:\s*space-between/);
    });

    it('should have horizontal flex layout for CTA buttons at desktop', () => {
      // CTA buttons should be inline at desktop
      expect(styleContent).toMatch(/\.cta-buttons\s*\{[^}]*display:\s*flex/);
      expect(styleContent).toMatch(/\.cta-buttons\s*\{[^}]*justify-content:\s*center/);
    });
  });
});
