/**
 * Desktop Responsive Design Unit Tests
 * Owner: Scenario 9 - Responsive Design - Desktop
 *
 * Tests verify desktop responsive CSS rules and HTML structure.
 * These tests check CSS media queries and HTML elements that support
 * desktop viewport rendering (1920x1080+).
 */

const fs = require('fs');
const path = require('path');

describe('Desktop Responsive Design', () => {
  let generalCss;
  let chromeCss;
  let homepageCss;
  let indexHtml;

  beforeAll(() => {
    // Read CSS files
    generalCss = fs.readFileSync(
      path.join(__dirname, '../../../theme/css/general.css'),
      'utf8'
    );
    chromeCss = fs.readFileSync(
      path.join(__dirname, '../../../theme/css/chrome.css'),
      'utf8'
    );
    homepageCss = fs.readFileSync(
      path.join(__dirname, '../../../theme/css/homepage.css'),
      'utf8'
    );
    // Read HTML file
    indexHtml = fs.readFileSync(
      path.join(__dirname, '../../../book/index.html'),
      'utf8'
    );
  });

  describe('Test Case 1: Hero section spans appropriate width with centered content', () => {
    test('Hero section CSS has centering styles', () => {
      // Check hero has flexbox centering
      expect(homepageCss).toMatch(/\.hero\s*\{[^}]*display:\s*flex/);
      expect(homepageCss).toMatch(/\.hero\s*\{[^}]*align-items:\s*center/);
      expect(homepageCss).toMatch(/\.hero\s*\{[^}]*justify-content:\s*center/);
    });

    test('Hero content has max-width for desktop', () => {
      // Check hero-content has max-width defined
      expect(homepageCss).toMatch(/\.hero-content\s*\{[^}]*max-width:/);
    });

    test('Desktop media query enhances hero sizing', () => {
      // Check for desktop breakpoint adjustments
      expect(generalCss).toMatch(/@media\s*\(\s*min-width:\s*1200px\s*\)/);
      expect(generalCss).toContain('.hero-content');
    });

    test('HTML contains required hero elements', () => {
      expect(indexHtml).toContain('id="hero"');
      expect(indexHtml).toContain('class="hero-content"');
      expect(indexHtml).toContain('class="hero-title"');
      expect(indexHtml).toContain('class="hero-tagline"');
      expect(indexHtml).toContain('class="cta-button"');
    });
  });

  describe('Test Case 2: Features grid displays in multi-column layout', () => {
    test('Features grid uses CSS Grid', () => {
      expect(homepageCss).toMatch(/\.features-grid\s*\{[^}]*display:\s*grid/);
    });

    test('Features grid has responsive columns', () => {
      // Check for grid-template-columns with auto-fit or similar
      expect(homepageCss).toMatch(/\.features-grid\s*\{[^}]*grid-template-columns:/);
    });

    test('Desktop media query defines 2-column layout', () => {
      // Check desktop breakpoint sets explicit columns
      expect(generalCss).toMatch(/@media\s*\(\s*min-width:\s*1200px\s*\)[^]*\.features-grid\s*\{[^}]*grid-template-columns:\s*repeat\s*\(\s*2/);
    });

    test('Features grid has max-width constraint', () => {
      expect(homepageCss).toMatch(/\.features-grid\s*\{[^}]*max-width:/);
    });

    test('HTML contains multiple feature cards', () => {
      const featureCardCount = (indexHtml.match(/class="feature-card"/g) || []).length;
      expect(featureCardCount).toBeGreaterThanOrEqual(2);
    });
  });

  describe('Test Case 3: Navigation displays horizontally without hamburger menu', () => {
    test('Navigation uses flexbox for horizontal layout', () => {
      expect(chromeCss).toMatch(/\.nav-list\s*\{[^}]*display:\s*flex/);
    });

    test('Navigation list has row direction by default', () => {
      // Should not have flex-direction: column for desktop
      // or should have row as default
      const navListStyle = chromeCss.match(/\.nav-list\s*\{[^}]*\}/);
      if (navListStyle) {
        // Either no flex-direction (defaults to row) or explicit row
        const hasColumnDirection = navListStyle[0].includes('flex-direction: column');
        expect(hasColumnDirection).toBe(false);
      }
    });

    test('Mobile menu toggle is hidden by default', () => {
      expect(chromeCss).toMatch(/\.mobile-menu-toggle\s*\{[^}]*display:\s*none/);
    });

    test('Desktop media query keeps navigation visible', () => {
      // Check desktop breakpoint maintains nav display
      expect(generalCss).toMatch(/@media\s*\(\s*min-width:\s*1200px\s*\)[^]*\.main-nav\s*\{[^}]*display:\s*flex/);
    });

    test('HTML contains navigation elements', () => {
      expect(indexHtml).toContain('class="main-nav"');
      expect(indexHtml).toContain('class="nav-list"');
      expect(indexHtml).toContain('class="nav-link"');
      expect(indexHtml).toContain('class="mobile-menu-toggle"');
    });

    test('Navigation has multiple links', () => {
      const navLinkCount = (indexHtml.match(/class="nav-link/g) || []).length;
      expect(navLinkCount).toBeGreaterThanOrEqual(3);
    });
  });

  describe('Test Case 4: No horizontal scrollbar appears', () => {
    test('HTML and body have overflow-x hidden', () => {
      expect(generalCss).toMatch(/html,?\s*body\s*\{[^}]*overflow-x:\s*hidden/);
    });

    test('HTML and body have max-width constraint', () => {
      expect(generalCss).toMatch(/html,?\s*body\s*\{[^}]*max-width:\s*100vw/);
    });

    test('All major containers have max-width constraints', () => {
      // Check that content containers have max-width
      expect(homepageCss).toMatch(/\.hero-content\s*\{[^}]*max-width:/);
      expect(homepageCss).toMatch(/\.features-grid\s*\{[^}]*max-width:/);
      expect(chromeCss).toMatch(/\.header-container\s*\{[^}]*max-width:/);
      expect(chromeCss).toMatch(/\.footer-container\s*\{[^}]*max-width:/);
    });

    test('Box-sizing is set to border-box', () => {
      expect(generalCss).toMatch(/\*\s*\{[^}]*box-sizing:\s*border-box/);
    });

    test('Images have max-width 100%', () => {
      expect(generalCss).toMatch(/img\s*\{[^}]*max-width:\s*100%/);
    });
  });

  describe('Desktop Layout Breakpoints', () => {
    test('Has 1200px desktop breakpoint', () => {
      expect(generalCss).toMatch(/@media\s*\(\s*min-width:\s*1200px\s*\)/);
    });

    test('Has 1920px extra-large desktop breakpoint', () => {
      expect(generalCss).toMatch(/@media\s*\(\s*min-width:\s*1920px\s*\)/);
    });

    test('Desktop breakpoint adjusts hero sizing', () => {
      // Look for hero adjustments in desktop media query
      const desktopMediaQuery = generalCss.match(/@media\s*\(\s*min-width:\s*1200px\s*\)[^@]*/);
      expect(desktopMediaQuery).not.toBeNull();
      expect(desktopMediaQuery[0]).toContain('.hero');
    });

    test('Desktop breakpoint adjusts features grid', () => {
      const desktopMediaQuery = generalCss.match(/@media\s*\(\s*min-width:\s*1200px\s*\)[^@]*/);
      expect(desktopMediaQuery).not.toBeNull();
      expect(desktopMediaQuery[0]).toContain('.features-grid');
    });

    test('Desktop breakpoint adjusts roadmap columns', () => {
      const desktopMediaQuery = generalCss.match(/@media\s*\(\s*min-width:\s*1200px\s*\)[^@]*/);
      expect(desktopMediaQuery).not.toBeNull();
      expect(desktopMediaQuery[0]).toContain('.roadmap-columns');
    });
  });

  describe('Extra Large Desktop (1920px+)', () => {
    test('Extra large breakpoint increases content max-width', () => {
      const xlMediaQuery = generalCss.match(/@media\s*\(\s*min-width:\s*1920px\s*\)[^@]*/);
      expect(xlMediaQuery).not.toBeNull();
    });

    test('Extra large breakpoint adjusts hero content width', () => {
      const xlMediaQuery = generalCss.match(/@media\s*\(\s*min-width:\s*1920px\s*\)[^@]*/);
      expect(xlMediaQuery).not.toBeNull();
      expect(xlMediaQuery[0]).toContain('.hero-content');
    });

    test('Extra large breakpoint adjusts container widths', () => {
      const xlMediaQuery = generalCss.match(/@media\s*\(\s*min-width:\s*1920px\s*\)[^@]*/);
      expect(xlMediaQuery).not.toBeNull();
      // Should have header/footer container adjustments
      expect(xlMediaQuery[0]).toContain('container');
    });
  });

  describe('HTML Structure Validation', () => {
    test('HTML has proper viewport meta tag', () => {
      expect(indexHtml).toMatch(/<meta\s+name="viewport"[^>]*content="[^"]*width=device-width/);
    });

    test('HTML has all required sections', () => {
      expect(indexHtml).toContain('id="hero"');
      expect(indexHtml).toContain('id="features"');
      expect(indexHtml).toContain('id="quickstart"');
      expect(indexHtml).toContain('id="roadmap"');
    });

    test('HTML has header and footer', () => {
      expect(indexHtml).toContain('class="site-header"');
      expect(indexHtml).toContain('class="site-footer"');
    });

    test('HTML includes required CSS files', () => {
      expect(indexHtml).toContain('css/general.css');
      expect(indexHtml).toContain('css/homepage.css');
      expect(indexHtml).toContain('css/chrome.css');
    });
  });
});
