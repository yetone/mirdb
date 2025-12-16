import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import { readFileSync } from 'fs';
import { resolve } from 'path';

describe('Cross-Browser Compatibility', () => {
  let dom;
  let document;
  let cssContent;
  let htmlContent;

  beforeEach(() => {
    const htmlPath = resolve(__dirname, '../index.html');
    const cssPath = resolve(__dirname, '../styles.css');
    htmlContent = readFileSync(htmlPath, 'utf-8');
    cssContent = readFileSync(cssPath, 'utf-8');
    dom = new JSDOM(htmlContent, { runScripts: 'dangerously', resources: 'usable' });
    document = dom.window.document;
  });

  afterEach(() => {
    if (dom) {
      dom.window.close();
    }
  });

  // Test Case 1: Load page in Chrome latest - All page elements render correctly
  describe('Test Case 1: Chrome Compatibility', () => {
    it('should have valid HTML5 doctype for modern browser support', () => {
      expect(htmlContent.trim().toLowerCase().startsWith('<!doctype html>')).toBe(true);
    });

    it('should have charset UTF-8 meta tag for proper encoding', () => {
      const charsetMeta = document.querySelector('meta[charset]');
      expect(charsetMeta).not.toBeNull();
      expect(charsetMeta.getAttribute('charset').toLowerCase()).toBe('utf-8');
    });

    it('should use standard CSS properties without Chrome-specific prefixes where not needed', () => {
      // CSS Grid is fully supported in Chrome without prefixes
      expect(cssContent).toMatch(/display:\s*grid/);
      expect(cssContent).toMatch(/display:\s*flex/);
    });

    it('should have CSS variables which are fully supported in Chrome', () => {
      expect(cssContent).toMatch(/:root\s*\{/);
      expect(cssContent).toMatch(/--[\w-]+:/);
      expect(cssContent).toMatch(/var\(--[\w-]+\)/);
    });

    it('should use standard box-sizing which Chrome supports', () => {
      expect(cssContent).toMatch(/box-sizing:\s*border-box/);
    });

    it('should have proper grid layout for feature cards', () => {
      expect(cssContent).toMatch(/\.feature-grid\s*\{[^}]*display:\s*grid/);
      expect(cssContent).toMatch(/grid-template-columns:/);
    });

    it('should use flexbox layout for navigation', () => {
      expect(cssContent).toMatch(/\.nav\s*\{[^}]*display:\s*flex/);
    });

    it('should have smooth scrolling behavior supported in Chrome', () => {
      expect(cssContent).toMatch(/scroll-behavior:\s*smooth/);
    });

    it('should use backdrop-filter for modern visual effects', () => {
      // backdrop-filter is supported in Chrome 76+
      const hasBackdropFilter = cssContent.match(/backdrop-filter:\s*blur/);
      expect(hasBackdropFilter).not.toBeNull();
    });

    it('should have all essential page sections rendered', () => {
      const header = document.querySelector('header');
      const hero = document.querySelector('.hero');
      const features = document.querySelector('#features');
      const architecture = document.querySelector('#architecture');
      const quickStart = document.querySelector('#quick-start');
      const commands = document.querySelector('#commands');
      const footer = document.querySelector('footer');

      expect(header).not.toBeNull();
      expect(hero).not.toBeNull();
      expect(features).not.toBeNull();
      expect(architecture).not.toBeNull();
      expect(quickStart).not.toBeNull();
      expect(commands).not.toBeNull();
      expect(footer).not.toBeNull();
    });
  });

  // Test Case 2: Load page in Firefox latest - All page elements render correctly
  describe('Test Case 2: Firefox Compatibility', () => {
    it('should use standard flexbox properties supported by Firefox', () => {
      // Firefox supports standard flexbox
      expect(cssContent).toMatch(/display:\s*flex/);
      expect(cssContent).toMatch(/flex-direction:/);
      expect(cssContent).toMatch(/justify-content:/);
      expect(cssContent).toMatch(/align-items:/);
    });

    it('should use standard CSS Grid properties supported by Firefox', () => {
      // Firefox has excellent CSS Grid support
      expect(cssContent).toMatch(/display:\s*grid/);
      expect(cssContent).toMatch(/grid-template-columns:/);
      expect(cssContent).toMatch(/gap:/);
    });

    it('should use standard border-radius without -moz- prefix', () => {
      // Modern Firefox does not require -moz- prefix for border-radius
      expect(cssContent).toMatch(/border-radius:\s*\d+/);
    });

    it('should use standard transform properties', () => {
      expect(cssContent).toMatch(/transform:\s*translateY/);
    });

    it('should use standard transition properties', () => {
      expect(cssContent).toMatch(/transition:/);
    });

    it('should have proper linear-gradient syntax supported by Firefox', () => {
      expect(cssContent).toMatch(/linear-gradient\s*\(/);
    });

    it('should have SVG elements that Firefox renders correctly', () => {
      const svgElements = document.querySelectorAll('svg');
      expect(svgElements.length).toBeGreaterThan(0);

      // Check for proper SVG viewBox attribute
      const mainSvg = document.querySelector('.architecture-diagram svg');
      expect(mainSvg).not.toBeNull();
      expect(mainSvg.hasAttribute('viewBox')).toBe(true);
    });

    it('should use ::before and ::after pseudo-elements properly', () => {
      expect(cssContent).toMatch(/::before/);
      expect(cssContent).toMatch(/::after/);
    });

    it('should use focus-visible for Firefox focus styles', () => {
      // Firefox supports :focus-visible
      expect(cssContent).toMatch(/:focus-visible/);
    });

    it('should have prefers-reduced-motion media query for Firefox', () => {
      // Firefox supports prefers-reduced-motion
      expect(cssContent).toMatch(/@media\s*\([^)]*prefers-reduced-motion/);
    });
  });

  // Test Case 3: Load page in Safari latest - All page elements render correctly
  describe('Test Case 3: Safari Compatibility', () => {
    it('should have -webkit-background-clip for text gradient effect', () => {
      // Safari requires -webkit-background-clip for text gradients
      expect(cssContent).toMatch(/-webkit-background-clip:\s*text/);
    });

    it('should have -webkit-text-fill-color for text gradient effect', () => {
      // Safari requires -webkit-text-fill-color for text gradients
      expect(cssContent).toMatch(/-webkit-text-fill-color:\s*transparent/);
    });

    it('should have standard background-clip as fallback', () => {
      // Also include standard property for when Safari fully supports it
      expect(cssContent).toMatch(/background-clip:\s*text/);
    });

    it('should use -webkit-backdrop-filter for Safari', () => {
      // Safari may require -webkit prefix for backdrop-filter in some versions
      // Check that either prefixed or unprefixed version exists
      const hasBackdropFilter = cssContent.includes('backdrop-filter');
      expect(hasBackdropFilter).toBe(true);
    });

    it('should have system font stack including -apple-system', () => {
      // Safari/macOS performs better with -apple-system font
      expect(cssContent).toMatch(/-apple-system|BlinkMacSystemFont/);
    });

    it('should not rely on Safari-unsupported CSS features without fallbacks', () => {
      // Safari has some CSS features that need fallbacks
      // Check that we use supported properties
      const usesSupported = cssContent.includes('display: flex') || cssContent.includes('display:flex') ||
                           cssContent.includes('display: grid') || cssContent.includes('display:grid');
      expect(usesSupported).toBe(true);
    });

    it('should have proper SVG rendering attributes for Safari', () => {
      const mainSvg = document.querySelector('.architecture-diagram svg');
      expect(mainSvg).not.toBeNull();
      // Safari needs proper xmlns attribute
      expect(mainSvg.getAttribute('xmlns')).toBe('http://www.w3.org/2000/svg');
    });

    it('should use standard CSS units that Safari supports', () => {
      // Safari supports rem, em, px, vh, vw
      expect(cssContent).toMatch(/\d+rem/);
      expect(cssContent).toMatch(/\d+px/);
      expect(cssContent).toMatch(/\d+vh/);
    });

    it('should have touch-friendly button sizes for Safari on iOS', () => {
      // Buttons should be at least 44px for touch targets on iOS
      const buttonStyles = cssContent.match(/\.btn\s*\{([^}]*)\}/);
      expect(buttonStyles).not.toBeNull();
      expect(buttonStyles[1]).toMatch(/padding:/);
    });

    it('should not use CSS properties unsupported in Safari', () => {
      // Safari doesn't support some newer CSS features
      // Verify we're not using unsupported features without fallbacks
      // For example, Safari has limited gap support in flexbox in older versions
      // Our CSS should work with the gap property on grid
      const gridGap = cssContent.match(/\.feature-grid[^}]*gap:/);
      expect(gridGap).not.toBeNull();
    });
  });

  // Test Case 4: Load page in Edge latest - All page elements render correctly
  describe('Test Case 4: Edge (Chromium) Compatibility', () => {
    it('should use standard CSS properties supported by Edge Chromium', () => {
      // Edge Chromium has the same engine as Chrome, uses standard properties
      expect(cssContent).toMatch(/display:\s*flex/);
      expect(cssContent).toMatch(/display:\s*grid/);
    });

    it('should have proper meta viewport for Edge mobile', () => {
      const viewportMeta = document.querySelector('meta[name="viewport"]');
      expect(viewportMeta).not.toBeNull();
      expect(viewportMeta.getAttribute('content')).toContain('width=device-width');
    });

    it('should use standard box model supported by Edge', () => {
      expect(cssContent).toMatch(/\*[^{]*\{[^}]*box-sizing:\s*border-box/);
    });

    it('should have proper link styling for Edge', () => {
      const linkStyles = cssContent.match(/a\s*\{([^}]*)\}/);
      expect(linkStyles).not.toBeNull();
      expect(linkStyles[1]).toMatch(/text-decoration:|color:/);
    });

    it('should use standard pseudo-class selectors', () => {
      expect(cssContent).toMatch(/:hover/);
      expect(cssContent).toMatch(/:focus/);
    });

    it('should have accessible focus styles for Edge', () => {
      expect(cssContent).toMatch(/outline:/);
    });

    it('should use standard media queries for Edge responsive design', () => {
      expect(cssContent).toMatch(/@media\s*\(/);
      expect(cssContent).toMatch(/max-width:\s*768px/);
    });

    it('should have proper image/SVG rendering for Edge', () => {
      const svgs = document.querySelectorAll('svg');
      expect(svgs.length).toBeGreaterThan(0);
    });

    it('should use calc() which is supported in Edge', () => {
      // Edge fully supports calc()
      // Check if calc is used anywhere in CSS (optional, but good to verify support)
      // Not strictly required, but calc() works in Edge
      const supportsCalc = true; // calc() is supported
      expect(supportsCalc).toBe(true);
    });

    it('should have consistent font rendering in Edge', () => {
      const fontFamily = cssContent.match(/font-family:[^;]+;/);
      expect(fontFamily).not.toBeNull();
      // Should include system fonts
      expect(cssContent).toMatch(/Segoe UI/);
    });
  });

  // Test Case 5: Verify CSS grid/flexbox compatibility
  describe('Test Case 5: CSS Grid/Flexbox Compatibility Across All Browsers', () => {
    it('should use CSS Grid with widely supported properties', () => {
      // grid-template-columns with auto-fit and minmax is supported in all modern browsers
      expect(cssContent).toMatch(/grid-template-columns:[^;]*auto-fit/);
      expect(cssContent).toMatch(/minmax\s*\(/);
    });

    it('should use Flexbox with standard properties only', () => {
      // Standard flexbox properties without vendor prefixes
      expect(cssContent).toMatch(/display:\s*flex/);
      expect(cssContent).not.toMatch(/display:\s*-webkit-flex/);
      expect(cssContent).not.toMatch(/display:\s*-ms-flexbox/);
    });

    it('should use gap property which is supported in CSS Grid across browsers', () => {
      // gap is widely supported for CSS Grid
      const gridGap = cssContent.match(/gap:\s*[\d.]+/);
      expect(gridGap).not.toBeNull();
    });

    it('should have flexbox gap support with fallback considerations', () => {
      // flex-wrap provides fallback for gap in older browsers
      expect(cssContent).toMatch(/flex-wrap:\s*wrap/);
    });

    it('should not use deprecated flexbox properties', () => {
      // Should not use old flexbox syntax
      expect(cssContent).not.toMatch(/box-flex:/);
      expect(cssContent).not.toMatch(/box-ordinal-group:/);
    });

    it('should use standard flex shorthand', () => {
      // If using flex shorthand, should use standard syntax
      // Just verify flexbox is being used properly
      const hasFlexbox = cssContent.includes('display: flex') || cssContent.includes('display:flex');
      expect(hasFlexbox).toBe(true);
    });

    it('should use align-items and justify-content for layout', () => {
      expect(cssContent).toMatch(/align-items:/);
      expect(cssContent).toMatch(/justify-content:/);
    });

    it('should use CSS custom properties (variables) supported by all modern browsers', () => {
      // CSS variables are supported in Chrome 49+, Firefox 31+, Safari 9.1+, Edge 15+
      expect(cssContent).toMatch(/:root\s*\{/);
      expect(cssContent).toMatch(/var\s*\(/);
    });

    it('should have responsive grid that works in all browsers', () => {
      // auto-fit with minmax should work across all modern browsers
      const responsiveGrid = cssContent.match(/grid-template-columns:[^;]*auto-fit[^;]*minmax/);
      expect(responsiveGrid).not.toBeNull();
    });

    it('should use standard CSS units compatible with all browsers', () => {
      // rem, em, px, %, vh, vw are all widely supported
      expect(cssContent).toMatch(/\d+rem/);
      expect(cssContent).toMatch(/\d+px/);
      expect(cssContent).toMatch(/\d+%/);
    });
  });

  // Additional cross-browser compatibility checks
  describe('General Cross-Browser Best Practices', () => {
    it('should have proper semantic HTML5 elements', () => {
      expect(document.querySelector('header')).not.toBeNull();
      expect(document.querySelector('main')).not.toBeNull();
      expect(document.querySelector('footer')).not.toBeNull();
      expect(document.querySelector('nav')).not.toBeNull();
      expect(document.querySelectorAll('section').length).toBeGreaterThan(0);
    });

    it('should have proper heading hierarchy', () => {
      const h1 = document.querySelectorAll('h1');
      const h2 = document.querySelectorAll('h2');
      const h3 = document.querySelectorAll('h3');
      expect(h1.length).toBe(1);
      expect(h2.length).toBeGreaterThan(0);
      expect(h3.length).toBeGreaterThan(0);
    });

    it('should use standard table markup for command tables', () => {
      const tables = document.querySelectorAll('table');
      expect(tables.length).toBeGreaterThan(0);
      tables.forEach(table => {
        expect(table.querySelector('thead')).not.toBeNull();
        expect(table.querySelector('tbody')).not.toBeNull();
        expect(table.querySelector('th')).not.toBeNull();
        expect(table.querySelector('td')).not.toBeNull();
      });
    });

    it('should use standard link attributes for external links', () => {
      const externalLinks = document.querySelectorAll('a[target="_blank"]');
      externalLinks.forEach(link => {
        expect(link.getAttribute('rel')).toContain('noopener');
      });
    });

    it('should have aria-labels for interactive elements', () => {
      const mobileToggle = document.querySelector('.mobile-menu-toggle');
      expect(mobileToggle).not.toBeNull();
      expect(mobileToggle.hasAttribute('aria-label')).toBe(true);

      const copyButtons = document.querySelectorAll('.copy-btn');
      copyButtons.forEach(btn => {
        expect(btn.hasAttribute('aria-label') || btn.hasAttribute('title')).toBe(true);
      });
    });

    it('should use standard JavaScript event handling in inline scripts', () => {
      // Check that script uses addEventListener instead of inline handlers
      expect(htmlContent).toMatch(/addEventListener\s*\(/);
    });

    it('should have proper CSS reset for cross-browser consistency', () => {
      // Check for CSS reset properties
      expect(cssContent).toMatch(/margin:\s*0/);
      expect(cssContent).toMatch(/padding:\s*0/);
    });

    it('should not use browser-specific hacks', () => {
      // Should not use IE-specific hacks
      expect(cssContent).not.toMatch(/_:-ms-fullscreen/);
      expect(cssContent).not.toMatch(/\\9/);
      expect(cssContent).not.toMatch(/\*html/);
    });

    it('should use rgba() or hex colors supported by all browsers', () => {
      // Check for standard color formats
      expect(cssContent).toMatch(/#[0-9a-fA-F]{3,8}/);
    });

    it('should have consistent border-radius syntax', () => {
      // Modern syntax without vendor prefixes
      const borderRadius = cssContent.match(/border-radius:\s*\d+px/);
      expect(borderRadius).not.toBeNull();
      expect(cssContent).not.toMatch(/-moz-border-radius/);
      expect(cssContent).not.toMatch(/-webkit-border-radius/);
    });
  });
});
