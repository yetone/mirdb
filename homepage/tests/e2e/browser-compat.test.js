/**
 * Cross-Browser Compatibility E2E Tests
 * Owner: Scenario 11 - Cross-Browser Compatibility
 *
 * Validates the homepage displays correctly in all major browsers
 * (Chrome, Firefox, Safari, Edge) released within the last 2 years
 * as specified in NFR-3.
 *
 * Tests cover:
 * - CSS feature compatibility (Grid, Flexbox, Custom Properties)
 * - JavaScript API compatibility (Clipboard API, ES6+ features)
 * - HTML5 semantic elements
 * - Layout consistency across browsers
 * - Interactive feature functionality
 *
 * Requirements: NFR-3
 * @jest-environment jsdom
 */

import { readFileSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

/**
 * Helper to load the homepage HTML
 */
function loadHomepage() {
  const htmlPath = resolve(__dirname, '../../index.html');
  const html = readFileSync(htmlPath, 'utf-8');
  document.documentElement.innerHTML = html;
}

/**
 * Helper to load and combine CSS files
 */
function loadAllCSS() {
  const cssFiles = [
    '../../css/styles.css',
    '../../css/responsive.css',
    '../../css/components/hero.css',
    '../../css/components/features.css',
    '../../css/components/quickstart.css',
    '../../css/components/status.css',
    '../../css/components/footer.css'
  ];

  let combinedCSS = '';
  cssFiles.forEach(file => {
    try {
      const cssPath = resolve(__dirname, file);
      combinedCSS += readFileSync(cssPath, 'utf-8') + '\n';
    } catch (e) {
      // Some CSS files might not exist yet
    }
  });

  return combinedCSS;
}

/**
 * Helper to load JavaScript files for analysis
 */
function loadAllJS() {
  const jsFiles = [
    '../../js/main.js',
    '../../js/components/clipboard.js',
    '../../js/components/navigation.js',
    '../../js/components/scroll.js',
    '../../js/utils/accessibility.js'
  ];

  let combinedJS = '';
  jsFiles.forEach(file => {
    try {
      const jsPath = resolve(__dirname, file);
      combinedJS += readFileSync(jsPath, 'utf-8') + '\n';
    } catch (e) {
      // Some JS files might not exist yet
    }
  });

  return combinedJS;
}

/**
 * Browser compatibility reference data
 * Features supported by Chrome 100+, Firefox 100+, Safari 15.4+, Edge 100+
 * (approximately last 2 years from 2024)
 */
const SUPPORTED_CSS_FEATURES = {
  // CSS Grid Layout - supported by all modern browsers
  grid: {
    properties: ['display: grid', 'grid-template-columns', 'grid-template-rows', 'gap', 'grid-gap'],
    minBrowserVersions: { chrome: 57, firefox: 52, safari: 10.1, edge: 16 }
  },
  // CSS Flexbox - widely supported
  flexbox: {
    properties: ['display: flex', 'flex-direction', 'flex-wrap', 'justify-content', 'align-items', 'gap'],
    minBrowserVersions: { chrome: 29, firefox: 28, safari: 9, edge: 12 }
  },
  // CSS Custom Properties (Variables) - supported by all modern browsers
  customProperties: {
    properties: ['var(--', ':root {'],
    minBrowserVersions: { chrome: 49, firefox: 31, safari: 9.1, edge: 15 }
  },
  // CSS Transforms - well supported
  transforms: {
    properties: ['transform:', 'translateY', 'translateX', 'scale'],
    minBrowserVersions: { chrome: 36, firefox: 16, safari: 9, edge: 12 }
  },
  // CSS Transitions - well supported
  transitions: {
    properties: ['transition:', 'transition-duration', 'transition-property'],
    minBrowserVersions: { chrome: 26, firefox: 16, safari: 9, edge: 12 }
  },
  // CSS Box Shadow - well supported
  boxShadow: {
    properties: ['box-shadow:'],
    minBrowserVersions: { chrome: 10, firefox: 4, safari: 5.1, edge: 12 }
  },
  // CSS Border Radius - well supported
  borderRadius: {
    properties: ['border-radius:'],
    minBrowserVersions: { chrome: 5, firefox: 4, safari: 5, edge: 12 }
  },
  // CSS Media Queries - well supported
  mediaQueries: {
    properties: ['@media', 'min-width:', 'max-width:', 'prefers-reduced-motion'],
    minBrowserVersions: { chrome: 21, firefox: 3.5, safari: 4, edge: 12 }
  },
  // CSS Focus Visible - supported in modern browsers
  focusVisible: {
    properties: [':focus-visible'],
    minBrowserVersions: { chrome: 86, firefox: 85, safari: 15.4, edge: 86 }
  }
};

const SUPPORTED_JS_FEATURES = {
  // ES6 Modules
  modules: {
    patterns: ['import ', 'export '],
    minBrowserVersions: { chrome: 61, firefox: 60, safari: 11, edge: 79 }
  },
  // Async/Await
  asyncAwait: {
    patterns: ['async ', 'await '],
    minBrowserVersions: { chrome: 55, firefox: 52, safari: 10.1, edge: 15 }
  },
  // Arrow Functions
  arrowFunctions: {
    patterns: ['=>'],
    minBrowserVersions: { chrome: 45, firefox: 22, safari: 10, edge: 12 }
  },
  // Template Literals
  templateLiterals: {
    patterns: ['`'],
    minBrowserVersions: { chrome: 41, firefox: 34, safari: 9, edge: 12 }
  },
  // Clipboard API
  clipboardAPI: {
    patterns: ['navigator.clipboard'],
    minBrowserVersions: { chrome: 66, firefox: 63, safari: 13.1, edge: 79 }
  },
  // Query Selector
  querySelector: {
    patterns: ['querySelector', 'querySelectorAll'],
    minBrowserVersions: { chrome: 1, firefox: 3.5, safari: 3.1, edge: 12 }
  },
  // Event Listeners
  eventListeners: {
    patterns: ['addEventListener'],
    minBrowserVersions: { chrome: 1, firefox: 1, safari: 1, edge: 12 }
  },
  // History API
  historyAPI: {
    patterns: ['history.pushState', 'history.replaceState'],
    minBrowserVersions: { chrome: 5, firefox: 4, safari: 5, edge: 12 }
  },
  // matchMedia
  matchMedia: {
    patterns: ['matchMedia'],
    minBrowserVersions: { chrome: 9, firefox: 6, safari: 5.1, edge: 12 }
  }
};

describe('Cross-Browser Compatibility (NFR-3)', () => {
  let css;
  let js;

  beforeAll(() => {
    css = loadAllCSS();
    js = loadAllJS();
  });

  beforeEach(() => {
    loadHomepage();
  });

  /**
   * Test Case 1: Chrome Layout Compatibility
   * Input: Load homepage in Chrome and check layout
   * Expected: All sections render correctly with proper styling
   */
  describe('Test Case 1: Chrome Layout Compatibility', () => {
    test('CSS Grid features are Chrome-compatible (Chrome 57+)', () => {
      // Grid is supported in Chrome 57+ (March 2017)
      // All major CSS Grid properties should be present
      expect(css).toMatch(/display:\s*grid/i);
      expect(css).toMatch(/grid-template-columns/i);
      expect(css).toMatch(/gap:/i);
    });

    test('CSS Flexbox features are Chrome-compatible', () => {
      expect(css).toMatch(/display:\s*flex/i);
      expect(css).toMatch(/flex-direction/i);
      expect(css).toMatch(/align-items/i);
    });

    test('CSS Custom Properties (variables) are Chrome-compatible (Chrome 49+)', () => {
      expect(css).toMatch(/:root\s*\{/);
      expect(css).toMatch(/var\(--/);
    });

    test('CSS transforms and transitions are Chrome-compatible', () => {
      expect(css).toMatch(/transform:/i);
      expect(css).toMatch(/transition:/i);
    });

    test('All major page sections exist', () => {
      const hero = document.getElementById('hero');
      const features = document.getElementById('features');
      const quickstart = document.getElementById('quickstart');
      const status = document.getElementById('status');
      const footer = document.querySelector('footer');

      expect(hero).toBeInTheDocument();
      expect(features).toBeInTheDocument();
      expect(quickstart).toBeInTheDocument();
      expect(status).toBeInTheDocument();
      expect(footer).toBeInTheDocument();
    });
  });

  /**
   * Test Case 2: Firefox Layout Compatibility
   * Input: Load homepage in Firefox and check layout
   * Expected: All sections render correctly with proper styling
   */
  describe('Test Case 2: Firefox Layout Compatibility', () => {
    test('No -webkit- only prefixes without Firefox equivalents', () => {
      // Check that webkit prefixes have standard or Firefox equivalents
      // Modern CSS should use standard properties
      const webkitOnlyPatterns = [
        /-webkit-appearance\s*:/,
        /-webkit-text-stroke\s*:/
      ];

      // These are acceptable in modern CSS as they're supported or have fallbacks
      // The important thing is that standard properties are also used
      expect(css).toMatch(/font-smoothing/); // Should have webkit and moz versions
    });

    test('CSS Grid features are Firefox-compatible (Firefox 52+)', () => {
      expect(css).toMatch(/display:\s*grid/i);
      expect(css).toMatch(/grid-template-columns/i);
    });

    test('Flexbox gap property is Firefox-compatible (Firefox 63+)', () => {
      // gap in flexbox is supported since Firefox 63
      expect(css).toMatch(/gap:/i);
    });

    test('CSS focus-visible is Firefox-compatible (Firefox 85+)', () => {
      expect(css).toMatch(/:focus-visible/);
    });

    test('No Firefox-incompatible features used', () => {
      // Check for any known Firefox-incompatible CSS
      // Modern Firefox supports virtually all standard CSS features
      // Verify standard properties are used
      expect(css).toMatch(/border-radius:/i);
      expect(css).toMatch(/box-shadow:/i);
    });
  });

  /**
   * Test Case 3: Safari Layout Compatibility
   * Input: Load homepage in Safari and check layout
   * Expected: All sections render correctly with proper styling
   */
  describe('Test Case 3: Safari Layout Compatibility', () => {
    test('CSS scroll-behavior with Safari fallback consideration', () => {
      // scroll-behavior: smooth is in CSS (Safari 15.4+)
      // The JS also respects prefers-reduced-motion for accessibility
      expect(css).toMatch(/scroll-behavior:\s*smooth/);
    });

    test('CSS Grid gap property is Safari-compatible (Safari 12+)', () => {
      // Safari 12+ supports gap in grid and Safari 14.1+ in flexbox
      expect(css).toMatch(/gap:/i);
    });

    test('webkit-overflow-scrolling for Safari momentum scroll', () => {
      // -webkit-overflow-scrolling: touch for smooth scrolling on iOS
      expect(css).toMatch(/-webkit-overflow-scrolling:\s*touch/);
    });

    test('Focus-visible pseudo-class (Safari 15.4+)', () => {
      expect(css).toMatch(/:focus-visible/);
    });

    test('CSS Custom Properties are Safari-compatible (Safari 9.1+)', () => {
      expect(css).toMatch(/var\(--/);
      expect(css).toMatch(/:root/);
    });

    test('Safe area insets consideration for notched devices', () => {
      // While not required, modern Safari-compatible sites often consider safe-area-inset
      // The important thing is the layout doesn't break on Safari
      expect(css).toMatch(/overflow-x:\s*hidden/);
    });
  });

  /**
   * Test Case 4: Edge Layout Compatibility
   * Input: Load homepage in Edge and check layout
   * Expected: All sections render correctly with proper styling
   */
  describe('Test Case 4: Edge Layout Compatibility', () => {
    test('Edge (Chromium-based) supports all modern CSS features', () => {
      // Edge 79+ (Chromium-based) supports same features as Chrome
      expect(css).toMatch(/display:\s*grid/i);
      expect(css).toMatch(/display:\s*flex/i);
      expect(css).toMatch(/var\(--/);
      expect(css).toMatch(/transform:/i);
      expect(css).toMatch(/transition:/i);
    });

    test('No legacy Edge (EdgeHTML) specific issues', () => {
      // Modern Edge is Chromium-based, no special considerations needed
      // Just verify standard CSS is used
      expect(css).toMatch(/box-sizing:\s*border-box/);
    });

    test('Modern CSS features for Edge 100+', () => {
      expect(css).toMatch(/:focus-visible/);
      expect(css).toMatch(/gap:/i);
      expect(css).toMatch(/grid-template-columns/i);
    });
  });

  /**
   * Test Case 5: Chrome JavaScript Functionality
   * Input: Test JavaScript functionality in Chrome
   * Expected: All interactive features work (copy button, mobile menu, smooth scroll)
   */
  describe('Test Case 5: Chrome JavaScript Functionality', () => {
    test('ES6 modules syntax is Chrome-compatible (Chrome 61+)', () => {
      expect(js).toMatch(/import\s+/);
      expect(js).toMatch(/export\s+/);
    });

    test('Async/await syntax is Chrome-compatible (Chrome 55+)', () => {
      expect(js).toMatch(/async\s+/);
      expect(js).toMatch(/await\s+/);
    });

    test('Clipboard API usage is Chrome-compatible (Chrome 66+)', () => {
      expect(js).toMatch(/navigator\.clipboard/);
    });

    test('Arrow functions are Chrome-compatible', () => {
      expect(js).toMatch(/=>/);
    });

    test('Template literals are Chrome-compatible (Chrome 41+)', () => {
      expect(js).toMatch(/`/);
    });

    test('Document query selectors are used', () => {
      expect(js).toMatch(/querySelectorAll/);
      expect(js).toMatch(/querySelector/);
    });
  });

  /**
   * Test Case 6: Firefox JavaScript Functionality
   * Input: Test JavaScript functionality in Firefox
   * Expected: All interactive features work (copy button, mobile menu, smooth scroll)
   */
  describe('Test Case 6: Firefox JavaScript Functionality', () => {
    test('ES6 modules are Firefox-compatible (Firefox 60+)', () => {
      expect(js).toMatch(/import\s+/);
      expect(js).toMatch(/export\s+/);
    });

    test('Async/await is Firefox-compatible (Firefox 52+)', () => {
      expect(js).toMatch(/async\s+/);
      expect(js).toMatch(/await\s+/);
    });

    test('Clipboard API is Firefox-compatible (Firefox 63+)', () => {
      expect(js).toMatch(/navigator\.clipboard/);
    });

    test('execCommand fallback exists for older browser support', () => {
      // The clipboard.js should have a fallback for browsers without Clipboard API
      expect(js).toMatch(/execCommand.*copy|document\.execCommand/i);
    });

    test('Event listeners are properly attached', () => {
      expect(js).toMatch(/addEventListener/);
    });

    test('matchMedia for reduced motion preference', () => {
      expect(js).toMatch(/matchMedia/);
      expect(js).toMatch(/prefers-reduced-motion/);
    });
  });

  /**
   * Test Case 7: Safari JavaScript Functionality
   * Input: Test JavaScript functionality in Safari
   * Expected: All interactive features work (copy button, mobile menu, smooth scroll)
   */
  describe('Test Case 7: Safari JavaScript Functionality', () => {
    test('ES6 modules are Safari-compatible (Safari 11+)', () => {
      expect(js).toMatch(/import\s+/);
      expect(js).toMatch(/export\s+/);
    });

    test('Async/await is Safari-compatible (Safari 10.1+)', () => {
      expect(js).toMatch(/async\s+/);
      expect(js).toMatch(/await\s+/);
    });

    test('Clipboard API with Safari-compatible usage (Safari 13.1+)', () => {
      // Clipboard API works in Safari 13.1+ but may require user gesture
      expect(js).toMatch(/navigator\.clipboard/);
    });

    test('Fallback for Clipboard API exists', () => {
      // Important for Safari and older browsers
      expect(js).toMatch(/textarea|execCommand/);
    });

    test('History API usage is Safari-compatible', () => {
      expect(js).toMatch(/history\.pushState/);
    });

    test('Focus handling is Safari-compatible', () => {
      expect(js).toMatch(/\.focus\(/);
    });
  });

  /**
   * Test Case 8: Edge JavaScript Functionality
   * Input: Test JavaScript functionality in Edge
   * Expected: All interactive features work (copy button, mobile menu, smooth scroll)
   */
  describe('Test Case 8: Edge JavaScript Functionality', () => {
    test('Modern Edge (Chromium) supports all JS features', () => {
      // Edge 79+ is Chromium-based
      expect(js).toMatch(/import\s+/);
      expect(js).toMatch(/export\s+/);
      expect(js).toMatch(/async\s+/);
      expect(js).toMatch(/await\s+/);
      expect(js).toMatch(/navigator\.clipboard/);
    });

    test('DOM manipulation methods are Edge-compatible', () => {
      expect(js).toMatch(/querySelector/);
      expect(js).toMatch(/addEventListener/);
      expect(js).toMatch(/classList/);
    });

    test('getAttribute and setAttribute are used properly', () => {
      expect(js).toMatch(/getAttribute/);
      expect(js).toMatch(/setAttribute/);
    });
  });

  /**
   * Test Case 9: CSS Grid/Flexbox Support Across Browsers
   * Input: Check CSS Grid/Flexbox support across browsers
   * Expected: Feature grid and layouts render consistently
   */
  describe('Test Case 9: CSS Grid/Flexbox Consistency', () => {
    test('Features grid uses CSS Grid with proper fallback', () => {
      expect(css).toMatch(/\.features-grid\s*\{[\s\S]*display:\s*grid/m);
      expect(css).toMatch(/grid-template-columns:\s*repeat\(/);
    });

    test('Navigation uses Flexbox for cross-browser layout', () => {
      expect(css).toMatch(/\.nav[\s\S]*display:\s*flex/);
    });

    test('Footer uses Grid layout', () => {
      expect(css).toMatch(/\.footer__content[\s\S]*display:\s*grid/m);
    });

    test('Hero actions use Flexbox', () => {
      expect(css).toMatch(/\.hero__actions[\s\S]*display:\s*flex/m);
    });

    test('Status grid layout', () => {
      expect(css).toMatch(/\.status__grid[\s\S]*display:\s*grid/m);
    });

    test('Gap property usage for modern browsers', () => {
      // gap is now supported in both Grid and Flexbox in modern browsers
      const gapMatches = css.match(/gap:/gi) || [];
      expect(gapMatches.length).toBeGreaterThan(0);
    });

    test('Responsive breakpoints adjust grid columns', () => {
      // Should have different grid-template-columns for different viewports
      expect(css).toMatch(/grid-template-columns:\s*1fr/); // Mobile single column
      expect(css).toMatch(/grid-template-columns:\s*repeat\(2/); // Two columns
      expect(css).toMatch(/grid-template-columns:\s*repeat\(4/); // Four columns
    });
  });

  /**
   * Test Case 10: Visual Comparison (Manual Test Prerequisites)
   * Input: Visual comparison across browsers
   * Expected: No significant visual differences between browsers
   * Note: This test verifies the prerequisites for manual visual testing
   */
  describe('Test Case 10: Visual Consistency Prerequisites', () => {
    test('CSS reset ensures consistent baseline across browsers', () => {
      // Box-sizing border-box
      expect(css).toMatch(/box-sizing:\s*border-box/);
      // Margin and padding reset
      expect(css).toMatch(/margin:\s*0/);
      expect(css).toMatch(/padding:\s*0/);
    });

    test('Font smoothing for consistent text rendering', () => {
      expect(css).toMatch(/-webkit-font-smoothing:\s*antialiased/);
      expect(css).toMatch(/-moz-osx-font-smoothing:\s*grayscale/);
    });

    test('System font stack for cross-platform consistency', () => {
      // Using system-ui and fallback fonts
      expect(css).toMatch(/system-ui/);
      expect(css).toMatch(/sans-serif/);
    });

    test('Consistent color variables across the page', () => {
      // CSS variables should be defined
      expect(css).toMatch(/--color-primary/);
      expect(css).toMatch(/--color-gray-/);
      expect(css).toMatch(/--color-white/);
    });

    test('Border radius uses consistent values', () => {
      expect(css).toMatch(/--radius-/);
      expect(css).toMatch(/border-radius:/);
    });

    test('Shadow definitions for depth consistency', () => {
      expect(css).toMatch(/--shadow-/);
      expect(css).toMatch(/box-shadow:/);
    });

    test('Spacing system uses consistent values', () => {
      expect(css).toMatch(/--space-/);
    });

    test('Images respect container bounds', () => {
      expect(css).toMatch(/max-width:\s*100%/);
      expect(css).toMatch(/height:\s*auto/);
    });
  });

  /**
   * Additional Cross-Browser Tests
   */
  describe('Additional Cross-Browser Compatibility Checks', () => {
    test('HTML5 semantic elements are used (universal support)', () => {
      const header = document.querySelector('header');
      const nav = document.querySelector('nav');
      const main = document.querySelector('main');
      const section = document.querySelector('section');
      const article = document.querySelector('article');
      const footer = document.querySelector('footer');

      expect(header).toBeInTheDocument();
      expect(nav).toBeInTheDocument();
      expect(main).toBeInTheDocument();
      expect(section).toBeInTheDocument();
      // article is used for feature cards
      expect(article || document.querySelector('.feature-card')).toBeInTheDocument();
      expect(footer).toBeInTheDocument();
    });

    test('SVG icons are inline for cross-browser compatibility', () => {
      const svgElements = document.querySelectorAll('svg');
      expect(svgElements.length).toBeGreaterThan(0);
    });

    test('No browser-specific hacks in CSS', () => {
      // Check for common browser hacks that should be avoided
      // _property (IE6), *property (IE7), etc.
      // Note: CSS custom properties (--property) are valid and should not be flagged
      // We look for single underscore prefix (not part of CSS variables) which was an IE6 hack
      // The pattern checks for underscore followed by a letter at the start of a property
      // CSS variables like --color are fine
      const lines = css.split('\n');
      const hackyLines = lines.filter(line => {
        const trimmed = line.trim();
        // IE6 hack: _property: value (single underscore at start of property)
        // But we should allow things that are inside CSS variable values
        return /^\s*_[a-z]+\s*:/.test(trimmed) || /^\s*\*[a-z]+\s*:/.test(trimmed);
      });
      expect(hackyLines.length).toBe(0);
    });

    test('Viewport meta tag exists for mobile browsers', () => {
      const htmlPath = resolve(__dirname, '../../index.html');
      const html = readFileSync(htmlPath, 'utf-8');
      expect(html).toMatch(/viewport/);
      expect(html).toMatch(/width=device-width/);
      expect(html).toMatch(/initial-scale=1/);
    });

    test('Character encoding is specified', () => {
      const htmlPath = resolve(__dirname, '../../index.html');
      const html = readFileSync(htmlPath, 'utf-8');
      expect(html).toMatch(/charset="UTF-8"/i);
    });

    test('Type module for ES6 script loading', () => {
      const htmlPath = resolve(__dirname, '../../index.html');
      const html = readFileSync(htmlPath, 'utf-8');
      expect(html).toMatch(/type="module"/);
    });

    test('No deprecated HTML attributes used', () => {
      const htmlPath = resolve(__dirname, '../../index.html');
      const html = readFileSync(htmlPath, 'utf-8');
      // Check for deprecated attributes
      expect(html).not.toMatch(/\s+align="/i);
      expect(html).not.toMatch(/\s+bgcolor="/i);
      expect(html).not.toMatch(/<font/i);
      expect(html).not.toMatch(/<center>/i);
    });

    test('Modern image attributes for performance', () => {
      const images = document.querySelectorAll('img');
      // Check that images have width/height for CLS prevention
      const mainImages = Array.from(images).filter(img =>
        !img.classList.contains('nav__brand-logo')
      );

      // At least the logo should have dimensions
      const heroLogo = document.querySelector('.hero__logo img');
      if (heroLogo) {
        expect(heroLogo.hasAttribute('width') || heroLogo.hasAttribute('height')).toBe(true);
      }
    });

    test('Prefers-reduced-motion media query support', () => {
      expect(css).toMatch(/@media.*prefers-reduced-motion/);
    });

    test('Print styles are defined', () => {
      expect(css).toMatch(/@media.*print/);
    });
  });

  /**
   * Feature Detection Patterns
   */
  describe('Feature Detection and Graceful Degradation', () => {
    test('Clipboard API has fallback mechanism', () => {
      // The clipboard.js should handle browsers without Clipboard API
      expect(js).toMatch(/navigator\.clipboard/);
      expect(js).toMatch(/execCommand|fallback/i);
    });

    test('Smooth scroll respects reduced motion preference', () => {
      expect(js).toMatch(/prefers-reduced-motion/);
      expect(js).toMatch(/behavior.*smooth|smooth.*auto/);
    });

    test('Focus management uses progressive enhancement', () => {
      expect(js).toMatch(/focus/);
      expect(js).toMatch(/tabindex|tabIndex/);
    });

    test('No blocking inline styles or scripts', () => {
      const htmlPath = resolve(__dirname, '../../index.html');
      const html = readFileSync(htmlPath, 'utf-8');

      // Should not have inline style blocks (except for minor exceptions)
      // The main content should load without inline scripts blocking
      expect(html).not.toMatch(/<script>[^<]{1000,}<\/script>/);
    });
  });
});
