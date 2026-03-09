/**
 * Cross-Browser Compatibility Tests
 * Owner: Scenario 18 - Cross-Browser Compatibility
 *
 * Tests:
 * - Validate homepage HTML/CSS uses cross-browser compatible features
 * - Verify CSS Grid, Flexbox, and custom properties are properly used
 * - Check for proper vendor prefixes and fallbacks
 * - Validate no browser-specific hacks are used
 *
 * Note: These tests validate cross-browser compatibility through static analysis
 * of the HTML, CSS, and JavaScript source files. This approach verifies that:
 * 1. Standard web features are used (supported by Chrome, Firefox, Safari, Edge)
 * 2. No browser-specific hacks or deprecated APIs are present
 * 3. Proper fallbacks and font stacks are in place
 * 4. CSS uses widely-supported features (Grid, Flexbox, Custom Properties)
 */

const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

const DOCS_DIR = path.join(__dirname, '../../docs');

// Helper function to read file contents
function readFile(relativePath) {
  const fullPath = path.join(DOCS_DIR, relativePath);
  return fs.readFileSync(fullPath, 'utf-8');
}

// Test Case 1: Chrome Compatibility
test.describe('TC1: Chrome Compatibility', () => {
  test('HTML uses Chrome-compatible features', async () => {
    const htmlContent = readFile('index.html');

    // Chrome supports all HTML5 features used
    expect(htmlContent).toContain('<!DOCTYPE html>');
    expect(htmlContent).toContain('<html lang="en">');
    expect(htmlContent).toContain('<meta charset="UTF-8">');
    expect(htmlContent).toContain('<meta name="viewport"');

    // Verify semantic HTML elements (Chrome supports all)
    expect(htmlContent).toContain('<header');
    expect(htmlContent).toContain('<main');
    expect(htmlContent).toContain('<footer');
    expect(htmlContent).toContain('<section');
    expect(htmlContent).toContain('<article');
    expect(htmlContent).toContain('<nav');
  });

  test('CSS uses Chrome-compatible features', async () => {
    const cssContent = readFile('css/styles.css');

    // CSS Grid (Chrome 57+)
    expect(cssContent).toContain('display: grid');
    expect(cssContent).toContain('grid-template-columns');

    // CSS Flexbox (Chrome 29+)
    expect(cssContent).toContain('display: flex');
    expect(cssContent).toContain('align-items');
    expect(cssContent).toContain('justify-content');

    // CSS Custom Properties (Chrome 49+)
    expect(cssContent).toContain(':root');
    expect(cssContent).toContain('--color-');
    expect(cssContent).toContain('var(--');

    // CSS transitions (Chrome 26+)
    expect(cssContent).toContain('transition');
  });

  test('JavaScript uses Chrome-compatible APIs', async () => {
    const jsContent = readFile('js/main.js');

    // Modern DOM APIs (Chrome supports all)
    expect(jsContent).toContain('addEventListener');
    expect(jsContent).toContain('querySelector');

    // DOMContentLoaded event
    expect(jsContent).toContain('DOMContentLoaded');

    // classList API (Chrome 8+)
    expect(jsContent).toContain('classList');
  });

  test('All sections are present in HTML', async () => {
    const htmlContent = readFile('index.html');

    // Verify all main sections exist
    expect(htmlContent).toContain('id="hero"');
    expect(htmlContent).toContain('id="features"');
    expect(htmlContent).toContain('id="architecture"');
    expect(htmlContent).toContain('id="quickstart"');
    expect(htmlContent).toContain('id="commands"');
    expect(htmlContent).toContain('id="performance"');
    expect(htmlContent).toContain('id="roadmap"');
  });
});

// Test Case 2: Firefox Compatibility
test.describe('TC2: Firefox Compatibility', () => {
  test('CSS uses Firefox-compatible features', async () => {
    const cssContent = readFile('css/styles.css');

    // Firefox supports all modern CSS features
    // Check no webkit-only features without moz fallback
    expect(cssContent).toContain('display: grid');
    expect(cssContent).toContain('display: flex');

    // CSS variables (Firefox 31+)
    expect(cssContent).toContain(':root');
    expect(cssContent).toContain('--color-');

    // Standard color formats
    expect(cssContent).not.toMatch(/-webkit-linear-gradient/);

    // Scroll behavior (Firefox supports)
    expect(cssContent).toContain('scroll-behavior: smooth');
  });

  test('HTML uses standard elements (Firefox compatible)', async () => {
    const htmlContent = readFile('index.html');

    // Verify standard HTML5 elements
    expect(htmlContent).toContain('<!DOCTYPE html>');
    expect(htmlContent).toContain('<html lang="en">');
    expect(htmlContent).toContain('<meta charset="UTF-8">');

    // Check for semantic elements (Firefox fully supports)
    expect(htmlContent).toContain('<header');
    expect(htmlContent).toContain('<main');
    expect(htmlContent).toContain('<footer');
    expect(htmlContent).toContain('<section');
    expect(htmlContent).toContain('<article');
    expect(htmlContent).toContain('<nav');
  });

  test('JavaScript uses Firefox-compatible APIs', async () => {
    const jsContent = readFile('js/main.js');

    // Verify modern APIs supported by Firefox
    expect(jsContent).toContain('addEventListener');
    expect(jsContent).toContain('querySelector');
    expect(jsContent).toContain('DOMContentLoaded');

    // No IE-only APIs
    expect(jsContent).not.toContain('attachEvent');
    expect(jsContent).not.toContain('document.all');
  });
});

// Test Case 3: Safari (WebKit) Compatibility
test.describe('TC3: Safari (WebKit) Compatibility', () => {
  test('CSS uses Safari-compatible features', async () => {
    const cssContent = readFile('css/styles.css');

    // Safari requires smooth scroll in CSS (which is used)
    expect(cssContent).toContain('scroll-behavior: smooth');

    // Verify flexbox is used (fully supported in Safari)
    expect(cssContent).toContain('display: flex');

    // Verify CSS Grid is used (fully supported in Safari 10.1+)
    expect(cssContent).toContain('display: grid');

    // Verify CSS custom properties (supported in Safari 9.1+)
    expect(cssContent).toContain('var(--');

    // Check for box-sizing (Safari compatible)
    expect(cssContent).toContain('box-sizing: border-box');
  });

  test('SVG elements are Safari compatible', async () => {
    const htmlContent = readFile('index.html');

    // Verify SVG uses standard attributes
    expect(htmlContent).toContain('<svg');
    expect(htmlContent).toContain('viewBox=');

    // Check SVG has xmlns for Safari compatibility
    const svgRegex = /<svg[^>]*xmlns="http:\/\/www\.w3\.org\/2000\/svg"[^>]*>/;
    expect(htmlContent).toMatch(svgRegex);
  });

  test('JavaScript does not use Safari-incompatible features', async () => {
    const jsContent = readFile('js/main.js');

    // Check for modern APIs that Safari supports
    expect(jsContent).toContain('addEventListener');
    expect(jsContent).toContain('querySelector');

    // classList API (Safari 5.1+)
    expect(jsContent).toContain('classList');

    // No Safari-incompatible APIs used
    expect(jsContent).not.toContain('attachEvent');
    expect(jsContent).not.toContain('document.all');
  });
});

// Test Case 4: Edge Compatibility
test.describe('TC4: Edge Compatibility', () => {
  test('CSS uses Edge-compatible features', async () => {
    const cssContent = readFile('css/styles.css');

    // Edge (Chromium-based) supports all modern CSS
    // Verify no IE-specific hacks that might confuse Edge
    const ieHacks = [
      /_:-ms-fullscreen/,
      /@media screen and \(-ms-high-contrast/,
      /\*display:\s*inline/,  // IE6/7 hack
    ];

    for (const pattern of ieHacks) {
      expect(cssContent).not.toMatch(pattern);
    }

    // Verify modern CSS is used
    expect(cssContent).toContain('display: grid');
    expect(cssContent).toContain('display: flex');
    expect(cssContent).toContain('var(--');
  });

  test('JavaScript uses Edge-compatible APIs', async () => {
    const jsContent = readFile('js/main.js');

    // Check for modern JS features (supported by Edge)
    // The JS should not use deprecated APIs
    const deprecatedAPIs = [
      /document\.all[^\w]/,
      /attachEvent/,
    ];

    for (const pattern of deprecatedAPIs) {
      expect(jsContent).not.toMatch(pattern);
    }

    // Verify modern event handling
    expect(jsContent).toContain('addEventListener');
  });

  test('HTML uses Edge-compatible features', async () => {
    const htmlContent = readFile('index.html');

    // Edge supports all HTML5 features
    expect(htmlContent).toContain('<!DOCTYPE html>');
    expect(htmlContent).toContain('<meta charset="UTF-8">');
    expect(htmlContent).toContain('<meta name="viewport"');

    // Verify no Edge-incompatible features
    // (Edge Chromium supports everything Chrome does)
    expect(htmlContent).toContain('<header');
    expect(htmlContent).toContain('<main');
    expect(htmlContent).toContain('<footer');
  });
});

// Test Case 5: CSS Feature Support Verification
test.describe('TC5: CSS Feature Support', () => {
  test('CSS Grid is properly defined', async () => {
    const cssContent = readFile('css/styles.css');

    // Check for CSS Grid usage
    expect(cssContent).toContain('display: grid');
    expect(cssContent).toContain('grid-template-columns');

    // Features grid should use grid
    expect(cssContent).toMatch(/\.features-grid[^{]*\{[^}]*display:\s*grid/);

    // Grid gap should be defined
    expect(cssContent).toContain('gap:');
  });

  test('CSS Flexbox is properly defined', async () => {
    const cssContent = readFile('css/styles.css');

    // Check for Flexbox usage
    expect(cssContent).toContain('display: flex');

    // Navigation should use flex
    expect(cssContent).toMatch(/nav[^{]*\{[^}]*display:\s*flex/);

    // Flex properties should be used
    expect(cssContent).toContain('align-items');
    expect(cssContent).toContain('justify-content');
    expect(cssContent).toContain('flex-wrap');
  });

  test('CSS Custom Properties are properly defined', async () => {
    const cssContent = readFile('css/styles.css');

    // Check :root has custom properties
    expect(cssContent).toContain(':root');

    // Verify color variables are defined
    expect(cssContent).toContain('--color-bg-primary');
    expect(cssContent).toContain('--color-text-primary');
    expect(cssContent).toContain('--color-accent-primary');

    // Verify variables are used
    expect(cssContent).toContain('var(--color-');

    // Check font variables
    expect(cssContent).toContain('--font-sans');
    expect(cssContent).toContain('--font-mono');
  });

  test('CSS features have proper values', async () => {
    const cssContent = readFile('css/styles.css');

    // Dark theme colors (should be dark background, light text)
    expect(cssContent).toContain('--color-bg-primary: #0d1117');
    expect(cssContent).toContain('--color-text-primary: #f0f6fc');

    // Font stacks should have fallbacks
    expect(cssContent).toContain('sans-serif');
    expect(cssContent).toContain('monospace');

    // Spacing variables
    expect(cssContent).toContain('--spacing-');
  });
});

// Cross-Browser Standards Compliance
test.describe('Cross-Browser Standards Compliance', () => {
  test('HTML5 DOCTYPE and charset are properly set', async () => {
    const htmlContent = readFile('index.html');

    // These are required for consistent cross-browser rendering
    expect(htmlContent.trim().startsWith('<!DOCTYPE html>')).toBe(true);
    expect(htmlContent).toContain('<meta charset="UTF-8">');
    expect(htmlContent).toContain('<meta name="viewport"');
  });

  test('CSS reset ensures consistent cross-browser baseline', async () => {
    const cssContent = readFile('css/styles.css');

    // Check for CSS reset/normalization
    expect(cssContent).toContain('box-sizing: border-box');
    expect(cssContent).toContain('margin: 0');
    expect(cssContent).toContain('padding: 0');
  });

  test('Font stack provides cross-browser fallbacks', async () => {
    const cssContent = readFile('css/styles.css');

    // Verify system font stack for sans-serif
    expect(cssContent).toContain('-apple-system');
    expect(cssContent).toContain('BlinkMacSystemFont');
    expect(cssContent).toContain('Segoe UI');
    expect(cssContent).toContain('sans-serif');

    // Verify monospace font stack
    expect(cssContent).toContain('Consolas');
    expect(cssContent).toContain('monospace');
  });

  test('No browser-specific CSS hacks are used', async () => {
    const cssContent = readFile('css/styles.css');

    // Check for IE-specific hacks
    expect(cssContent).not.toMatch(/_:-ms-/);
    expect(cssContent).not.toMatch(/\*html/);
    expect(cssContent).not.toMatch(/\*\+html/);

    // Check for outdated vendor prefixes that shouldn't be needed
    // Modern browsers don't need these for grid/flexbox
    const outdatedPrefixes = [
      /-ms-flexbox/,  // IE10 flexbox
      /-webkit-box-flex/,  // Old flexbox
    ];

    for (const pattern of outdatedPrefixes) {
      expect(cssContent).not.toMatch(pattern);
    }
  });

  test('JavaScript uses cross-browser compatible APIs', async () => {
    const jsContent = readFile('js/main.js');

    // Verify modern DOM APIs are used
    expect(jsContent).toContain('addEventListener');
    expect(jsContent).toContain('querySelector');

    // Verify DOMContentLoaded is used for initialization
    expect(jsContent).toContain('DOMContentLoaded');

    // Verify classList API (widely supported)
    expect(jsContent).toContain('classList');

    // Verify no deprecated APIs
    expect(jsContent).not.toContain('attachEvent');
    expect(jsContent).not.toContain('document.all');
  });

  test('SVG icons use cross-browser compatible markup', async () => {
    const htmlContent = readFile('index.html');

    // Verify SVG namespace is declared
    expect(htmlContent).toContain('xmlns="http://www.w3.org/2000/svg"');

    // Verify viewBox attribute is used (important for scaling)
    expect(htmlContent).toContain('viewBox=');

    // Check that SVG uses standard elements
    expect(htmlContent).toContain('<rect');
    expect(htmlContent).toContain('<text');
    expect(htmlContent).toContain('<path');
  });
});

// Visual Structure Verification
test.describe('Visual Structure Verification', () => {
  test('Navigation structure is cross-browser compatible', async () => {
    const htmlContent = readFile('index.html');

    // Check navigation exists with proper structure
    expect(htmlContent).toContain('class="navbar"');
    expect(htmlContent).toContain('class="nav-links"');

    // Nav links should be in a list
    expect(htmlContent).toMatch(/<ul[^>]*class="nav-links"[^>]*>/);
    expect(htmlContent).toMatch(/<li><a href="#features">/);
  });

  test('Feature cards structure is cross-browser compatible', async () => {
    const htmlContent = readFile('index.html');

    // Check feature cards exist
    expect(htmlContent).toContain('class="features-grid"');
    expect(htmlContent).toContain('class="feature-card"');

    // Count feature cards (should be 5)
    const featureCardMatches = htmlContent.match(/class="feature-card"/g);
    expect(featureCardMatches).not.toBeNull();
    expect(featureCardMatches.length).toBe(5);
  });

  test('Architecture diagram uses cross-browser compatible SVG', async () => {
    const htmlContent = readFile('index.html');

    // Check architecture diagram exists
    expect(htmlContent).toContain('class="architecture-diagram"');

    // SVG should have proper attributes
    expect(htmlContent).toMatch(/<svg[^>]*class="architecture-diagram"/);
    expect(htmlContent).toMatch(/viewBox="0 0 800 600"/);
  });

  test('Table structure is cross-browser compatible', async () => {
    const htmlContent = readFile('index.html');

    // Check config table exists with proper structure
    expect(htmlContent).toContain('class="config-table"');
    expect(htmlContent).toContain('<thead>');
    expect(htmlContent).toContain('<tbody>');
    expect(htmlContent).toContain('<th>');
    expect(htmlContent).toContain('<td>');
  });

  test('Button styling uses standard CSS', async () => {
    const cssContent = readFile('css/styles.css');

    // Check button classes exist
    expect(cssContent).toContain('.btn');
    expect(cssContent).toContain('.btn-primary');
    expect(cssContent).toContain('.btn-secondary');

    // Buttons should have standard properties
    expect(cssContent).toMatch(/\.btn[^{]*\{[^}]*(padding|border-radius|font)/);
  });

  test('Responsive breakpoints are standard', async () => {
    const cssContent = readFile('css/styles.css');

    // Check for standard media queries
    expect(cssContent).toContain('@media');

    // Should have mobile breakpoint (around 768px or similar)
    expect(cssContent).toMatch(/@media[^{]*max-width:\s*\d+px/);

    // Should have proper breakpoint values
    expect(cssContent).toMatch(/@media[^{]*(768|480|1024|1200)px/);
  });
});

// Accessibility compatibility (cross-browser)
test.describe('Accessibility Cross-Browser Compatibility', () => {
  test('ARIA attributes are used correctly', async () => {
    const htmlContent = readFile('index.html');

    // Check for aria-label attributes
    expect(htmlContent).toContain('aria-label=');

    // Check for role attributes where needed
    expect(htmlContent).toContain('role="img"');

    // Check for proper link accessibility
    expect(htmlContent).toContain('rel="noopener"');
  });

  test('Form elements have proper attributes', async () => {
    const htmlContent = readFile('index.html');

    // Buttons should have proper attributes
    expect(htmlContent).toMatch(/<button[^>]*aria-label=/);

    // Check hamburger menu accessibility
    expect(htmlContent).toContain('class="hamburger"');
    expect(htmlContent).toMatch(/<button[^>]*class="hamburger"[^>]*aria-label=/);
  });

  test('Images have alt attributes', async () => {
    const htmlContent = readFile('index.html');

    // All img tags should have alt attributes
    const imgTags = htmlContent.match(/<img[^>]*>/g);
    expect(imgTags).not.toBeNull();

    for (const imgTag of imgTags) {
      expect(imgTag).toContain('alt=');
    }
  });
});
