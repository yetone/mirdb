/**
 * CSS Browser Compatibility Unit Tests
 * Owner: Scenario 11 - Cross-Browser Compatibility
 *
 * Unit tests that validate CSS features used are compatible with modern browsers
 * without requiring browser execution (static analysis).
 *
 * Test cases:
 * - CSS features compatibility (no deprecated or unsupported features)
 * - Font rendering with fallbacks defined
 */
const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

// Read CSS file content
const cssPath = path.join(__dirname, '../../css/styles.css');
const cssContent = fs.readFileSync(cssPath, 'utf-8');

// Read HTML file content
const htmlPath = path.join(__dirname, '../../index.html');
const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

test.describe('CSS Browser Compatibility - Static Analysis', () => {
  test('TC5: CSS uses only modern, well-supported features', () => {
    // List of CSS features used that are widely supported (97%+ browser support)
    const supportedFeatures = {
      flexbox: /display:\s*flex/g,
      grid: /display:\s*grid/g,
      stickyPosition: /position:\s*sticky/g,
      borderRadius: /border-radius:/g,
      boxShadow: /box-shadow:/g,
      transitions: /transition:/g,
      gradients: /linear-gradient/g,
      boxSizing: /box-sizing:/g,
      gridTemplateColumns: /grid-template-columns:/g
    };

    // Verify each modern feature is used
    for (const [feature, regex] of Object.entries(supportedFeatures)) {
      const matches = cssContent.match(regex);
      expect(matches, `CSS should use ${feature}`).toBeTruthy();
    }

    // Verify NO deprecated or unsupported features
    const deprecatedFeatures = [
      /-webkit-box\s/g,           // Old flexbox syntax
      /display:\s*-webkit-box/g,  // Old flexbox
      /display:\s*-ms-flexbox/g,  // Old IE flexbox
      /-ms-grid/g,                // Old IE Grid
      /filter:\s*alpha/g,         // Old IE filter
      /zoom:/g,                   // Non-standard zoom
    ];

    for (const regex of deprecatedFeatures) {
      const matches = cssContent.match(regex);
      expect(matches, `CSS should not use deprecated features: ${regex}`).toBeFalsy();
    }
  });

  test('TC5b: No browser-specific vendor prefixes required', () => {
    // Modern CSS features don't need vendor prefixes
    // Verify we're not using unnecessary prefixes that indicate legacy browser targeting
    const unnecessaryPrefixes = [
      /-webkit-flex/g,      // Modern flexbox doesn't need prefix
      /-webkit-grid/g,      // Grid doesn't need prefix
      /-webkit-sticky/g,    // Sticky doesn't need prefix in modern browsers
      /-moz-flex/g,         // Firefox flexbox
      /-ms-flex/g,          // IE flexbox
    ];

    for (const regex of unnecessaryPrefixes) {
      const matches = cssContent.match(regex);
      expect(matches, `CSS should not use unnecessary prefix: ${regex}`).toBeFalsy();
    }
  });

  test('TC6: Font rendering with proper fallback chain', () => {
    // Check for system font stack in body
    const hasSystemFonts = cssContent.includes('-apple-system') ||
                           cssContent.includes('BlinkMacSystemFont') ||
                           cssContent.includes('Segoe UI') ||
                           cssContent.includes('system-ui');

    expect(hasSystemFonts, 'CSS should include system font stack').toBe(true);

    // Check for sans-serif fallback
    expect(cssContent, 'CSS should include sans-serif fallback').toContain('sans-serif');

    // Check for monospace font stack for code blocks
    const hasMonospaceFonts = cssContent.includes('Consolas') ||
                              cssContent.includes('Menlo') ||
                              cssContent.includes('SFMono');

    expect(hasMonospaceFonts, 'CSS should include monospace font stack').toBe(true);

    // Check for monospace fallback
    expect(cssContent, 'CSS should include monospace fallback').toContain('monospace');
  });

  test('TC6b: Font families have complete fallback chains', () => {
    // Extract all font-family declarations
    const fontFamilyRegex = /font-family:\s*([^;]+);/g;
    const fontFamilyMatches = cssContent.matchAll(fontFamilyRegex);

    for (const match of fontFamilyMatches) {
      const fontStack = match[1];

      // Each font stack should have multiple fonts (fallbacks)
      const fonts = fontStack.split(',').map(f => f.trim());
      expect(fonts.length, `Font stack "${fontStack}" should have fallbacks`).toBeGreaterThan(1);

      // Each stack should end with a generic family
      const lastFont = fonts[fonts.length - 1].toLowerCase();
      const genericFamilies = ['sans-serif', 'serif', 'monospace', 'cursive', 'fantasy', 'system-ui'];
      const hasGenericFallback = genericFamilies.some(g => lastFont.includes(g));
      expect(hasGenericFallback, `Font stack "${fontStack}" should end with generic family`).toBe(true);
    }
  });

  test('TC1-4: HTML structure is valid for cross-browser rendering', () => {
    // Check for proper HTML5 doctype
    expect(htmlContent.toLowerCase(), 'HTML should have HTML5 doctype').toContain('<!doctype html>');

    // Check for viewport meta tag (important for mobile browsers)
    expect(htmlContent, 'HTML should have viewport meta tag').toContain('viewport');
    expect(htmlContent, 'HTML should have width=device-width').toContain('width=device-width');

    // Check for charset declaration
    expect(htmlContent.toLowerCase(), 'HTML should have charset declaration').toContain('charset');
    expect(htmlContent, 'HTML should use UTF-8').toContain('UTF-8');

    // Check for lang attribute on html element
    expect(htmlContent, 'HTML should have lang attribute').toMatch(/<html[^>]+lang=/);

    // All required sections exist
    const requiredSections = [
      '#hero',
      '#features',
      '#getting-started',
      '#architecture',
      '#configuration',
      '#commands',
      '#status',
      '<nav',
      '<footer'
    ];

    for (const section of requiredSections) {
      if (section.startsWith('#')) {
        expect(htmlContent, `HTML should have ${section} section`).toContain(`id="${section.substring(1)}"`);
      } else {
        expect(htmlContent, `HTML should have ${section} element`).toContain(section);
      }
    }
  });

  test('TC7: SVG icons use standard attributes', () => {
    // Check that SVGs use standard attributes (not deprecated)
    const svgRegex = /<svg[^>]*>/g;
    const svgMatches = htmlContent.matchAll(svgRegex);

    for (const match of svgMatches) {
      const svgTag = match[0];

      // Should not have deprecated attributes
      expect(svgTag, 'SVG should not use deprecated enable-background').not.toContain('enable-background');
      expect(svgTag, 'SVG should not use deprecated xml:space').not.toContain('xml:space');
    }
  });

  test('TC10: External links have security attributes', () => {
    // Find external links (target="_blank")
    const externalLinkRegex = /<a[^>]*target="_blank"[^>]*>/g;
    const linkMatches = htmlContent.matchAll(externalLinkRegex);

    for (const match of linkMatches) {
      const linkTag = match[0];

      // Should have rel="noopener" for security (prevents tab-napping)
      expect(linkTag, `External link should have noopener: ${linkTag.substring(0, 50)}`).toContain('noopener');
    }
  });

  test('TC8: Tables use standard accessible markup', () => {
    // Check tables have thead for cross-browser styling support
    expect(htmlContent, 'Tables should have thead element').toContain('<thead>');
    expect(htmlContent, 'Tables should have tbody element').toContain('<tbody>');

    // Check for scope attributes on th elements (accessibility & browser consistency)
    expect(htmlContent, 'Table headers should have scope attribute').toMatch(/<th[^>]*scope=/);
  });

  test('TC9: Smooth scroll is CSS-based (cross-browser support)', () => {
    // Check for CSS scroll-behavior
    expect(cssContent, 'CSS should define scroll-behavior: smooth').toContain('scroll-behavior: smooth');
  });
});
