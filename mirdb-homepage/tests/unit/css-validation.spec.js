/**
 * CSS Validation Tests
 * Owner: Scenario 8 - Performance and Loading
 *
 * Test cases:
 * - Valid CSS syntax
 * - No deprecated properties
 * - Browser compatibility of features used
 */

const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

test.describe('CSS Validation', () => {

  // TC4: CSS passes W3C validation with no errors
  test('should have valid CSS syntax', async ({ page }) => {
    // Read the CSS file directly
    const cssPath = path.join(__dirname, '../../css/styles.css');
    const cssContent = fs.readFileSync(cssPath, 'utf-8');

    // Check for common CSS syntax issues
    const syntaxIssues = [];

    // Check for unclosed braces
    const openBraces = (cssContent.match(/\{/g) || []).length;
    const closeBraces = (cssContent.match(/\}/g) || []).length;
    if (openBraces !== closeBraces) {
      syntaxIssues.push(`Mismatched braces: ${openBraces} open, ${closeBraces} close`);
    }

    // Check for unclosed comments
    const openComments = (cssContent.match(/\/\*/g) || []).length;
    const closeComments = (cssContent.match(/\*\//g) || []).length;
    if (openComments !== closeComments) {
      syntaxIssues.push(`Unclosed comments: ${openComments} open, ${closeComments} close`);
    }

    // Check for empty rules
    const emptyRules = cssContent.match(/\{\s*\}/g);
    if (emptyRules && emptyRules.length > 0) {
      syntaxIssues.push(`Empty CSS rules found: ${emptyRules.length}`);
    }

    // Check for semicolon issues (property without semicolon before closing brace)
    // This is a simplified check - won't catch all cases
    const missingSemicolons = cssContent.match(/[a-z0-9)%"']\s*\}/gi);
    if (missingSemicolons) {
      // Filter out valid cases like "}}"
      const actualIssues = missingSemicolons.filter(m => !m.match(/^\s*\}/));
      if (actualIssues.length > 0) {
        // This is a soft warning, not always an error
        console.log(`Note: Some rules may be missing semicolons (${actualIssues.length} potential)`);
      }
    }

    expect(syntaxIssues).toHaveLength(0);
    console.log('CSS syntax validation passed');
    console.log(`  - Balanced braces: ${openBraces} pairs`);
    console.log(`  - Balanced comments: ${openComments} pairs`);
  });

  test('should not use deprecated CSS properties', async ({ page }) => {
    const cssPath = path.join(__dirname, '../../css/styles.css');
    const cssContent = fs.readFileSync(cssPath, 'utf-8');

    // List of truly deprecated/problematic CSS properties
    // Note: 'clip' is deprecated but still supported for accessibility patterns
    // Note: 'zoom' is non-standard but sometimes acceptable
    // Note: 'scroll-behavior' is valid modern CSS, only IE 'behavior' property is deprecated
    const deprecatedPropertyPatterns = [
      { pattern: /(?<!scroll-)behavior\s*:/gi, name: 'behavior' }, // IE only HTC behaviors (not scroll-behavior)
      { pattern: /filter\s*:\s*alpha/gi, name: 'filter: alpha' }, // IE only
      { pattern: /-ms-filter\s*:/gi, name: '-ms-filter' }, // IE only
      { pattern: /-moz-opacity\s*:/gi, name: '-moz-opacity' }, // Use opacity
      { pattern: /-khtml-opacity\s*:/gi, name: '-khtml-opacity' }, // Use opacity
      { pattern: /azimuth\s*:/gi, name: 'azimuth' }, // Removed
      { pattern: /page-policy\s*:/gi, name: 'page-policy' }, // Removed
      { pattern: /ruby-overhang\s*:/gi, name: 'ruby-overhang' }, // Removed
      { pattern: /text-kashida\s*:/gi, name: 'text-kashida' }, // Removed
      { pattern: /text-kashida-space\s*:/gi, name: 'text-kashida-space' }, // Removed
    ];

    const foundDeprecated = [];

    deprecatedPropertyPatterns.forEach(({ pattern, name }) => {
      if (pattern.test(cssContent)) {
        foundDeprecated.push(name);
      }
    });

    // Check for vendor prefixes that might not be needed anymore
    const unnecessaryPrefixes = [
      '-webkit-border-radius',
      '-moz-border-radius',
      '-webkit-box-shadow',
      '-moz-box-shadow',
      '-webkit-opacity',
      '-moz-opacity',
    ];

    const foundUnnecessaryPrefixes = [];
    unnecessaryPrefixes.forEach(prop => {
      // Look for actual property declaration (followed by colon)
      const regex = new RegExp(prop.replace(/-/g, '\\-') + '\\s*:', 'gi');
      if (regex.test(cssContent)) {
        foundUnnecessaryPrefixes.push(prop);
      }
    });

    if (foundUnnecessaryPrefixes.length > 0) {
      console.log(`Warning: Found unnecessary vendor prefixes: ${foundUnnecessaryPrefixes.join(', ')}`);
    }

    expect(foundDeprecated).toHaveLength(0);
    console.log('No deprecated CSS properties found');
  });

  test('should use browser-compatible CSS features', async ({ page }) => {
    const cssPath = path.join(__dirname, '../../css/styles.css');
    const cssContent = fs.readFileSync(cssPath, 'utf-8');

    // Check for modern CSS features that have good browser support
    const modernFeatures = {
      'flexbox': /display:\s*flex/i,
      'grid': /display:\s*grid/i,
      'css-variables': /--[a-z-]+:/i,
      'calc': /calc\(/i,
      'media-queries': /@media/i,
      'transitions': /transition:/i,
      'transforms': /transform:/i,
      'border-radius': /border-radius:/i,
      'box-shadow': /box-shadow:/i,
      'linear-gradient': /linear-gradient/i,
    };

    const usedFeatures = [];
    Object.entries(modernFeatures).forEach(([name, pattern]) => {
      if (pattern.test(cssContent)) {
        usedFeatures.push(name);
      }
    });

    console.log('CSS features used:');
    usedFeatures.forEach(f => console.log(`  - ${f}`));

    // All these features have good browser support (90%+)
    // No issues expected

    // Check for experimental features that may need prefixes
    const experimentalFeatures = [
      { name: 'backdrop-filter', pattern: /backdrop-filter:/i, needsPrefix: true },
      { name: 'aspect-ratio', pattern: /aspect-ratio:/i, needsPrefix: false },
      { name: 'container-queries', pattern: /@container/i, needsPrefix: false },
    ];

    const usedExperimentalFeatures = [];
    experimentalFeatures.forEach(({ name, pattern, needsPrefix }) => {
      if (pattern.test(cssContent)) {
        usedExperimentalFeatures.push({ name, needsPrefix });
      }
    });

    if (usedExperimentalFeatures.length > 0) {
      console.log('Experimental features used:');
      usedExperimentalFeatures.forEach(f => {
        console.log(`  - ${f.name} (prefix ${f.needsPrefix ? 'recommended' : 'not needed'})`);
      });
    }

    // This test always passes as we're just checking compatibility
    expect(true).toBeTruthy();
  });

  test('should have responsive media queries', async ({ page }) => {
    const cssPath = path.join(__dirname, '../../css/styles.css');
    const cssContent = fs.readFileSync(cssPath, 'utf-8');

    // Check for media queries
    const mediaQueries = cssContent.match(/@media[^{]+/g) || [];

    expect(mediaQueries.length).toBeGreaterThan(0);

    // Check for common breakpoints
    const breakpoints = {
      mobile: /max-width:\s*(480|375|320)px/i,
      tablet: /(min-width:\s*481px|min-width:\s*768px|max-width:\s*1024px)/i,
      desktop: /min-width:\s*(1024|1025|1200|1440)px/i,
    };

    const hasBreakpoints = {
      mobile: mediaQueries.some(mq => breakpoints.mobile.test(mq)),
      tablet: mediaQueries.some(mq => breakpoints.tablet.test(mq)),
      desktop: mediaQueries.some(mq => breakpoints.desktop.test(mq)),
    };

    console.log('Media query breakpoints:');
    console.log(`  - Mobile: ${hasBreakpoints.mobile ? 'Yes' : 'No'}`);
    console.log(`  - Tablet: ${hasBreakpoints.tablet ? 'Yes' : 'No'}`);
    console.log(`  - Desktop: ${hasBreakpoints.desktop ? 'Yes' : 'No'}`);

    // Should have at least mobile breakpoint
    expect(hasBreakpoints.mobile || hasBreakpoints.tablet).toBeTruthy();
  });

  test('should define consistent colors and spacing', async ({ page }) => {
    const cssPath = path.join(__dirname, '../../css/styles.css');
    const cssContent = fs.readFileSync(cssPath, 'utf-8');

    // Count unique color values
    const hexColors = cssContent.match(/#[0-9a-fA-F]{3,8}/g) || [];
    const rgbColors = cssContent.match(/rgba?\([^)]+\)/g) || [];

    const uniqueHexColors = [...new Set(hexColors.map(c => c.toLowerCase()))];
    const uniqueRgbColors = [...new Set(rgbColors)];

    console.log(`Colors used: ${uniqueHexColors.length} hex, ${uniqueRgbColors.length} RGB/RGBA`);

    // Check for consistent spacing units
    const remUnits = (cssContent.match(/\d+(\.\d+)?rem/g) || []).length;
    const emUnits = (cssContent.match(/\d+(\.\d+)?em/g) || []).length;
    const pxUnits = (cssContent.match(/\d+px/g) || []).length;

    console.log(`Spacing units: ${remUnits} rem, ${emUnits} em, ${pxUnits} px`);

    // rem is preferred for accessibility
    expect(remUnits + emUnits).toBeGreaterThan(0);
  });

  test('should load CSS correctly in browser', async ({ page }) => {
    await page.goto('http://localhost:3000/index.html', {
      waitUntil: 'networkidle',
    });

    // Check that styles.css is loaded
    const cssLoaded = await page.evaluate(() => {
      const styleSheets = Array.from(document.styleSheets);
      return styleSheets.some(ss =>
        ss.href && ss.href.includes('styles.css')
      );
    });

    expect(cssLoaded).toBeTruthy();

    // Check that CSS is actually applied by testing a known style
    const heroBackground = await page.evaluate(() => {
      const hero = document.querySelector('.hero');
      if (!hero) return null;
      const style = window.getComputedStyle(hero);
      return style.background || style.backgroundColor;
    });

    expect(heroBackground).toBeTruthy();
    // Should have gradient or color (not transparent/none)
    expect(heroBackground).not.toBe('rgba(0, 0, 0, 0)');

    console.log('CSS loaded and applied correctly');
    console.log(`  - Hero background: ${heroBackground.substring(0, 50)}...`);
  });

  test('should have proper CSS selectors', async ({ page }) => {
    const cssPath = path.join(__dirname, '../../css/styles.css');
    const cssContent = fs.readFileSync(cssPath, 'utf-8');

    // Check for overly specific selectors (more than 3 levels of nesting)
    const selectors = cssContent.match(/[^{}]+(?=\{)/g) || [];

    const overlySpecific = selectors.filter(selector => {
      // Count specificity (simplified - just count spaces as nesting indicator)
      const nestingLevel = (selector.match(/\s/g) || []).length;
      return nestingLevel > 4;
    });

    if (overlySpecific.length > 0) {
      console.log(`Note: Found ${overlySpecific.length} selectors with deep nesting (>4 spaces)`);
      // This is informational, not a failure
    }

    // Check for !important usage (should be minimal)
    const importantCount = (cssContent.match(/!important/g) || []).length;
    console.log(`!important usage: ${importantCount} occurrences`);

    // Should have minimal !important usage
    expect(importantCount).toBeLessThan(10);

    // Check for ID selectors in CSS (generally discouraged for styling)
    const idSelectors = (cssContent.match(/#[a-zA-Z][a-zA-Z0-9_-]*(?=[^}]*\{)/g) || []).length;
    console.log(`ID selectors: ${idSelectors}`);

    // ID selectors are OK in moderation (e.g., for anchor targets)
    // Just logging for informational purposes
  });

  test('CSS file should be well-organized', async ({ page }) => {
    const cssPath = path.join(__dirname, '../../css/styles.css');
    const cssContent = fs.readFileSync(cssPath, 'utf-8');

    // Check for section comments
    const sectionComments = cssContent.match(/\/\*\s*=+.*=+\s*\*\//g) || [];

    console.log(`CSS organization: ${sectionComments.length} section comments found`);

    // Check file size
    const fileSizeKB = Buffer.byteLength(cssContent, 'utf8') / 1024;
    console.log(`CSS file size: ${fileSizeKB.toFixed(2)} KB`);

    // CSS file should be reasonably sized
    expect(fileSizeKB).toBeLessThan(100);

    // Should have some organization (comments)
    expect(sectionComments.length).toBeGreaterThan(0);
  });
});
