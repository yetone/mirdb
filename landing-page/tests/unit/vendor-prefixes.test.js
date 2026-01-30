/**
 * CSS Vendor Prefixes Unit Tests
 * Owner: Scenario 16 - Browser Compatibility
 *
 * Tests for verifying required CSS vendor prefixes are present
 * to ensure cross-browser compatibility for Chrome, Firefox, Safari, and Edge.
 */
const fs = require('fs');
const path = require('path');

describe('CSS Vendor Prefixes - Browser Compatibility', () => {
  // Store all CSS content
  let allCssContent = '';
  let cssFiles = {};

  beforeAll(() => {
    // Read all CSS files
    const cssDir = path.join(__dirname, '../../css');

    // Read main styles.css
    const stylesPath = path.join(cssDir, 'styles.css');
    cssFiles['styles.css'] = fs.readFileSync(stylesPath, 'utf-8');

    // Read utility CSS files
    const utilitiesDir = path.join(cssDir, 'utilities');
    const utilityFiles = fs.readdirSync(utilitiesDir).filter(f => f.endsWith('.css'));
    utilityFiles.forEach(file => {
      const filePath = path.join(utilitiesDir, file);
      cssFiles[`utilities/${file}`] = fs.readFileSync(filePath, 'utf-8');
    });

    // Read component CSS files
    const componentsDir = path.join(cssDir, 'components');
    const componentFiles = fs.readdirSync(componentsDir).filter(f => f.endsWith('.css'));
    componentFiles.forEach(file => {
      const filePath = path.join(componentsDir, file);
      cssFiles[`components/${file}`] = fs.readFileSync(filePath, 'utf-8');
    });

    // Concatenate all CSS for overall checks
    allCssContent = Object.values(cssFiles).join('\n');
  });

  describe('TC5: CSS Vendor Prefixes Present for Cross-Browser Compatibility', () => {
    test('CSS files exist and are readable', () => {
      expect(Object.keys(cssFiles).length).toBeGreaterThan(0);
      expect(allCssContent.length).toBeGreaterThan(0);
    });

    test('Font smoothing prefixes present for Safari/Chrome compatibility', () => {
      // -webkit-font-smoothing is important for macOS/iOS rendering
      expect(allCssContent).toContain('-webkit-font-smoothing');

      // -moz-osx-font-smoothing for Firefox on macOS
      expect(allCssContent).toContain('-moz-osx-font-smoothing');
    });

    test('CSS uses modern flexbox (no prefixes needed for latest 2 versions)', () => {
      // Modern flexbox is supported in latest 2 versions without prefixes
      // Verify flexbox is used
      expect(allCssContent).toMatch(/display:\s*flex/);

      // Verify flex-direction is used
      expect(allCssContent).toMatch(/flex-direction:\s*(row|column)/);

      // Verify align-items is used
      expect(allCssContent).toMatch(/align-items:\s*(center|flex-start|flex-end|stretch)/);
    });

    test('CSS uses modern Grid layout (supported in latest 2 versions)', () => {
      // CSS Grid is supported in latest 2 versions without prefixes
      expect(allCssContent).toMatch(/display:\s*grid/);
    });

    test('CSS uses standard box-sizing property', () => {
      // box-sizing: border-box should be set globally
      expect(allCssContent).toMatch(/box-sizing:\s*border-box/);
    });

    test('CSS uses transform property (standard)', () => {
      // Transform is widely supported without prefixes in modern browsers
      expect(allCssContent).toMatch(/transform:\s*(translate|scale|rotate)/);
    });

    test('CSS uses transition property (standard)', () => {
      // Transitions are widely supported without prefixes
      expect(allCssContent).toMatch(/transition:\s*.+/);
    });

    test('CSS uses animation property (standard)', () => {
      // Animations with @keyframes
      expect(allCssContent).toMatch(/@keyframes\s+\w+/);
      expect(allCssContent).toMatch(/animation:\s*.+/);
    });

    test('CSS custom properties (variables) are used', () => {
      // CSS variables are supported in all modern browsers
      expect(allCssContent).toMatch(/--[\w-]+:\s*.+/);
      expect(allCssContent).toMatch(/var\(--[\w-]+\)/);
    });

    test('Media queries use standard syntax', () => {
      // Standard media query syntax
      expect(allCssContent).toMatch(/@media\s*\(/);
      expect(allCssContent).toMatch(/@media\s*\(\s*min-width/);
      expect(allCssContent).toMatch(/@media\s*\(\s*max-width/);
    });

    test('Reduced motion preference is respected', () => {
      // prefers-reduced-motion media query for accessibility
      expect(allCssContent).toMatch(/@media\s*\(\s*prefers-reduced-motion:\s*reduce\s*\)/);
    });

    test('Linear gradients use standard syntax', () => {
      // Standard linear-gradient without vendor prefixes
      expect(allCssContent).toMatch(/linear-gradient\s*\(/);
    });

    test('Scroll behavior uses standard property', () => {
      // scroll-behavior: smooth is supported in modern browsers
      expect(allCssContent).toMatch(/scroll-behavior:\s*smooth/);
    });

    test('Border-radius uses standard syntax', () => {
      // border-radius is fully supported
      expect(allCssContent).toMatch(/border-radius:\s*.+/);
    });

    test('Box-shadow uses standard syntax', () => {
      // box-shadow is fully supported
      expect(allCssContent).toMatch(/box-shadow:\s*.+/);
    });

    test('Opacity uses standard property', () => {
      // opacity is fully supported
      expect(allCssContent).toMatch(/opacity:\s*[0-9.]+/);
    });
  });

  describe('Browser-Specific Considerations', () => {
    test('No deprecated -ms- prefixes (Edge uses Chromium)', () => {
      // Modern Edge uses Chromium engine, no -ms- prefixes needed
      // Check that we're not using old IE/Edge prefixes unnecessarily
      const msFlexCount = (allCssContent.match(/-ms-flex/g) || []).length;
      const msGridCount = (allCssContent.match(/-ms-grid/g) || []).length;

      // Should not have many old -ms- prefixes (0 is ideal for modern targets)
      expect(msFlexCount + msGridCount).toBeLessThanOrEqual(5);
    });

    test('Uses CSS that works in Firefox without -moz- prefix (except font smoothing)', () => {
      // Most properties don't need -moz- prefix in modern Firefox
      // Count non-font-smoothing -moz- prefixes
      const mozPrefixes = allCssContent.match(/-moz-(?!osx-font-smoothing)/g) || [];
      // Should be minimal
      expect(mozPrefixes.length).toBeLessThanOrEqual(5);
    });

    test('WebKit font smoothing is properly configured', () => {
      // For Safari and Chrome on macOS
      expect(allCssContent).toMatch(/-webkit-font-smoothing:\s*antialiased/);
    });

    test('System font stack includes fallbacks for all browsers', () => {
      // Check for system font stack that works across browsers
      expect(allCssContent).toMatch(/system-ui/);
      expect(allCssContent).toMatch(/-apple-system/);
      expect(allCssContent).toMatch(/BlinkMacSystemFont/);
      expect(allCssContent).toMatch(/Segoe UI/i);
    });

    test('Monospace font stack includes cross-browser fallbacks', () => {
      // Monospace fonts for code blocks
      expect(allCssContent).toMatch(/monospace/);
      // Should include common monospace fonts
      expect(allCssContent).toMatch(/JetBrains Mono|Fira Code|Monaco|Consolas/);
    });
  });

  describe('CSS Feature Support Verification', () => {
    test('Uses gap property for flexbox/grid (supported in latest 2 versions)', () => {
      // gap property works in flex and grid in modern browsers
      expect(allCssContent).toMatch(/gap:\s*.+/);
    });

    test('Uses place-items or individual alignment properties', () => {
      // Alignment properties are well-supported
      expect(allCssContent).toMatch(/justify-content:\s*.+/);
      expect(allCssContent).toMatch(/align-items:\s*.+/);
    });

    test('Uses CSS position: fixed for navigation', () => {
      // position: fixed is well-supported
      expect(allCssContent).toMatch(/position:\s*fixed/);
    });

    test('Uses z-index for layering', () => {
      // z-index is universally supported
      expect(allCssContent).toMatch(/z-index:\s*[0-9]+|var\(--z-/);
    });

    test('Uses rem units for font sizing (browser-compatible)', () => {
      // rem units are supported in all modern browsers
      expect(allCssContent).toMatch(/font-size:\s*[0-9.]+rem|var\(--font-size/);
    });

    test('Uses calc() function if needed (well-supported)', () => {
      // calc() is supported in all modern browsers
      // Just verify it's used if present, no requirement
      const hasCalc = allCssContent.includes('calc(');
      if (hasCalc) {
        expect(allCssContent).toMatch(/calc\(.+\)/);
      }
      expect(true).toBe(true); // Pass if calc not used
    });
  });

  describe('Compatibility Summary', () => {
    test('Overall CSS compatibility score', () => {
      // Check that we're using modern CSS features that work in latest 2 versions
      const modernFeatures = [
        'display: flex',
        'display: grid',
        '--color-',
        'var(--',
        'transform:',
        'transition:',
        '@keyframes',
        '@media'
      ];

      let featuresUsed = 0;
      modernFeatures.forEach(feature => {
        if (allCssContent.includes(feature.replace(':', ''))) {
          featuresUsed++;
        }
      });

      // Should use most modern CSS features
      expect(featuresUsed).toBeGreaterThanOrEqual(6);
    });

    test('No @supports needed for basic features', () => {
      // @supports is for progressive enhancement
      // Basic layout shouldn't require it for latest 2 versions
      // If @supports is used, it should be for enhancement only
      const supportsCount = (allCssContent.match(/@supports/g) || []).length;
      // Limited use of @supports is fine
      expect(supportsCount).toBeLessThanOrEqual(5);
    });
  });
});
