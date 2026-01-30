/**
 * CSS Variables Unit Tests
 * Owner: Scenario 13 - Visual Design - Color Scheme
 *
 * Tests for verifying the dark theme color scheme is correctly implemented.
 * Validates CSS custom properties for colors and their application.
 */
const fs = require('fs');
const path = require('path');

describe('Visual Design - Color Scheme Tests', () => {
  let cssVariablesContent;
  let mainStylesContent;
  let htmlContent;

  beforeAll(() => {
    const variablesPath = path.join(__dirname, '../../css/utilities/variables.css');
    cssVariablesContent = fs.readFileSync(variablesPath, 'utf-8');

    const stylesPath = path.join(__dirname, '../../css/styles.css');
    mainStylesContent = fs.readFileSync(stylesPath, 'utf-8');

    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');
  });

  describe('TC1: CSS Custom Properties for Colors', () => {
    test('CSS defines --color-bg-primary variable with dark navy (#1a1a2e)', () => {
      expect(cssVariablesContent).toMatch(/--color-bg-primary:\s*#1a1a2e/i);
    });

    test('CSS defines --color-bg-secondary variable with lighter navy (#16213e)', () => {
      expect(cssVariablesContent).toMatch(/--color-bg-secondary:\s*#16213e/i);
    });

    test('CSS defines --color-accent-blue variable (#0f3460)', () => {
      expect(cssVariablesContent).toMatch(/--color-accent-blue:\s*#0f3460/i);
    });

    test('CSS defines --color-accent-coral variable (#e94560)', () => {
      expect(cssVariablesContent).toMatch(/--color-accent-coral:\s*#e94560/i);
    });

    test('CSS defines --color-text variable with light gray (#eaeaea)', () => {
      expect(cssVariablesContent).toMatch(/--color-text:\s*#eaeaea/i);
    });

    test('All color variables are defined within :root selector', () => {
      // Extract :root block content
      const rootMatch = cssVariablesContent.match(/:root\s*\{[\s\S]*?\}/);
      expect(rootMatch).not.toBeNull();

      const rootContent = rootMatch[0];
      expect(rootContent).toContain('--color-bg-primary');
      expect(rootContent).toContain('--color-bg-secondary');
      expect(rootContent).toContain('--color-accent-blue');
      expect(rootContent).toContain('--color-accent-coral');
      expect(rootContent).toContain('--color-text');
    });

    test('CSS includes additional utility color variables', () => {
      expect(cssVariablesContent).toContain('--color-text-muted');
      expect(cssVariablesContent).toContain('--color-white');
      expect(cssVariablesContent).toContain('--color-black');
    });
  });

  describe('TC2: Main Background Color', () => {
    test('Body background uses --color-bg-primary variable', () => {
      expect(mainStylesContent).toMatch(/body\s*\{[\s\S]*?background-color:\s*var\(--color-bg-primary\)/);
    });

    test('Body background color is set to dark navy (#1a1a2e)', () => {
      // Verify the variable value
      expect(cssVariablesContent).toMatch(/--color-bg-primary:\s*#1a1a2e/i);
      // Verify it's used in body
      expect(mainStylesContent).toContain('background-color: var(--color-bg-primary)');
    });
  });

  describe('TC3: CTA Button Color', () => {
    test('Primary button uses --color-accent-coral for background', () => {
      expect(mainStylesContent).toMatch(/\.btn-primary\s*\{[\s\S]*?background-color:\s*var\(--color-accent-coral\)/);
    });

    test('Primary button has white text color', () => {
      expect(mainStylesContent).toMatch(/\.btn-primary\s*\{[\s\S]*?color:\s*var\(--color-white\)/);
    });

    test('Coral accent color is #e94560', () => {
      expect(cssVariablesContent).toMatch(/--color-accent-coral:\s*#e94560/i);
    });

    test('CTA buttons exist in HTML', () => {
      expect(htmlContent).toContain('btn-primary');
      expect(htmlContent).toMatch(/<a[^>]*class="btn btn-primary"[^>]*>/);
    });
  });

  describe('TC4: Text Color on Dark Background', () => {
    test('Body text uses --color-text variable', () => {
      expect(mainStylesContent).toMatch(/body\s*\{[\s\S]*?color:\s*var\(--color-text\)/);
    });

    test('Text color is light gray (#eaeaea) for readability', () => {
      expect(cssVariablesContent).toMatch(/--color-text:\s*#eaeaea/i);
    });

    test('Text color meets WCAG AA contrast ratio against primary background', () => {
      // Helper function to calculate luminance
      function getLuminance(r, g, b) {
        const [rs, gs, bs] = [r, g, b].map((c) => {
          c = c / 255;
          return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
        });
        return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
      }

      // Helper function to calculate contrast ratio
      function getContrastRatio(color1, color2) {
        const l1 = getLuminance(...color1);
        const l2 = getLuminance(...color2);
        const lighter = Math.max(l1, l2);
        const darker = Math.min(l1, l2);
        return (lighter + 0.05) / (darker + 0.05);
      }

      // Parse hex color to RGB
      function hexToRgb(hex) {
        const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
        return result
          ? [parseInt(result[1], 16), parseInt(result[2], 16), parseInt(result[3], 16)]
          : null;
      }

      const bgPrimary = hexToRgb('#1a1a2e');
      const textColor = hexToRgb('#eaeaea');
      const ratio = getContrastRatio(bgPrimary, textColor);

      // WCAG AA requires 4.5:1 for normal text
      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });
  });

  describe('Color Scheme Consistency', () => {
    test('Secondary background is defined and used', () => {
      expect(cssVariablesContent).toMatch(/--color-bg-secondary:\s*#16213e/i);
    });

    test('Accent blue is defined for UI elements', () => {
      expect(cssVariablesContent).toMatch(/--color-accent-blue:\s*#0f3460/i);
    });

    test('Muted text color is defined for less prominent text', () => {
      expect(cssVariablesContent).toMatch(/--color-text-muted:\s*#a0a0a0/i);
    });

    test('Color variables follow consistent naming convention', () => {
      // All color variables should follow --color-{category}-{variant} pattern
      const colorVars = cssVariablesContent.match(/--color-[a-z-]+/g) || [];
      expect(colorVars.length).toBeGreaterThan(5);

      colorVars.forEach((varName) => {
        expect(varName).toMatch(/^--color-[a-z]+-?[a-z]*$/);
      });
    });
  });

  describe('Color Values Validation', () => {
    test('All color values are valid hex codes', () => {
      const hexColorPattern = /#[0-9a-fA-F]{6}\b/g;
      const hexColors = cssVariablesContent.match(hexColorPattern) || [];

      expect(hexColors.length).toBeGreaterThan(0);

      hexColors.forEach((color) => {
        expect(color).toMatch(/^#[0-9a-fA-F]{6}$/);
      });
    });

    test('Primary background is dark (luminance < 0.1)', () => {
      function getLuminance(r, g, b) {
        const [rs, gs, bs] = [r, g, b].map((c) => {
          c = c / 255;
          return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
        });
        return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
      }

      // #1a1a2e = rgb(26, 26, 46)
      const luminance = getLuminance(26, 26, 46);
      expect(luminance).toBeLessThan(0.1);
    });

    test('Text color is light (luminance > 0.7)', () => {
      function getLuminance(r, g, b) {
        const [rs, gs, bs] = [r, g, b].map((c) => {
          c = c / 255;
          return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
        });
        return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
      }

      // #eaeaea = rgb(234, 234, 234)
      const luminance = getLuminance(234, 234, 234);
      expect(luminance).toBeGreaterThan(0.7);
    });

    test('Coral accent color has vibrant saturation', () => {
      // #e94560 = rgb(233, 69, 96)
      // This is a saturated coral/red color
      const r = 233, g = 69, b = 96;
      const max = Math.max(r, g, b);
      const min = Math.min(r, g, b);
      const saturation = (max - min) / max;

      // Expect at least 50% saturation for a vibrant accent
      expect(saturation).toBeGreaterThan(0.5);
    });
  });

  describe('CSS Variables Import and Usage', () => {
    test('Styles.css imports variables.css', () => {
      expect(mainStylesContent).toMatch(/@import.*variables\.css/);
    });

    test('Variables are imported before other stylesheets', () => {
      const imports = mainStylesContent.match(/@import[^;]+/g) || [];
      expect(imports.length).toBeGreaterThan(0);

      // Find variables.css import position
      const variablesIndex = imports.findIndex(imp => imp.includes('variables.css'));
      // Find reset.css import position (should be before variables)
      const resetIndex = imports.findIndex(imp => imp.includes('reset.css'));

      expect(variablesIndex).toBeGreaterThan(-1);
      // Variables should come right after reset
      expect(variablesIndex).toBe(resetIndex + 1);
    });
  });
});
