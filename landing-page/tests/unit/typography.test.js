/**
 * Typography Unit Tests
 * Owner: Scenario 14 - Visual Design - Typography
 * Tests for validating typography CSS and font loading strategy
 */
const fs = require('fs');
const path = require('path');

describe('Typography Tests', () => {
  let variablesCss;
  let stylesCss;
  let htmlContent;

  beforeAll(() => {
    const variablesPath = path.join(__dirname, '../../css/utilities/variables.css');
    const stylesPath = path.join(__dirname, '../../css/styles.css');
    const htmlPath = path.join(__dirname, '../../index.html');

    variablesCss = fs.readFileSync(variablesPath, 'utf-8');
    stylesCss = fs.readFileSync(stylesPath, 'utf-8');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');
  });

  describe('Heading Font-Family CSS', () => {
    test('TC1: CSS variables define heading font with Inter or system sans-serif', () => {
      // Check that --font-heading variable exists
      expect(variablesCss).toMatch(/--font-heading:/);

      // Check that Inter is in the font stack
      expect(variablesCss).toMatch(/--font-heading:[^;]*Inter/);

      // Check for system font fallbacks (system-ui or sans-serif)
      const fontHeadingMatch = variablesCss.match(/--font-heading:\s*([^;]+);/);
      expect(fontHeadingMatch).toBeTruthy();

      const fontStack = fontHeadingMatch[1];
      expect(fontStack).toMatch(/system-ui|sans-serif/);
    });

    test('Headings use the --font-heading variable', () => {
      // Check that h1-h6 elements use the font-heading variable
      expect(stylesCss).toMatch(/h1[^{]*{[^}]*font-family:\s*var\(--font-heading\)/s);
    });

    test('Heading font stack includes proper fallbacks', () => {
      const fontHeadingMatch = variablesCss.match(/--font-heading:\s*([^;]+);/);
      const fontStack = fontHeadingMatch[1];

      // Should have multiple fallback fonts
      const fonts = fontStack.split(',').map(f => f.trim());
      expect(fonts.length).toBeGreaterThanOrEqual(3);

      // Should end with generic sans-serif
      expect(fonts[fonts.length - 1]).toBe('sans-serif');
    });
  });

  describe('Code Block Font-Family CSS', () => {
    test('TC2: CSS variables define code font with JetBrains Mono, Fira Code, or monospace', () => {
      // Check that --font-code variable exists
      expect(variablesCss).toMatch(/--font-code:/);

      // Check for JetBrains Mono or Fira Code in the font stack
      const fontCodeMatch = variablesCss.match(/--font-code:\s*([^;]+);/);
      expect(fontCodeMatch).toBeTruthy();

      const fontStack = fontCodeMatch[1];
      expect(fontStack).toMatch(/JetBrains Mono|Fira Code/);

      // Check for monospace fallback
      expect(fontStack).toMatch(/monospace/);
    });

    test('Code elements use the --font-code variable', () => {
      // Check that code elements use the font-code variable
      expect(stylesCss).toMatch(/code\s*{[^}]*font-family:\s*var\(--font-code\)/s);
    });

    test('Code font stack includes proper fallbacks', () => {
      const fontCodeMatch = variablesCss.match(/--font-code:\s*([^;]+);/);
      const fontStack = fontCodeMatch[1];

      // Should have multiple fallback fonts
      const fonts = fontStack.split(',').map(f => f.trim());
      expect(fonts.length).toBeGreaterThanOrEqual(2);

      // Should end with generic monospace
      expect(fonts[fonts.length - 1]).toBe('monospace');
    });
  });

  describe('Font Loading Strategy', () => {
    test('TC3: Google Fonts loaded with display=swap for proper fallback', () => {
      // Check that Google Fonts link includes display=swap
      expect(htmlContent).toMatch(/fonts\.googleapis\.com[^"]*display=swap/);
    });

    test('Inter font is loaded from Google Fonts', () => {
      // Check that Inter font is requested from Google Fonts
      expect(htmlContent).toMatch(/fonts\.googleapis\.com[^"]*Inter/);
    });

    test('JetBrains Mono font is loaded from Google Fonts', () => {
      // Check that JetBrains Mono font is requested
      expect(htmlContent).toMatch(/fonts\.googleapis\.com[^"]*JetBrains[+%20]?Mono/);
    });

    test('Font preconnect hints are present for performance', () => {
      // Check for preconnect to Google Fonts domains
      expect(htmlContent).toMatch(/<link[^>]*rel="preconnect"[^>]*href="https:\/\/fonts\.googleapis\.com"/);
      expect(htmlContent).toMatch(/<link[^>]*rel="preconnect"[^>]*href="https:\/\/fonts\.gstatic\.com"/);
    });

    test('Google Fonts link has crossorigin for gstatic preconnect', () => {
      // The gstatic preconnect should have crossorigin attribute
      expect(htmlContent).toMatch(/<link[^>]*rel="preconnect"[^>]*href="https:\/\/fonts\.gstatic\.com"[^>]*crossorigin/);
    });

    test('Custom font variables are defined in CSS', () => {
      // Verify both font variables are defined in :root
      expect(variablesCss).toMatch(/:root\s*{[^}]*--font-heading:/s);
      expect(variablesCss).toMatch(/:root\s*{[^}]*--font-code:/s);
    });
  });

  describe('Typography Application', () => {
    test('Body uses font-body variable', () => {
      expect(stylesCss).toMatch(/body\s*{[^}]*font-family:\s*var\(--font-body\)/s);
    });

    test('Font-body variable is defined', () => {
      expect(variablesCss).toMatch(/--font-body:/);
    });

    test('Headings have appropriate font-weight', () => {
      // Headings should have bold weight (600-700)
      expect(stylesCss).toMatch(/h1[^{]*{[^}]*font-weight:\s*(600|700|bold)/s);
    });

    test('Headings have appropriate line-height', () => {
      // Headings should have tighter line-height for visual appeal
      expect(stylesCss).toMatch(/h1[^{]*{[^}]*line-height:\s*1\.[0-4]/s);
    });
  });
});
