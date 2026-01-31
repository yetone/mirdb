/**
 * Code Block Styling Unit Tests
 * Owner: Scenario 18 - Code Syntax Highlighting
 *
 * Tests:
 * - Code blocks use monospace font (Fira Code, JetBrains Mono, or system monospace)
 * - Code blocks have dark background (#2D2D2D) for contrast
 *
 * Requirements: Test Cases 3 and 4
 */

const fs = require('fs');
const path = require('path');

describe('Code Block Styling', () => {
  let prismThemeCss;
  let mainStylesCss;

  beforeAll(() => {
    // Read the prism-theme.css file
    const prismThemePath = path.join(__dirname, '../../css/prism-theme.css');
    prismThemeCss = fs.readFileSync(prismThemePath, 'utf8');

    // Read the main styles.css file
    const mainStylesPath = path.join(__dirname, '../../css/styles.css');
    mainStylesCss = fs.readFileSync(mainStylesPath, 'utf8');
  });

  describe('Test Case 3: Code blocks use monospace font', () => {
    test('prism-theme.css defines monospace font family for code elements', () => {
      // Check for monospace font definitions in prism theme
      const hasMonospaceFonts =
        prismThemeCss.includes('Fira Code') ||
        prismThemeCss.includes('JetBrains Mono') ||
        prismThemeCss.includes('Cascadia Code') ||
        prismThemeCss.includes('monospace');

      expect(hasMonospaceFonts).toBe(true);
    });

    test('prism-theme.css applies font-family to pre or code elements', () => {
      // Check that font-family is applied to language-prefixed elements
      const fontFamilyRegex = /pre\[class\*="language-"\][\s\S]*?font-family.*monospace/;
      const codeFontFamilyRegex = /code\[class\*="language-"\][\s\S]*?font-family.*monospace/;

      const hasFontFamilyOnPre = fontFamilyRegex.test(prismThemeCss);
      const hasFontFamilyOnCode = codeFontFamilyRegex.test(prismThemeCss);

      // Either pre or code should have font-family defined
      expect(hasFontFamilyOnPre || hasFontFamilyOnCode).toBe(true);
    });

    test('main styles.css defines monospace font for code elements', () => {
      // Check for --font-family-mono CSS variable
      const hasMonoVariable = mainStylesCss.includes('--font-family-mono');

      // Check for actual monospace fonts
      const hasMonospaceFonts =
        mainStylesCss.includes('Fira Code') ||
        mainStylesCss.includes('JetBrains Mono') ||
        mainStylesCss.includes('monospace');

      expect(hasMonoVariable && hasMonospaceFonts).toBe(true);
    });

    test('code element uses var(--font-family-mono) or direct monospace font', () => {
      // Check that code elements reference the monospace font
      const codeStyleRegex = /code\s*\{[\s\S]*?font-family[^}]*/;
      const codeStyle = mainStylesCss.match(codeStyleRegex);

      expect(codeStyle).not.toBeNull();

      const usesMonospace =
        codeStyle[0].includes('--font-family-mono') ||
        codeStyle[0].includes('monospace');

      expect(usesMonospace).toBe(true);
    });
  });

  describe('Test Case 4: Code block background contrast', () => {
    test('prism-theme.css defines dark background #2D2D2D for code blocks', () => {
      // Check for the dark background color
      const hasDarkBackground = prismThemeCss.includes('#2D2D2D');

      expect(hasDarkBackground).toBe(true);
    });

    test('prism-theme.css applies background to pre[class*="language-"]', () => {
      // Check that background-color is applied to pre elements
      const preBackgroundRegex = /pre\[class\*="language-"\][\s\S]*?background-color:\s*#2D2D2D/;

      const hasPreBackground = preBackgroundRegex.test(prismThemeCss);

      expect(hasPreBackground).toBe(true);
    });

    test('main styles.css defines --color-secondary as #2D2D2D for code blocks', () => {
      // Check for the secondary color variable (used for code blocks)
      const hasSecondaryColor = mainStylesCss.includes('--color-secondary: #2D2D2D');

      expect(hasSecondaryColor).toBe(true);
    });

    test('pre element in main styles uses dark background', () => {
      // Check that pre elements use the dark background
      const preStyleRegex = /pre\s*\{[\s\S]*?background-color[^}]*/;
      const preStyle = mainStylesCss.match(preStyleRegex);

      expect(preStyle).not.toBeNull();

      const usesDarkBackground =
        preStyle[0].includes('--color-secondary') ||
        preStyle[0].includes('#2D2D2D') ||
        preStyle[0].includes('var(--color-secondary)');

      expect(usesDarkBackground).toBe(true);
    });

    test('code block text has sufficient contrast with dark background', () => {
      // Check for light text color on dark background
      const hasLightTextInPrism =
        prismThemeCss.includes('#F5F5F5') ||
        prismThemeCss.includes('#f8f8f2') ||
        prismThemeCss.includes('--color-text-inverse');

      const hasInverseTextVar = mainStylesCss.includes('--color-text-inverse: #FFFFFF');

      expect(hasLightTextInPrism || hasInverseTextVar).toBe(true);
    });
  });
});
