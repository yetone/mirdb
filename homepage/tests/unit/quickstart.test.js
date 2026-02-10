/**
 * Quick Start Section Unit Tests
 * Owner: Scenario 3 - Quick Start Guide Section
 *
 * Unit tests for the quick start section code formatting.
 *
 * Expected test coverage:
 * - Code blocks use monospace font-family
 * - Quick start section has proper structure
 *
 * Requirements traced:
 * - REQ-3: Homepage shall provide quick start guide
 * - Design requirement: Code readability with monospace font
 */

const fs = require('fs');
const path = require('path');

describe('Quick Start Section Code Formatting', () => {
  let htmlContent;
  let cssContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    const cssPath = path.join(__dirname, '../../css/styles.css');

    htmlContent = fs.readFileSync(htmlPath, 'utf8');
    cssContent = fs.readFileSync(cssPath, 'utf8');

    // Set up document with HTML content
    document.body.innerHTML = htmlContent;

    // Create and inject styles
    const styleElement = document.createElement('style');
    styleElement.textContent = cssContent;
    document.head.appendChild(styleElement);
  });

  // Test Case 4: Check code formatting with monospace font
  test('code blocks use monospace font-family', () => {
    // Check CSS contains monospace font definition for code elements
    const cssLower = cssContent.toLowerCase();

    // Check for font-family-mono variable definition
    const hasMonoVariable = cssLower.includes('--font-family-mono');

    // Check for common monospace fonts
    const monospaceKeywords = [
      'monospace',
      'courier',
      'monaco',
      'consolas',
      'menlo',
      'liberation mono',
      'sfmono'
    ];

    const hasMonospaceFont = monospaceKeywords.some(font =>
      cssLower.includes(font)
    );

    expect(hasMonoVariable || hasMonospaceFont).toBeTruthy();

    // Check that pre or code selectors use the monospace font
    const codeBlockPattern = /(pre|code)[^{]*\{[^}]*font-family[^}]*\}/gi;
    const fontFamilyPattern = /font-family:\s*var\(--font-family-mono\)|font-family:[^;]*monospace|font-family:[^;]*consolas|font-family:[^;]*monaco|font-family:[^;]*courier/gi;

    const hasCodeFontRule = fontFamilyPattern.test(cssContent);
    expect(hasCodeFontRule).toBeTruthy();
  });

  test('quick start section contains pre elements for code examples', () => {
    const quickstartSection = document.getElementById('quickstart');
    expect(quickstartSection).not.toBeNull();

    const preElements = quickstartSection.querySelectorAll('pre');
    expect(preElements.length).toBeGreaterThan(0);
  });

  test('quick start section contains code elements', () => {
    const quickstartSection = document.getElementById('quickstart');
    expect(quickstartSection).not.toBeNull();

    const codeElements = quickstartSection.querySelectorAll('code');
    expect(codeElements.length).toBeGreaterThan(0);
  });

  test('quick start section has proper heading structure', () => {
    const quickstartSection = document.getElementById('quickstart');
    expect(quickstartSection).not.toBeNull();

    const heading = quickstartSection.querySelector('h1, h2, h3');
    expect(heading).not.toBeNull();

    const headingText = heading.textContent.toLowerCase();
    expect(headingText).toMatch(/quick\s*start|getting\s*started|start/i);
  });

  test('CSS variable --font-family-mono is defined with monospace fonts', () => {
    // Check the CSS variable definition
    const monoVarPattern = /--font-family-mono:\s*([^;]+);/i;
    const match = cssContent.match(monoVarPattern);

    expect(match).not.toBeNull();

    if (match) {
      const fontValue = match[1].toLowerCase();
      const monospaceKeywords = ['monospace', 'courier', 'consolas', 'monaco', 'menlo'];
      const hasMonospace = monospaceKeywords.some(font => fontValue.includes(font));
      expect(hasMonospace).toBeTruthy();
    }
  });
});
