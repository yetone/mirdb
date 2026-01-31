/**
 * Syntax Highlighting Unit Tests
 * Owner: Scenario 4 - Quick Start Guide Section
 *
 * Tests:
 * - Code blocks use Prism.js or similar for syntax highlighting
 * - Language classes are correctly applied (bash, toml, rust)
 *
 * Requirements: REQ-4, Test Case 5
 */

const fs = require('fs');
const path = require('path');

describe('Syntax Highlighting Configuration', () => {
  let htmlContent;

  beforeAll(() => {
    // Read the index.html file
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');
  });

  test('Test Case 5: Code blocks use Prism.js or similar for syntax highlighting', () => {
    // Check if Prism.js is included
    const hasPrismScript = htmlContent.includes('prism') ||
                          htmlContent.includes('Prism') ||
                          htmlContent.includes('highlight.js') ||
                          htmlContent.includes('hljs');

    // Check if prism theme CSS is included
    const hasPrismTheme = htmlContent.includes('prism-theme.css') ||
                         htmlContent.includes('prism.css') ||
                         htmlContent.includes('highlight');

    // Either Prism.js library or CSS class-based highlighting should be used
    const hasLanguageClasses = htmlContent.includes('language-bash') ||
                               htmlContent.includes('language-toml') ||
                               htmlContent.includes('language-rust') ||
                               htmlContent.includes('language-shell') ||
                               htmlContent.includes('language-text');

    // At least one form of syntax highlighting should be present
    expect(hasPrismScript || hasPrismTheme || hasLanguageClasses).toBeTruthy();
  });

  test('Quick Start section has code blocks with language classes', () => {
    // Look for the quickstart section
    const quickstartMatch = htmlContent.match(/<section[^>]*id="quickstart"[^>]*>([\s\S]*?)<\/section>/i);
    expect(quickstartMatch).not.toBeNull();

    const quickstartContent = quickstartMatch ? quickstartMatch[1] : '';

    // Check for code elements with language classes in the quick start section
    const hasCodeWithLanguage = quickstartContent.includes('class="language-') ||
                                quickstartContent.includes("class='language-");

    // The quick start section should have properly marked code blocks
    expect(hasCodeWithLanguage).toBeTruthy();
  });

  test('Syntax highlighting supports bash for shell commands', () => {
    // Check for bash/shell language class
    const hasBashHighlighting = htmlContent.includes('language-bash') ||
                                htmlContent.includes('language-shell') ||
                                htmlContent.includes('language-sh');

    expect(hasBashHighlighting).toBeTruthy();
  });

  test('Syntax highlighting supports toml for configuration files', () => {
    // Check for toml language class
    const hasTomlHighlighting = htmlContent.includes('language-toml') ||
                                htmlContent.includes('language-ini') ||
                                htmlContent.includes('language-properties');

    expect(hasTomlHighlighting).toBeTruthy();
  });

  test('Code blocks have proper pre and code element structure', () => {
    // Look for proper pre > code structure
    const hasPreCodeStructure = htmlContent.includes('<pre') &&
                                htmlContent.includes('<code');

    expect(hasPreCodeStructure).toBeTruthy();
  });

  test('Quick Start section includes Prism.js script or equivalent', () => {
    // Check for Prism.js CDN or local script
    const hasPrismCDN = htmlContent.includes('cdn.jsdelivr.net/npm/prismjs') ||
                        htmlContent.includes('cdnjs.cloudflare.com/ajax/libs/prism') ||
                        htmlContent.includes('prism.min.js') ||
                        htmlContent.includes('prism.js');

    // Or check for highlight.js as an alternative
    const hasHighlightJS = htmlContent.includes('highlight.min.js') ||
                          htmlContent.includes('highlight.js') ||
                          htmlContent.includes('hljs');

    // Or inline highlighting via CSS classes
    const hasInlineHighlighting = htmlContent.includes('.token') ||
                                  htmlContent.includes('token-');

    // At least one syntax highlighting solution should be present
    expect(hasPrismCDN || hasHighlightJS || hasInlineHighlighting).toBeTruthy();
  });
});
