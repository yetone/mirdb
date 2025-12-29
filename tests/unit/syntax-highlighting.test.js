/**
 * Unit tests for Syntax Highlighting (NFR-5)
 * Verifies Prism.js integration and syntax highlighting setup
 */

const { test, describe } = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');

const indexHtmlPath = path.resolve(__dirname, '../../index.html');
const stylesCssPath = path.resolve(__dirname, '../../styles.css');

describe('Syntax Highlighting Unit Tests', () => {

  /**
   * Test Case 3 (unit): Verify Prism.js integration at HTML level
   */
  test('TC3 Unit: Prism.js CSS and JS files are properly included', () => {
    const html = fs.readFileSync(indexHtmlPath, 'utf8');

    // Check for Prism CSS (theme)
    const hasPrismCss = html.includes('prism') &&
                        (html.includes('.css') || html.includes('themes'));
    assert.ok(hasPrismCss, 'Prism.js CSS theme should be included');

    // Check for Prism JS
    const hasPrismJs = html.includes('prism.min.js') || html.includes('prism.js');
    assert.ok(hasPrismJs, 'Prism.js JavaScript should be included');

    // Check for language-specific Prism components
    const hasPythonComponent = html.includes('prism-python');
    assert.ok(hasPythonComponent, 'Prism Python language component should be included');

    const hasBashComponent = html.includes('prism-bash');
    assert.ok(hasBashComponent, 'Prism Bash language component should be included');
  });

  test('Code blocks have proper language classes for Prism.js', () => {
    const html = fs.readFileSync(indexHtmlPath, 'utf8');

    // Check for language-specific classes on code elements
    const languageClasses = [
      'language-python',
      'language-bash'
    ];

    for (const langClass of languageClasses) {
      assert.ok(
        html.includes(`class="${langClass}"`) || html.includes(`class='${langClass}'`),
        `Should have ${langClass} class on code elements`
      );
    }

    // Verify pre elements also have language classes (Prism requires this)
    const preLangPattern = /<pre[^>]*class="language-\w+"[^>]*>/gi;
    const preMatches = html.match(preLangPattern);
    assert.ok(preMatches && preMatches.length >= 1, 'pre elements should have language-* classes');
  });

  test('Prism theme is loaded from CDN', () => {
    const html = fs.readFileSync(indexHtmlPath, 'utf8');

    // Check for Prism CDN link
    const hasPrismCdn = html.includes('cdnjs.cloudflare.com/ajax/libs/prism') ||
                        html.includes('cdn.jsdelivr.net') ||
                        html.includes('unpkg.com/prismjs');
    assert.ok(hasPrismCdn, 'Prism should be loaded from a CDN');

    // Check for a specific theme (prism-tomorrow is a dark theme suitable for dark backgrounds)
    const hasTheme = html.includes('prism-tomorrow') ||
                     html.includes('prism-okaidia') ||
                     html.includes('prism-dark') ||
                     html.includes('themes/prism');
    assert.ok(hasTheme, 'A Prism theme should be specified');
  });

  test('CSS styles support code block readability', () => {
    const css = fs.readFileSync(stylesCssPath, 'utf8');

    // Check for code-related CSS variables or styles
    const hasCodeStyles = css.includes('code') || css.includes('pre');
    assert.ok(hasCodeStyles, 'CSS should include code/pre styles');

    // Check for font-mono variable or monospace font
    const hasMonoFont = css.includes('mono') || css.includes('monospace') || css.includes('Consolas');
    assert.ok(hasMonoFont, 'CSS should define monospace font for code');

    // Check for Prism override styles (to ensure consistency with site theme)
    const hasPrismOverride = css.includes('language-') || css.includes('[class*="language-"]');
    assert.ok(hasPrismOverride, 'CSS should include Prism override styles');
  });

  test('Code blocks have appropriate structure for syntax highlighting', () => {
    const html = fs.readFileSync(indexHtmlPath, 'utf8');

    // Verify code is wrapped in pre > code structure
    const preCodePattern = /<pre[^>]*>\s*<code/gi;
    const matches = html.match(preCodePattern);
    assert.ok(matches && matches.length >= 3, 'Should have multiple pre > code structures');

    // Verify code blocks have IDs for interaction
    const codeIds = ['code-connect', 'code-set', 'code-get', 'code-install', 'code-config'];
    let foundCount = 0;
    for (const id of codeIds) {
      if (html.includes(`id="${id}"`)) {
        foundCount++;
      }
    }
    assert.ok(foundCount >= 3, 'Code blocks should have IDs for clipboard functionality');
  });

  test('Multiple programming languages are supported', () => {
    const html = fs.readFileSync(indexHtmlPath, 'utf8');

    // Check for Python code
    assert.ok(html.includes('language-python'), 'Should support Python syntax highlighting');

    // Check for Bash code
    assert.ok(html.includes('language-bash'), 'Should support Bash syntax highlighting');

    // Check for TOML code (configuration)
    assert.ok(html.includes('language-toml'), 'Should support TOML syntax highlighting');

    // Verify Prism components are loaded for each language
    assert.ok(html.includes('prism-python'), 'Prism Python component should be loaded');
    assert.ok(html.includes('prism-bash'), 'Prism Bash component should be loaded');
    assert.ok(html.includes('prism-toml'), 'Prism TOML component should be loaded');
  });

  test('Prism theme provides good contrast for dark backgrounds', () => {
    const html = fs.readFileSync(indexHtmlPath, 'utf8');

    // The site uses a dark theme, so Prism theme should be a dark-compatible one
    // prism-tomorrow, prism-okaidia, prism-dark, prism-twilight are dark themes
    const darkThemes = ['tomorrow', 'okaidia', 'dark', 'twilight', 'atom-dark', 'nord'];
    const hasDarkTheme = darkThemes.some(theme => html.toLowerCase().includes(theme));

    assert.ok(hasDarkTheme, 'Should use a dark-compatible Prism theme for the dark site design');
  });

});
