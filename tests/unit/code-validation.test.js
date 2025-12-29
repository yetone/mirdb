/**
 * Unit tests for Code Examples Section
 * Test Case 5 (unit): Syntax highlighting classes
 * Test Case 7 (unit): Python code is syntactically valid
 */

const { test, describe } = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');

const indexHtmlPath = path.resolve(__dirname, '../../index.html');

describe('Code Examples Unit Tests', () => {

  // Test Case 5 (unit): Check syntax highlighting classes in HTML
  test('TC5 Unit: Code blocks have syntax highlighting classes applied', () => {
    const html = fs.readFileSync(indexHtmlPath, 'utf8');

    // Check for Prism.js CSS inclusion
    assert.ok(
      html.includes('prism') && (html.includes('.css') || html.includes('.min.css')),
      'Prism.js CSS should be included'
    );

    // Check for language-python class on code elements
    assert.ok(
      html.includes('class="language-python"') || html.includes("class='language-python'"),
      'Code blocks should have language-python class for syntax highlighting'
    );

    // Check for Prism.js JavaScript inclusion
    assert.ok(
      html.includes('prism') && html.includes('.js'),
      'Prism.js JavaScript should be included for runtime highlighting'
    );
  });

  // Test Case 7: Verify Python code is syntactically correct
  test('TC7: Python code examples are syntactically correct and copy-paste ready', () => {
    const html = fs.readFileSync(indexHtmlPath, 'utf8');

    // Extract Python code from code blocks
    // Match content inside <code id="code-connect" ...>...</code> etc
    const codeBlockRegex = /<code[^>]*class="language-python"[^>]*>([\s\S]*?)<\/code>/gi;
    const matches = html.matchAll(codeBlockRegex);
    const codeBlocks = [...matches].map(m => m[1]);

    assert.ok(codeBlocks.length >= 2, 'Should have at least 2 Python code blocks');

    // Verify each code block has valid Python syntax patterns
    codeBlocks.forEach((code, index) => {
      // Decode HTML entities
      const decodedCode = code
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>')
        .replace(/&amp;/g, '&')
        .replace(/&quot;/g, '"')
        .replace(/&#39;/g, "'");

      // Check for Python-specific syntax elements
      // Valid Python should have:
      // - import statements, OR
      // - function calls with parentheses, OR
      // - variable assignments, OR
      // - comments starting with #

      const hasPythonSyntax =
        decodedCode.includes('import ') ||
        decodedCode.includes('= ') ||
        /\w+\(.*\)/.test(decodedCode) ||
        decodedCode.includes('#');

      assert.ok(
        hasPythonSyntax,
        `Code block ${index + 1} should contain valid Python syntax patterns`
      );

      // Verify no obvious syntax errors:
      // - Unmatched parentheses
      const openParens = (decodedCode.match(/\(/g) || []).length;
      const closeParens = (decodedCode.match(/\)/g) || []).length;
      assert.strictEqual(
        openParens,
        closeParens,
        `Code block ${index + 1} should have matched parentheses`
      );

      // - Unmatched brackets
      const openBrackets = (decodedCode.match(/\[/g) || []).length;
      const closeBrackets = (decodedCode.match(/\]/g) || []).length;
      assert.strictEqual(
        openBrackets,
        closeBrackets,
        `Code block ${index + 1} should have matched brackets`
      );

      // - Unmatched quotes (basic check)
      const singleQuotes = (decodedCode.match(/'/g) || []).length;
      const doubleQuotes = (decodedCode.match(/"/g) || []).length;
      assert.ok(
        singleQuotes % 2 === 0,
        `Code block ${index + 1} should have matched single quotes`
      );
      assert.ok(
        doubleQuotes % 2 === 0,
        `Code block ${index + 1} should have matched double quotes`
      );
    });

    // Specific content checks for copy-paste readiness

    // Connection code should have proper import and client setup
    const connectionCode = codeBlocks.find(c => c.includes('memcache.Client'));
    assert.ok(connectionCode, 'Should have connection example');
    assert.ok(connectionCode.includes('import memcache'), 'Connection should import memcache');
    assert.ok(connectionCode.includes("127.0.0.1:12333"), 'Connection should have correct address');

    // SET code should have mc.set call
    const setCode = codeBlocks.find(c => c.includes('mc.set'));
    assert.ok(setCode, 'Should have SET operation example');

    // GET code should have mc.get call
    const getCode = codeBlocks.find(c => c.includes('mc.get'));
    assert.ok(getCode, 'Should have GET operation example');
  });

  // Additional test: Verify HTML structure is valid for code blocks
  test('Code blocks have proper HTML structure', () => {
    const html = fs.readFileSync(indexHtmlPath, 'utf8');

    // Each code block should be inside a pre element
    const preCodePattern = /<pre[^>]*class="language-\w+"[^>]*>\s*<code/gi;
    const matches = html.match(preCodePattern);

    assert.ok(matches && matches.length >= 1, 'Code should be wrapped in pre.language-* > code');

    // Verify code blocks have IDs for copy functionality
    assert.ok(html.includes('id="code-connect"'), 'Connection code should have ID');
    assert.ok(html.includes('id="code-set"'), 'SET code should have ID');
    assert.ok(html.includes('id="code-get"'), 'GET code should have ID');
  });

  // Test: Copy buttons have correct data attributes
  test('Copy buttons have correct data-clipboard-target attributes', () => {
    const html = fs.readFileSync(indexHtmlPath, 'utf8');

    // Each copy button should reference a code block ID
    const copyBtnPattern = /data-clipboard-target="#code-\w+"/gi;
    const matches = html.match(copyBtnPattern);

    assert.ok(matches && matches.length >= 3, 'Should have copy buttons with clipboard targets');
  });

});
