/**
 * Tests for Code Snippets for Client Connection (REQ-10)
 * Verifies that code snippets demonstrating client connection examples exist and are properly formatted
 */

const fs = require('fs');
const path = require('path');

describe('Code Snippets for Client Connection', () => {
  let document;

  beforeEach(() => {
    const html = fs.readFileSync(path.resolve(__dirname, '../index.html'), 'utf8');
    document = new DOMParser().parseFromString(html, 'text/html');
  });

  // Test Case 1: Code snippet showing memcached client connection exists
  test('TC1: Code snippet showing memcached client connection exists', () => {
    const codeBlocks = document.querySelectorAll('pre, pre code');
    const connectionPatterns = [
      // Python memcached client patterns
      /import\s+(memcache|pymemcache)/i,
      /from\s+(memcache|pymemcache)/i,
      /Client\s*\(\s*["'].*["']\s*\)/i,
      /\.set\s*\(/i,
      /\.get\s*\(/i,
      // Node.js memcached client patterns
      /require\s*\(\s*["']memcached["']\s*\)/i,
      /new\s+Memcached\s*\(/i,
      // Ruby memcached client patterns
      /Dalli::Client/i,
      // Generic client connection patterns
      /memcache.*client/i,
      /connect.*12333/i,
      /localhost:12333/i,
      /127\.0\.0\.1:12333/i
    ];

    let hasClientConnection = false;
    codeBlocks.forEach(block => {
      const text = block.textContent;
      if (connectionPatterns.some(pattern => pattern.test(text))) {
        hasClientConnection = true;
      }
    });

    expect(hasClientConnection).toBe(true);
  });

  // Test Case 2: Code blocks have syntax highlighting classes applied
  test('TC2: Code blocks have syntax highlighting classes applied (language-* classes)', () => {
    const codeBlocks = document.querySelectorAll('pre code, pre');

    let hasSyntaxHighlighting = false;
    codeBlocks.forEach(block => {
      const className = block.className || '';
      // Check for language-* classes (Prism.js, highlight.js patterns)
      if (/language-\w+/.test(className)) {
        hasSyntaxHighlighting = true;
      }
    });

    expect(hasSyntaxHighlighting).toBe(true);
  });

  // Test Case 3: Code snippets are properly indented and formatted in monospace font
  test('TC3: Code snippets are properly indented and formatted in monospace font', () => {
    const html = fs.readFileSync(path.resolve(__dirname, '../index.html'), 'utf8');
    const css = fs.readFileSync(path.resolve(__dirname, '../styles.css'), 'utf8');

    // Check that pre and code elements exist in HTML
    const preElements = document.querySelectorAll('pre');
    const codeElements = document.querySelectorAll('code');

    expect(preElements.length).toBeGreaterThan(0);
    expect(codeElements.length).toBeGreaterThan(0);

    // Check CSS for monospace font styling
    const monospacePatterns = [
      /code\s*\{[^}]*font-family[^}]*monospace/is,
      /pre\s*\{[^}]*font-family[^}]*monospace/is,
      /pre\s+code\s*\{[^}]*font-family[^}]*monospace/is,
      /Monaco/i,
      /Menlo/i,
      /Consolas/i,
      /SFMono/i,
      /'Liberation Mono'/i
    ];

    const hasMonospaceFont = monospacePatterns.some(pattern => pattern.test(css));
    expect(hasMonospaceFont).toBe(true);

    // Check that code blocks have proper formatting (background color, padding)
    const hasCodeStyling = /pre\s*\{[^}]*(background|padding)[^}]*\}/is.test(css);
    expect(hasCodeStyling).toBe(true);
  });

  // Additional test: Client connection section exists with identifiable data attribute or id
  test('TC4: Client connection example section is identifiable', () => {
    // Look for a section with client connection examples
    const clientConnectionSection = document.querySelector('[data-section="client-connection"]') ||
                                    document.getElementById('client-connection') ||
                                    document.getElementById('client-examples');

    // Also check if there's a heading mentioning client connection
    const headings = document.querySelectorAll('h2, h3, h4');
    let hasClientHeading = false;
    headings.forEach(h => {
      const text = h.textContent.toLowerCase();
      if (text.includes('client') && (text.includes('connection') || text.includes('example'))) {
        hasClientHeading = true;
      }
    });

    const hasSection = clientConnectionSection !== null || hasClientHeading;
    expect(hasSection).toBe(true);
  });

  // Test: Code snippets demonstrate actual usage with set/get operations
  test('TC5: Code snippets demonstrate practical client usage', () => {
    const codeBlocks = document.querySelectorAll('pre, pre code');
    const usagePatterns = [
      // Setting values
      /\.set\s*\(\s*["'].*["']/i,
      /client\.set/i,
      // Getting values
      /\.get\s*\(\s*["'].*["']/i,
      /client\.get/i
    ];

    let hasPracticalUsage = false;
    codeBlocks.forEach(block => {
      const text = block.textContent;
      if (usagePatterns.some(pattern => pattern.test(text))) {
        hasPracticalUsage = true;
      }
    });

    expect(hasPracticalUsage).toBe(true);
  });
});
