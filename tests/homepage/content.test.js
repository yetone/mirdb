/**
 * Content Section Tests
 * Owner: Scenarios 2, 3, 4, 8 (shared)
 *
 * Tests for:
 * - Features section content and count
 * - Quick Start code blocks
 * - Comparison table content
 * - Project status checkmarks
 */

const { loadHTML, loadHTMLWithWindow } = require('./test-utils');

describe('Quick Start Section Validation', () => {
  let document;

  beforeAll(async () => {
    document = await loadHTML();
  });

  describe('Test Case 1: Quick Start Section Exists', () => {
    test('A section with content about Quick Start exists', () => {
      // Look for a section with id "quick-start" or containing "Quick Start" text
      const quickStartSection = document.querySelector('#quick-start');
      expect(quickStartSection).not.toBeNull();

      // Also verify it has "Quick Start" in the heading
      const heading = quickStartSection.querySelector('h2');
      expect(heading).not.toBeNull();
      expect(heading.textContent.toLowerCase()).toContain('quick start');
    });

    test('Quick Start section has data-testid attribute', () => {
      const section = document.querySelector('[data-testid="quick-start-section"]');
      expect(section).not.toBeNull();
    });
  });

  describe('Test Case 2: Set Command Example', () => {
    test('A code or pre element contains set command syntax', () => {
      const quickStartSection = document.querySelector('#quick-start');
      const codeElements = quickStartSection.querySelectorAll('pre, code');

      let hasSetCommand = false;
      for (const el of codeElements) {
        const text = el.textContent;
        // Check for set command with memcached protocol format
        if (text.includes('set') && (text.includes('<key>') || text.includes('mykey'))) {
          hasSetCommand = true;
          break;
        }
      }

      expect(hasSetCommand).toBe(true);
    });

    test('Set command shows proper memcached format', () => {
      const quickStartSection = document.querySelector('#quick-start');
      const preElements = quickStartSection.querySelectorAll('pre');

      let hasProperFormat = false;
      for (const pre of preElements) {
        const text = pre.textContent;
        // Should contain format: set <key> <flags> <ttl> <bytes>
        if (text.includes('set') &&
            (text.includes('<flags>') || text.includes(' 0 0 '))) {
          hasProperFormat = true;
          break;
        }
      }

      expect(hasProperFormat).toBe(true);
    });
  });

  describe('Test Case 3: Get Command Example', () => {
    test('A code or pre element contains get command syntax', () => {
      const quickStartSection = document.querySelector('#quick-start');
      const codeElements = quickStartSection.querySelectorAll('pre, code');

      let hasGetCommand = false;
      for (const el of codeElements) {
        const text = el.textContent;
        // Check for get command
        if (text.includes('get') && (text.includes('<key>') || text.includes('mykey'))) {
          hasGetCommand = true;
          break;
        }
      }

      expect(hasGetCommand).toBe(true);
    });

    test('Get command shows proper memcached format', () => {
      const quickStartSection = document.querySelector('#quick-start');
      const preElements = quickStartSection.querySelectorAll('pre');

      let hasProperFormat = false;
      for (const pre of preElements) {
        const text = pre.textContent;
        // Should contain format: get <key>
        if (text.includes('get') && text.includes('mykey')) {
          hasProperFormat = true;
          break;
        }
      }

      expect(hasProperFormat).toBe(true);
    });
  });

  describe('Test Case 4: Monospace Font Styling', () => {
    test('Code blocks use font-family containing monospace', () => {
      // Check the embedded style for monospace font
      const styleElements = document.querySelectorAll('style');
      let hasMonospaceStyle = false;

      for (const style of styleElements) {
        const cssText = style.textContent;
        // Look for font-family declarations with monospace
        if (cssText.includes('monospace') &&
            (cssText.includes('pre') || cssText.includes('code'))) {
          hasMonospaceStyle = true;
          break;
        }
      }

      expect(hasMonospaceStyle).toBe(true);
    });

    test('Pre and code elements are present in Quick Start section', () => {
      const quickStartSection = document.querySelector('#quick-start');
      const preElements = quickStartSection.querySelectorAll('pre');
      const codeElements = quickStartSection.querySelectorAll('code');

      // Should have multiple code blocks
      expect(preElements.length).toBeGreaterThan(0);
      expect(codeElements.length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 5: Code Block Visual Distinction', () => {
    test('Code blocks have background color different from main content', () => {
      const styleElements = document.querySelectorAll('style');
      let hasDistinctBackground = false;
      let codeBackground = null;
      let bodyBackground = null;

      for (const style of styleElements) {
        const cssText = style.textContent;

        // Look for pre background color
        const preMatch = cssText.match(/pre[^{]*\{[^}]*background(?:-color)?:\s*([^;]+)/);
        if (preMatch) {
          codeBackground = preMatch[1].trim();
        }

        // Look for body background color
        const bodyMatch = cssText.match(/body[^{]*\{[^}]*background(?:-color)?:\s*([^;]+)/);
        if (bodyMatch) {
          bodyBackground = bodyMatch[1].trim();
        }

        // Also check CSS variables
        if (cssText.includes('--bg-code') && cssText.includes('--bg-primary')) {
          hasDistinctBackground = true;
        }
      }

      // Either have distinct backgrounds or CSS variables for different colors
      expect(hasDistinctBackground || (codeBackground && codeBackground !== bodyBackground)).toBe(true);
    });

    test('Code blocks have border or distinct styling', () => {
      const styleElements = document.querySelectorAll('style');
      let hasDistinctStyling = false;

      for (const style of styleElements) {
        const cssText = style.textContent;
        // Look for pre styling with border, border-radius, or padding
        if (cssText.includes('pre') &&
            (cssText.includes('border') ||
             cssText.includes('border-radius') ||
             cssText.includes('padding'))) {
          hasDistinctStyling = true;
          break;
        }
      }

      expect(hasDistinctStyling).toBe(true);
    });
  });

  describe('Test Case 6: Server Startup Instructions', () => {
    test('Instructions for running MirDB server are present', () => {
      const quickStartSection = document.querySelector('#quick-start');
      const textContent = quickStartSection.textContent.toLowerCase();

      // Should have instructions about starting/running the server
      const hasServerInstructions =
        textContent.includes('start') ||
        textContent.includes('run') ||
        textContent.includes('server');

      expect(hasServerInstructions).toBe(true);
    });

    test('Server startup command is shown in code block', () => {
      const quickStartSection = document.querySelector('#quick-start');
      const preElements = quickStartSection.querySelectorAll('pre');

      let hasServerCommand = false;
      for (const pre of preElements) {
        const text = pre.textContent;
        // Check for cargo run or server startup command
        if (text.includes('cargo run') ||
            text.includes('mirdb-server') ||
            text.includes('telnet')) {
          hasServerCommand = true;
          break;
        }
      }

      expect(hasServerCommand).toBe(true);
    });

    test('Default port or connection info is provided', () => {
      const quickStartSection = document.querySelector('#quick-start');
      const textContent = quickStartSection.textContent;

      // Should mention the port or connection details
      const hasConnectionInfo =
        textContent.includes('12333') ||
        textContent.includes('localhost') ||
        textContent.includes('telnet');

      expect(hasConnectionInfo).toBe(true);
    });
  });
});
