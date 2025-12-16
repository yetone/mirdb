/**
 * Code Examples Display Tests
 * Scenario: Verify that code examples demonstrate client connection and are copy-paste ready (REQ-10)
 *
 * This test file validates:
 * - Multiple pre > code blocks exist on the page
 * - Prism.js is loaded for syntax highlighting
 * - Code elements have language-* class for highlighting
 * - Code example showing telnet or nc connection to port 12333 exists
 * - Code example demonstrating SET and GET commands exists
 */

const fs = require('fs');
const path = require('path');

describe('Code Examples Display (REQ-10)', () => {
  let htmlContent;
  let document;

  beforeAll(() => {
    const htmlPath = path.resolve(__dirname, '../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  // Test Case 1: Multiple pre > code blocks exist on the page
  describe('Test Case 1: Pre > code blocks exist', () => {
    test('should have multiple pre > code blocks on the page', () => {
      const preCodeBlocks = document.querySelectorAll('pre > code');

      // We expect multiple code blocks for a page with code examples
      expect(preCodeBlocks.length).toBeGreaterThan(0);

      // Verify we have meaningful content in the code blocks
      let hasNonEmptyCode = false;
      preCodeBlocks.forEach(block => {
        if (block.textContent.trim().length > 0) {
          hasNonEmptyCode = true;
        }
      });

      expect(hasNonEmptyCode).toBe(true);
    });

    test('should have at least 3 code blocks for installation, usage, and config', () => {
      const preCodeBlocks = document.querySelectorAll('pre > code');
      expect(preCodeBlocks.length).toBeGreaterThanOrEqual(3);
    });
  });

  // Test Case 2: Prism.js is loaded
  describe('Test Case 2: Prism.js is loaded', () => {
    test('should include Prism.js script in the page', () => {
      const scripts = document.querySelectorAll('script[src]');

      let hasPrism = false;
      scripts.forEach(script => {
        const src = script.getAttribute('src');
        if (src && src.toLowerCase().includes('prism')) {
          hasPrism = true;
        }
      });

      expect(hasPrism).toBe(true);
    });

    test('should include Prism.css stylesheet in the page', () => {
      const styleLinks = document.querySelectorAll('link[rel="stylesheet"]');

      let hasPrismCss = false;
      styleLinks.forEach(link => {
        const href = link.getAttribute('href');
        if (href && href.toLowerCase().includes('prism')) {
          hasPrismCss = true;
        }
      });

      expect(hasPrismCss).toBe(true);
    });

    test('Prism.js file should exist and be readable', () => {
      const prismPath = path.resolve(__dirname, '../prism.js');
      expect(fs.existsSync(prismPath)).toBe(true);

      const prismContent = fs.readFileSync(prismPath, 'utf8');
      expect(prismContent.length).toBeGreaterThan(0);
      expect(prismContent).toContain('Prism');
    });
  });

  // Test Case 3: Code elements have language-* class
  describe('Test Case 3: Code blocks have language class', () => {
    test('should have code elements with language-* class for syntax highlighting', () => {
      const codeBlocks = document.querySelectorAll('pre > code');

      let hasLanguageClass = false;
      const languageClasses = [];

      codeBlocks.forEach(block => {
        const classList = block.className;
        if (classList && classList.includes('language-')) {
          hasLanguageClass = true;
          const matches = classList.match(/language-\w+/g);
          if (matches) {
            languageClasses.push(...matches);
          }
        }
      });

      expect(hasLanguageClass).toBe(true);
      expect(languageClasses.length).toBeGreaterThan(0);
    });

    test('should have code blocks with recognized language classes (bash, toml, etc.)', () => {
      const codeBlocks = document.querySelectorAll('pre > code');

      const recognizedLanguages = ['bash', 'shell', 'toml', 'rust', 'javascript', 'js', 'sh', 'console'];
      let hasRecognizedLanguage = false;

      codeBlocks.forEach(block => {
        const classList = block.className;
        recognizedLanguages.forEach(lang => {
          if (classList && classList.includes(`language-${lang}`)) {
            hasRecognizedLanguage = true;
          }
        });
      });

      expect(hasRecognizedLanguage).toBe(true);
    });

    test('all code blocks should have a language class', () => {
      const codeBlocks = document.querySelectorAll('pre > code');

      codeBlocks.forEach(block => {
        const classList = block.className;
        // Each code block should have a language class for proper syntax highlighting
        expect(classList).toMatch(/language-\w+/);
      });
    });
  });

  // Test Case 4: Telnet/connection example exists
  describe('Test Case 4: Telnet/connection example exists', () => {
    test('should have code example showing telnet or nc connection to port 12333', () => {
      const codeBlocks = document.querySelectorAll('pre > code');

      let hasConnectionExample = false;
      let connectionExampleContent = '';

      codeBlocks.forEach(block => {
        const text = block.textContent;
        // Check for telnet or nc (netcat) connection to port 12333
        if ((text.includes('telnet') || text.includes('nc ') || text.includes('netcat'))
            && text.includes('12333')) {
          hasConnectionExample = true;
          connectionExampleContent = text;
        }
      });

      expect(hasConnectionExample).toBe(true);
      expect(connectionExampleContent).toMatch(/(telnet|nc|netcat).*12333|12333.*(telnet|nc|netcat)/s);
    });

    test('connection example should include localhost or 0.0.0.0', () => {
      const codeBlocks = document.querySelectorAll('pre > code');

      let hasLocalhost = false;

      codeBlocks.forEach(block => {
        const text = block.textContent;
        if ((text.includes('telnet') || text.includes('nc ')) &&
            (text.includes('localhost') || text.includes('127.0.0.1') || text.includes('0.0.0.0'))) {
          hasLocalhost = true;
        }
      });

      expect(hasLocalhost).toBe(true);
    });

    test('should have copy-paste ready connection command', () => {
      const codeBlocks = document.querySelectorAll('pre > code');

      let foundValidCommand = false;

      codeBlocks.forEach(block => {
        const text = block.textContent;
        // Check for a valid, complete connection command
        if (text.match(/telnet\s+localhost\s+12333/) ||
            text.match(/nc\s+localhost\s+12333/)) {
          foundValidCommand = true;
        }
      });

      expect(foundValidCommand).toBe(true);
    });
  });

  // Test Case 5: SET/GET example exists
  describe('Test Case 5: SET/GET example exists', () => {
    test('should have code example demonstrating SET command', () => {
      const codeBlocks = document.querySelectorAll('pre > code');

      let hasSetExample = false;

      codeBlocks.forEach(block => {
        const text = block.textContent;
        // Check for SET command in memcached format
        if (text.toLowerCase().includes('set ') &&
            (text.includes('STORED') || text.match(/set\s+\w+\s+\d+\s+\d+\s+\d+/i))) {
          hasSetExample = true;
        }
      });

      expect(hasSetExample).toBe(true);
    });

    test('should have code example demonstrating GET command', () => {
      const codeBlocks = document.querySelectorAll('pre > code');

      let hasGetExample = false;

      codeBlocks.forEach(block => {
        const text = block.textContent;
        // Check for GET command usage
        if (text.toLowerCase().includes('get ') &&
            (text.includes('VALUE') || text.includes('END') || text.match(/get\s+\w+/i))) {
          hasGetExample = true;
        }
      });

      expect(hasGetExample).toBe(true);
    });

    test('should show complete SET/GET workflow with expected output', () => {
      const codeBlocks = document.querySelectorAll('pre > code');

      let hasCompleteWorkflow = false;

      codeBlocks.forEach(block => {
        const text = block.textContent;
        // A complete workflow should show SET, its response (STORED), GET, and the VALUE response
        if (text.includes('set ') && text.includes('STORED') &&
            text.includes('get ') && (text.includes('VALUE') || text.includes('END'))) {
          hasCompleteWorkflow = true;
        }
      });

      expect(hasCompleteWorkflow).toBe(true);
    });

    test('SET example should follow memcached protocol format', () => {
      const codeBlocks = document.querySelectorAll('pre > code');

      let hasValidSetSyntax = false;

      codeBlocks.forEach(block => {
        const text = block.textContent;
        // Memcached SET format: set <key> <flags> <exptime> <bytes>
        if (text.match(/set\s+\w+\s+\d+\s+\d+\s+\d+/i)) {
          hasValidSetSyntax = true;
        }
      });

      expect(hasValidSetSyntax).toBe(true);
    });
  });

  // Additional tests for copy-paste readiness
  describe('Code Examples are Copy-Paste Ready', () => {
    test('code examples should not have HTML entities that would break copy-paste', () => {
      const codeBlocks = document.querySelectorAll('pre > code');

      codeBlocks.forEach(block => {
        const innerHTML = block.innerHTML;
        // The rendered textContent should be proper code, not escaped HTML
        // We're checking that essential characters are properly rendered
        const text = block.textContent;

        // Should not have broken/escaped content after rendering
        expect(text).not.toMatch(/&lt;(?!-)/); // Allow <!-- comments but not <
        expect(text).not.toMatch(/&gt;(?!-)/); // Allow --> comments but not >
        expect(text).not.toMatch(/&amp;(?!amp;)/);
      });
    });

    test('installation commands should be complete and executable', () => {
      const codeBlocks = document.querySelectorAll('pre > code');

      let hasGitClone = false;
      let hasCargoBuild = false;

      codeBlocks.forEach(block => {
        const text = block.textContent;
        if (text.includes('git clone')) {
          hasGitClone = true;
        }
        if (text.includes('cargo build')) {
          hasCargoBuild = true;
        }
      });

      expect(hasGitClone).toBe(true);
      expect(hasCargoBuild).toBe(true);
    });

    test('configuration example should be valid TOML', () => {
      const codeBlocks = document.querySelectorAll('pre > code.language-toml');

      expect(codeBlocks.length).toBeGreaterThan(0);

      // Check that TOML code has proper key = value format
      let hasValidToml = false;
      codeBlocks.forEach(block => {
        const text = block.textContent;
        // TOML should have key = value pairs
        if (text.match(/\w+\s*=\s*["']?[\w\d.\-:/]+["']?/)) {
          hasValidToml = true;
        }
      });

      expect(hasValidToml).toBe(true);
    });
  });
});
