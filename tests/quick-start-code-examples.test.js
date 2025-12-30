/**
 * E2E and Unit Tests for Quick-Start Code Examples (REQ-5)
 *
 * These tests verify that the MirDB homepage correctly displays
 * quick-start code examples for common programming languages/protocols
 * as specified in the PRD.
 */

const fs = require('fs');
const path = require('path');

describe('Quick-Start Code Examples - REQ-5', () => {
  let document;
  let htmlContent;

  beforeAll(() => {
    // Load the HTML file
    const htmlPath = path.join(__dirname, '..', 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Parse HTML using jsdom
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  describe('Quick Start Section Structure', () => {
    test('should have a quick-start section with id "quickstart"', () => {
      const quickStartSection = document.getElementById('quickstart');
      expect(quickStartSection).toBeTruthy();
    });

    test('should have a section title for Quick Start', () => {
      const quickStartSection = document.getElementById('quickstart');
      const sectionTitle = quickStartSection.querySelector('.section-title');
      expect(sectionTitle).toBeTruthy();
      expect(sectionTitle.textContent).toContain('Quick Start');
    });

    test('should have data-testid attribute for quick-start section', () => {
      const quickStartSection = document.querySelector('[data-testid="quick-start-section"]');
      expect(quickStartSection).toBeTruthy();
    });
  });

  describe('Test Case 1: Telnet Protocol Example', () => {
    /**
     * Test Case ID: 1
     * Input: Check for telnet protocol example
     * Expected: Code block shows telnet connection with SET/GET/DELETE examples
     */
    test('should display telnet protocol code block', () => {
      const telnetCodeBlock = document.querySelector('[data-testid="telnet-protocol-code-block"]');
      expect(telnetCodeBlock).toBeTruthy();
    });

    test('should show telnet connection command', () => {
      const telnetCodeBlock = document.querySelector('[data-testid="telnet-protocol-code-block"]');
      const codeContent = telnetCodeBlock.textContent;
      expect(codeContent).toContain('telnet');
      expect(codeContent).toContain('localhost');
      expect(codeContent).toContain('12333');
    });

    test('should show SET command example', () => {
      const telnetCodeBlock = document.querySelector('[data-testid="telnet-protocol-code-block"]');
      const codeContent = telnetCodeBlock.textContent;
      expect(codeContent).toContain('set');
      expect(codeContent).toContain('STORED');
    });

    test('should show GET command example', () => {
      const telnetCodeBlock = document.querySelector('[data-testid="telnet-protocol-code-block"]');
      const codeContent = telnetCodeBlock.textContent;
      expect(codeContent).toContain('get');
      expect(codeContent).toContain('VALUE');
      expect(codeContent).toContain('END');
    });

    test('should show DELETE command example', () => {
      const telnetCodeBlock = document.querySelector('[data-testid="telnet-protocol-code-block"]');
      const codeContent = telnetCodeBlock.textContent;
      expect(codeContent).toContain('delete');
      expect(codeContent).toContain('DELETED');
    });

    test('should have data-language attribute for protocol', () => {
      const telnetCodeBlock = document.querySelector('[data-testid="telnet-protocol-code-block"]');
      const dataLanguage = telnetCodeBlock.getAttribute('data-language');
      expect(dataLanguage).toBeTruthy();
      expect(dataLanguage).toMatch(/memcached|protocol/i);
    });
  });

  describe('Test Case 2: Syntax Highlighting', () => {
    /**
     * Test Case ID: 2
     * Input: Verify code syntax highlighting
     * Expected: Code blocks have syntax highlighting applied (keywords, strings, etc.)
     */
    test('should have syntax highlighting for comments', () => {
      const telnetCodeBlock = document.querySelector('[data-testid="telnet-protocol-code-block"]');
      const commentSpans = telnetCodeBlock.querySelectorAll('.code-comment');
      expect(commentSpans.length).toBeGreaterThan(0);
    });

    test('should have syntax highlighting for keywords', () => {
      const telnetCodeBlock = document.querySelector('[data-testid="telnet-protocol-code-block"]');
      const keywordSpans = telnetCodeBlock.querySelectorAll('.code-keyword');
      expect(keywordSpans.length).toBeGreaterThan(0);
    });

    test('should have syntax highlighting for responses/strings', () => {
      const telnetCodeBlock = document.querySelector('[data-testid="telnet-protocol-code-block"]');
      const responseSpans = telnetCodeBlock.querySelectorAll('.code-response');
      expect(responseSpans.length).toBeGreaterThan(0);
    });

    test('should have syntax highlighting for commands', () => {
      const telnetCodeBlock = document.querySelector('[data-testid="telnet-protocol-code-block"]');
      const commandSpans = telnetCodeBlock.querySelectorAll('.code-command');
      expect(commandSpans.length).toBeGreaterThan(0);
    });

    test('should have syntax highlighting for numbers', () => {
      const telnetCodeBlock = document.querySelector('[data-testid="telnet-protocol-code-block"]');
      const numberSpans = telnetCodeBlock.querySelectorAll('.code-number');
      expect(numberSpans.length).toBeGreaterThan(0);
    });

    test('installation code block should have syntax highlighting', () => {
      const installCodeBlock = document.querySelector('[data-testid="installation-code-block"]');
      expect(installCodeBlock).toBeTruthy();

      const commentSpans = installCodeBlock.querySelectorAll('.code-comment');
      expect(commentSpans.length).toBeGreaterThan(0);

      const commandSpans = installCodeBlock.querySelectorAll('.code-command');
      expect(commandSpans.length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 3: Monospace Font', () => {
    /**
     * Test Case ID: 3
     * Input: Check code block uses monospace font
     * Expected: Code examples are rendered in monospace font family
     */
    test('code block should have monospace font class defined in CSS', () => {
      // Check that the CSS defines monospace font for code-block
      const styleTag = document.querySelector('style');
      const cssContent = styleTag.textContent;

      // Check for monospace font in .code-block styles
      expect(cssContent).toMatch(/\.code-block[^{]*\{[^}]*font-family[^}]*mono/i);
    });

    test('code block should contain code element', () => {
      const telnetCodeBlock = document.querySelector('[data-testid="telnet-protocol-code-block"]');
      const codeElement = telnetCodeBlock.querySelector('code');
      expect(codeElement).toBeTruthy();
    });

    test('code block should contain pre element for proper formatting', () => {
      const telnetCodeBlock = document.querySelector('[data-testid="telnet-protocol-code-block"]');
      const preElement = telnetCodeBlock.querySelector('pre');
      expect(preElement).toBeTruthy();
    });
  });

  describe('Test Case 4: Copy-Friendly Code', () => {
    /**
     * Test Case ID: 4
     * Input: Verify code examples are copy-friendly
     * Expected: Code blocks can be selected and copied without line numbers or artifacts
     */
    test('code block should not contain line numbers', () => {
      const telnetCodeBlock = document.querySelector('[data-testid="telnet-protocol-code-block"]');
      const codeContent = telnetCodeBlock.textContent;

      // Check that content doesn't start with line numbers (like "1.", "2.", etc.)
      const lines = codeContent.trim().split('\n');
      const hasLineNumbers = lines.some(line => /^\s*\d+[\.\:\s]/.test(line.trim()));
      expect(hasLineNumbers).toBe(false);
    });

    test('code block should use pre element for preserving whitespace', () => {
      const telnetCodeBlock = document.querySelector('[data-testid="telnet-protocol-code-block"]');
      const preElement = telnetCodeBlock.querySelector('pre');
      expect(preElement).toBeTruthy();
    });

    test('code block should not have non-selectable elements mixed with code', () => {
      const telnetCodeBlock = document.querySelector('[data-testid="telnet-protocol-code-block"]');

      // All content should be within pre > code structure
      const preElement = telnetCodeBlock.querySelector('pre');
      const codeElement = preElement?.querySelector('code');
      expect(codeElement).toBeTruthy();

      // The code element should contain the actual code content
      expect(codeElement.textContent.length).toBeGreaterThan(0);
    });

    test('installation code block should also be copy-friendly', () => {
      const installCodeBlock = document.querySelector('[data-testid="installation-code-block"]');
      const preElement = installCodeBlock.querySelector('pre');
      const codeElement = preElement?.querySelector('code');

      expect(preElement).toBeTruthy();
      expect(codeElement).toBeTruthy();

      // Should contain actual installation commands
      const content = codeElement.textContent;
      expect(content).toContain('git clone');
      expect(content).toContain('cargo build');
    });
  });

  describe('Installation Code Block', () => {
    test('should display installation code block', () => {
      const installCodeBlock = document.querySelector('[data-testid="installation-code-block"]');
      expect(installCodeBlock).toBeTruthy();
    });

    test('should show git clone command', () => {
      const installCodeBlock = document.querySelector('[data-testid="installation-code-block"]');
      const codeContent = installCodeBlock.textContent;
      expect(codeContent).toContain('git clone');
      expect(codeContent).toContain('mirdb');
    });

    test('should show cargo build command', () => {
      const installCodeBlock = document.querySelector('[data-testid="installation-code-block"]');
      const codeContent = installCodeBlock.textContent;
      expect(codeContent).toContain('cargo build');
      expect(codeContent).toContain('--release');
    });

    test('should show run command', () => {
      const installCodeBlock = document.querySelector('[data-testid="installation-code-block"]');
      const codeContent = installCodeBlock.textContent;
      expect(codeContent).toContain('./target/release/mirdb');
    });

    test('should have data-language attribute for bash', () => {
      const installCodeBlock = document.querySelector('[data-testid="installation-code-block"]');
      const dataLanguage = installCodeBlock.getAttribute('data-language');
      expect(dataLanguage).toBe('bash');
    });
  });
});
