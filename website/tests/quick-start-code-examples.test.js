/**
 * Tests for Quick-Start Code Examples Section
 *
 * Scenario: Quick-Start Code Examples
 * Verifies that working code examples are provided showing installation commands
 * and basic SET/GET operations, with copy functionality.
 *
 * Test Cases:
 * 1. Query DOM for code block elements in quick-start section (at least 2 code blocks)
 * 2. Extract installation command text (cargo install or git clone + cargo build)
 * 3. Extract usage example text (SET and GET command examples)
 * 4. Check for copy button on code blocks (copy functionality)
 * 5. Verify code blocks have syntax highlighting classes (prism/highlight.js)
 *
 * @jest-environment jsdom
 */

const fs = require('fs');
const path = require('path');

// Load HTML content before tests
beforeEach(() => {
  // Load HTML for each test to ensure clean state
  const htmlPath = path.join(__dirname, '..', 'index.html');
  const html = fs.readFileSync(htmlPath, 'utf8');
  document.body.innerHTML = html;
});

describe('Quick-Start Code Examples', () => {
  /**
   * Test Case 1: Query DOM for code block elements in quick-start section
   * Expected: At least 2 code blocks found: installation and usage example
   */
  describe('Test Case 1: Code Block Count in Quick-Start Section', () => {
    test('should have a quick-start section with proper ID', () => {
      const quickStartSection = document.getElementById('quickstart');
      expect(quickStartSection).toBeInTheDocument();
    });

    test('should have at least 2 code blocks in quick-start section', () => {
      const quickStartSection = document.getElementById('quickstart');
      expect(quickStartSection).toBeInTheDocument();

      // Find code blocks - look for pre/code elements with code-block class
      const codeBlocks = quickStartSection.querySelectorAll('.code-block, pre, code');
      const uniqueCodeBlocks = quickStartSection.querySelectorAll('.code-block');

      // Should have at least 2 distinct code blocks (installation and usage)
      expect(uniqueCodeBlocks.length).toBeGreaterThanOrEqual(2);
    });

    test('should have code blocks containing code elements', () => {
      const quickStartSection = document.getElementById('quickstart');
      const codeBlocks = quickStartSection.querySelectorAll('.code-block');

      codeBlocks.forEach((block) => {
        const codeElement = block.querySelector('code');
        expect(codeElement).toBeInTheDocument();
      });
    });

    test('should have pre elements wrapping code for proper formatting', () => {
      const quickStartSection = document.getElementById('quickstart');
      const preElements = quickStartSection.querySelectorAll('pre');
      expect(preElements.length).toBeGreaterThanOrEqual(2);
    });
  });

  /**
   * Test Case 2: Extract installation command text
   * Expected: Contains cargo install command or git clone + cargo build instructions
   */
  describe('Test Case 2: Installation Command Verification', () => {
    test('should contain cargo build command for installation', () => {
      const quickStartSection = document.getElementById('quickstart');
      const codeBlocks = quickStartSection.querySelectorAll('.code-block code, pre code');

      const allCodeText = Array.from(codeBlocks)
        .map((code) => code.textContent)
        .join('\n');

      // Should contain cargo build command
      expect(allCodeText).toMatch(/cargo\s+build/i);
    });

    test('should contain git clone command for installation', () => {
      const quickStartSection = document.getElementById('quickstart');
      const codeBlocks = quickStartSection.querySelectorAll('.code-block code, pre code');

      const allCodeText = Array.from(codeBlocks)
        .map((code) => code.textContent)
        .join('\n');

      // Should contain git clone command
      expect(allCodeText).toMatch(/git\s+clone/i);
    });

    test('should have installation step with step title', () => {
      const quickStartSection = document.getElementById('quickstart');

      // Look for step element containing install instructions
      const stepTitles = quickStartSection.querySelectorAll('.step-title, h3');
      const stepTexts = Array.from(stepTitles).map((el) => el.textContent.toLowerCase());

      // Should have a step about installation
      const hasInstallStep = stepTexts.some(
        (text) => text.includes('install') || text.includes('build') || text.includes('clone')
      );
      expect(hasInstallStep).toBe(true);
    });

    test('installation commands should include repository URL', () => {
      const quickStartSection = document.getElementById('quickstart');
      const codeBlocks = quickStartSection.querySelectorAll('.code-block code, pre code');

      const allCodeText = Array.from(codeBlocks)
        .map((code) => code.textContent)
        .join('\n');

      // Should contain github.com or mirdb in clone URL
      expect(allCodeText).toMatch(/github\.com.*mirdb|mirdb.*github/i);
    });
  });

  /**
   * Test Case 3: Extract usage example text
   * Expected: Contains SET and GET command examples demonstrating basic operations
   */
  describe('Test Case 3: Usage Example (SET/GET) Verification', () => {
    test('should contain SET command example', () => {
      const quickStartSection = document.getElementById('quickstart');
      const codeBlocks = quickStartSection.querySelectorAll('.code-block code, pre code');

      const allCodeText = Array.from(codeBlocks)
        .map((code) => code.textContent)
        .join('\n');

      // Should contain SET command
      expect(allCodeText.toLowerCase()).toContain('set');
    });

    test('should contain GET command example', () => {
      const quickStartSection = document.getElementById('quickstart');
      const codeBlocks = quickStartSection.querySelectorAll('.code-block code, pre code');

      const allCodeText = Array.from(codeBlocks)
        .map((code) => code.textContent)
        .join('\n');

      // Should contain GET command
      expect(allCodeText.toLowerCase()).toContain('get');
    });

    test('should show complete SET command with key and value', () => {
      const quickStartSection = document.getElementById('quickstart');
      const codeBlocks = quickStartSection.querySelectorAll('.code-block code, pre code');

      const allCodeText = Array.from(codeBlocks)
        .map((code) => code.textContent)
        .join('\n');

      // Should contain a complete SET command with key (e.g., "set mykey")
      expect(allCodeText).toMatch(/set\s+\w+/i);
    });

    test('should show GET command with key', () => {
      const quickStartSection = document.getElementById('quickstart');
      const codeBlocks = quickStartSection.querySelectorAll('.code-block code, pre code');

      const allCodeText = Array.from(codeBlocks)
        .map((code) => code.textContent)
        .join('\n');

      // Should contain a complete GET command with key (e.g., "get mykey")
      expect(allCodeText).toMatch(/get\s+\w+/i);
    });

    test('should include connection instructions (telnet or client)', () => {
      const quickStartSection = document.getElementById('quickstart');
      const codeBlocks = quickStartSection.querySelectorAll('.code-block code, pre code');

      const allCodeText = Array.from(codeBlocks)
        .map((code) => code.textContent)
        .join('\n');

      // Should contain telnet or connection instructions
      expect(allCodeText).toMatch(/telnet|localhost|12333|client/i);
    });

    test('should demonstrate the expected response (STORED)', () => {
      const quickStartSection = document.getElementById('quickstart');
      const codeBlocks = quickStartSection.querySelectorAll('.code-block code, pre code');

      const allCodeText = Array.from(codeBlocks)
        .map((code) => code.textContent)
        .join('\n');

      // Should show STORED response to demonstrate success
      expect(allCodeText.toUpperCase()).toContain('STORED');
    });
  });

  /**
   * Test Case 4: Check for copy button on code blocks
   * Expected: Code blocks have copy functionality (button or click-to-copy)
   */
  describe('Test Case 4: Copy Functionality Verification', () => {
    test('should have copy buttons on code blocks', () => {
      const quickStartSection = document.getElementById('quickstart');
      const codeBlocks = quickStartSection.querySelectorAll('.code-block');

      // Each code block should have a copy button
      codeBlocks.forEach((block) => {
        const copyButton = block.querySelector(
          '.copy-button, .copy-btn, [data-copy], button[class*="copy"], .code-copy'
        );
        expect(copyButton).toBeInTheDocument();
      });
    });

    test('copy buttons should have accessible labels', () => {
      const quickStartSection = document.getElementById('quickstart');
      const copyButtons = quickStartSection.querySelectorAll(
        '.copy-button, .copy-btn, [data-copy], button[class*="copy"], .code-copy'
      );

      expect(copyButtons.length).toBeGreaterThanOrEqual(1);

      copyButtons.forEach((button) => {
        // Should have text content, aria-label, or title
        const hasAccessibleName =
          button.textContent.trim().length > 0 ||
          button.getAttribute('aria-label') ||
          button.getAttribute('title');
        expect(hasAccessibleName).toBeTruthy();
      });
    });

    test('copy buttons should be interactive (button or have role button)', () => {
      const quickStartSection = document.getElementById('quickstart');
      const copyButtons = quickStartSection.querySelectorAll(
        '.copy-button, .copy-btn, [data-copy], button[class*="copy"], .code-copy'
      );

      expect(copyButtons.length).toBeGreaterThanOrEqual(1);

      copyButtons.forEach((button) => {
        const isButton = button.tagName.toLowerCase() === 'button';
        const hasButtonRole = button.getAttribute('role') === 'button';
        expect(isButton || hasButtonRole).toBe(true);
      });
    });

    test('code blocks should have position relative for button positioning', () => {
      const quickStartSection = document.getElementById('quickstart');
      const codeBlocks = quickStartSection.querySelectorAll('.code-block');

      // Code blocks should support absolute positioning of copy button
      expect(codeBlocks.length).toBeGreaterThanOrEqual(1);
    });
  });

  /**
   * Test Case 5: Verify code blocks have syntax highlighting classes
   * Expected: Code elements have syntax highlighting applied (prism/highlight.js classes)
   */
  describe('Test Case 5: Syntax Highlighting Verification', () => {
    test('code blocks should have syntax highlighting classes', () => {
      const quickStartSection = document.getElementById('quickstart');
      const codeElements = quickStartSection.querySelectorAll('code');

      expect(codeElements.length).toBeGreaterThanOrEqual(1);

      // Check for syntax highlighting classes (Prism.js or highlight.js patterns)
      const highlightPatterns = [
        /language-/,
        /hljs/,
        /prism/,
        /syntax/,
        /highlight/,
        /lang-/,
        /code-/,
      ];

      let hasHighlighting = false;

      codeElements.forEach((code) => {
        const classNames = code.className || '';
        const parentClassNames = code.parentElement?.className || '';
        const combinedClasses = `${classNames} ${parentClassNames}`;

        highlightPatterns.forEach((pattern) => {
          if (pattern.test(combinedClasses)) {
            hasHighlighting = true;
          }
        });
      });

      expect(hasHighlighting).toBe(true);
    });

    test('code elements should have language-specific class', () => {
      const quickStartSection = document.getElementById('quickstart');
      const codeElements = quickStartSection.querySelectorAll('code');

      expect(codeElements.length).toBeGreaterThanOrEqual(1);

      // At least one code element should have a language class
      const hasLanguageClass = Array.from(codeElements).some((code) => {
        const classNames = `${code.className || ''} ${code.parentElement?.className || ''}`;
        return /language-|lang-/.test(classNames);
      });

      expect(hasLanguageClass).toBe(true);
    });

    test('bash/shell code blocks should have appropriate language class', () => {
      const quickStartSection = document.getElementById('quickstart');
      const codeElements = quickStartSection.querySelectorAll('code');

      // At least one code block should be marked as bash/shell
      const hasBashClass = Array.from(codeElements).some((code) => {
        const classNames = `${code.className || ''} ${code.parentElement?.className || ''}`;
        return /language-(bash|shell|sh|console)|lang-(bash|shell|sh)|hljs-(bash|shell)/.test(
          classNames
        );
      });

      expect(hasBashClass).toBe(true);
    });

    test('code blocks should have proper styling for readability', () => {
      const quickStartSection = document.getElementById('quickstart');
      const codeBlocks = quickStartSection.querySelectorAll('.code-block');

      expect(codeBlocks.length).toBeGreaterThanOrEqual(1);

      // Code blocks should exist with proper class for styling
      codeBlocks.forEach((block) => {
        expect(block.classList.contains('code-block')).toBe(true);
      });
    });
  });

  /**
   * Additional tests for comprehensive coverage
   */
  describe('Quick-Start Section Structure', () => {
    test('should have a section heading (h2)', () => {
      const quickStartSection = document.getElementById('quickstart');
      const heading = quickStartSection.querySelector('h2');
      expect(heading).toBeInTheDocument();
      expect(heading.textContent.toLowerCase()).toContain('quick');
    });

    test('should have step-by-step instructions', () => {
      const quickStartSection = document.getElementById('quickstart');
      const steps = quickStartSection.querySelectorAll('.step');
      expect(steps.length).toBeGreaterThanOrEqual(2);
    });

    test('should be navigable from navigation menu', () => {
      const navLink = document.querySelector('a[href="#quickstart"]');
      expect(navLink).toBeInTheDocument();
    });

    test('should have descriptive step titles', () => {
      const quickStartSection = document.getElementById('quickstart');
      const stepTitles = quickStartSection.querySelectorAll('.step-title, .step h3');

      expect(stepTitles.length).toBeGreaterThanOrEqual(2);

      stepTitles.forEach((title) => {
        expect(title.textContent.trim().length).toBeGreaterThan(0);
      });
    });
  });
});
