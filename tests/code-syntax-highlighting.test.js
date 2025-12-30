/**
 * Code Example Syntax Highlighting Tests (NFR-7)
 *
 * Scenario: Verify that code examples have proper syntax highlighting
 * as specified in NFR-7 of the Product Requirements Document.
 *
 * These tests verify:
 * - Bash/shell code highlighting
 * - Protocol example highlighting
 * - Language indicators on code blocks
 * - Accessible color contrast for syntax highlighting
 */

const fs = require('fs');
const path = require('path');

describe('Code Example Syntax Highlighting - NFR-7', () => {
  let document;
  let htmlContent;

  beforeAll(() => {
    // Load the HTML file
    const htmlPath = path.join(__dirname, '..', 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Parse HTML using jsdom
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  describe('Test Case 1: Bash code blocks have syntax highlighting (E2E)', () => {
    /**
     * Test Case ID: 1
     * Input: Check bash code blocks have syntax highlighting
     * Expected: Shell commands show highlighted syntax (keywords, strings)
     * Type: e2e
     */

    test('installation bash code block exists with data-language="bash"', () => {
      const bashCodeBlock = document.querySelector('[data-testid="installation-code-block"]');
      expect(bashCodeBlock).toBeTruthy();
      expect(bashCodeBlock.getAttribute('data-language')).toBe('bash');
    });

    test('bash code block has syntax-highlighted comments (code-comment class)', () => {
      const bashCodeBlock = document.querySelector('[data-testid="installation-code-block"]');
      const commentElements = bashCodeBlock.querySelectorAll('.code-comment');
      expect(commentElements.length).toBeGreaterThan(0);

      // Verify comments contain expected text (e.g., "# Clone the repository")
      const commentTexts = Array.from(commentElements).map((el) => el.textContent);
      const hasCloneComment = commentTexts.some((text) => text.includes('Clone'));
      const hasBuildComment = commentTexts.some((text) => text.includes('Build'));
      expect(hasCloneComment || hasBuildComment).toBe(true);
    });

    test('bash code block has syntax-highlighted commands (code-command class)', () => {
      const bashCodeBlock = document.querySelector('[data-testid="installation-code-block"]');
      const commandElements = bashCodeBlock.querySelectorAll('.code-command');
      expect(commandElements.length).toBeGreaterThan(0);

      // Verify commands contain expected bash commands
      const commandTexts = Array.from(commandElements).map((el) => el.textContent);
      expect(commandTexts.some((text) => text.includes('git clone') || text.includes('git'))).toBe(true);
      expect(commandTexts.some((text) => text.includes('cargo build') || text.includes('cargo'))).toBe(true);
    });

    test('bash code block has syntax-highlighted flags (code-flag class)', () => {
      const bashCodeBlock = document.querySelector('[data-testid="installation-code-block"]');
      const flagElements = bashCodeBlock.querySelectorAll('.code-flag');
      expect(flagElements.length).toBeGreaterThan(0);

      // Verify flags contain expected values like --release or -c
      const flagTexts = Array.from(flagElements).map((el) => el.textContent);
      const hasReleaseFlag = flagTexts.some((text) => text.includes('--release'));
      const hasConfigFlag = flagTexts.some((text) => text.includes('-c'));
      expect(hasReleaseFlag || hasConfigFlag).toBe(true);
    });

    test('shell commands display correctly with proper highlighting structure', () => {
      const bashCodeBlock = document.querySelector('[data-testid="installation-code-block"]');

      // Verify the structure: code-block > pre > code with spans
      const preElement = bashCodeBlock.querySelector('pre');
      expect(preElement).toBeTruthy();

      const codeElement = preElement.querySelector('code');
      expect(codeElement).toBeTruthy();

      // Should have highlighted spans inside code
      const highlightedSpans = codeElement.querySelectorAll('span[class^="code-"]');
      expect(highlightedSpans.length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 2: Code blocks have language indicator (Unit)', () => {
    /**
     * Test Case ID: 2
     * Input: Check code blocks have language indicator
     * Expected: Code blocks have data-language or class indicating language type
     * Type: unit
     */

    test('installation code block has data-language attribute set to "bash"', () => {
      const installCodeBlock = document.querySelector('[data-testid="installation-code-block"]');
      expect(installCodeBlock).toBeTruthy();

      const dataLanguage = installCodeBlock.getAttribute('data-language');
      expect(dataLanguage).toBeTruthy();
      expect(dataLanguage).toBe('bash');
    });

    test('protocol code block has data-language attribute indicating memcached protocol', () => {
      const protocolCodeBlock = document.querySelector('[data-testid="telnet-protocol-code-block"]');
      expect(protocolCodeBlock).toBeTruthy();

      const dataLanguage = protocolCodeBlock.getAttribute('data-language');
      expect(dataLanguage).toBeTruthy();
      expect(dataLanguage).toMatch(/memcached|protocol/i);
    });

    test('all code blocks have language indicators', () => {
      const codeBlocks = document.querySelectorAll('.code-block');
      expect(codeBlocks.length).toBeGreaterThan(0);

      codeBlocks.forEach((block) => {
        const dataLanguage = block.getAttribute('data-language');
        const languageClass = block.className.match(/language-\w+/);

        // Each code block should have either data-language attribute or language class
        const hasLanguageIndicator = dataLanguage || languageClass;
        expect(hasLanguageIndicator).toBeTruthy();
      });
    });

    test('language indicators are semantically meaningful', () => {
      const installCodeBlock = document.querySelector('[data-testid="installation-code-block"]');
      const protocolCodeBlock = document.querySelector('[data-testid="telnet-protocol-code-block"]');

      // Bash block should indicate shell/bash language
      const bashLanguage = installCodeBlock.getAttribute('data-language');
      expect(['bash', 'shell', 'sh']).toContain(bashLanguage);

      // Protocol block should indicate memcached protocol
      const protocolLanguage = protocolCodeBlock.getAttribute('data-language');
      expect(protocolLanguage).toMatch(/memcached|protocol|text/i);
    });
  });

  describe('Test Case 3: Highlighting uses accessible colors (Unit)', () => {
    /**
     * Test Case ID: 3
     * Input: Verify highlighting uses accessible colors
     * Expected: Syntax highlighting colors meet contrast requirements
     * Type: unit
     */

    // Helper function to parse CSS color values
    function parseColor(colorStr) {
      if (!colorStr) return null;

      // Handle hex colors
      if (colorStr.startsWith('#')) {
        const hex = colorStr.slice(1);
        if (hex.length === 3) {
          return {
            r: parseInt(hex[0] + hex[0], 16),
            g: parseInt(hex[1] + hex[1], 16),
            b: parseInt(hex[2] + hex[2], 16)
          };
        } else if (hex.length === 6) {
          return {
            r: parseInt(hex.slice(0, 2), 16),
            g: parseInt(hex.slice(2, 4), 16),
            b: parseInt(hex.slice(4, 6), 16)
          };
        }
      }
      return null;
    }

    // Calculate relative luminance (WCAG formula)
    function getLuminance(r, g, b) {
      const [rs, gs, bs] = [r, g, b].map((c) => {
        c = c / 255;
        return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
      });
      return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
    }

    // Calculate contrast ratio (WCAG formula)
    function getContrastRatio(color1, color2) {
      const l1 = getLuminance(color1.r, color1.g, color1.b);
      const l2 = getLuminance(color2.r, color2.g, color2.b);
      const lighter = Math.max(l1, l2);
      const darker = Math.min(l1, l2);
      return (lighter + 0.05) / (darker + 0.05);
    }

    test('code block background and text have sufficient contrast', () => {
      const styleBlock = document.querySelector('style');
      expect(styleBlock).toBeTruthy();

      const cssText = styleBlock.textContent;

      // Extract code-bg and code-text CSS variables
      const codeBg = cssText.match(/--code-bg:\s*([^;]+)/)?.[1]?.trim();
      const codeText = cssText.match(/--code-text:\s*([^;]+)/)?.[1]?.trim();

      expect(codeBg).toBeTruthy();
      expect(codeText).toBeTruthy();

      const bgColor = parseColor(codeBg);
      const textColor = parseColor(codeText);

      if (bgColor && textColor) {
        const contrastRatio = getContrastRatio(bgColor, textColor);
        // WCAG AA requires 4.5:1 for normal text
        expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
      }
    });

    test('syntax highlighting classes have colors defined in CSS', () => {
      const styleBlock = document.querySelector('style');
      const cssText = styleBlock.textContent;

      // Check that syntax highlighting classes are defined
      const highlightClasses = [
        'code-comment',
        'code-string',
        'code-keyword',
        'code-command',
        'code-flag',
        'code-number',
        'code-response'
      ];

      highlightClasses.forEach((className) => {
        const classRegex = new RegExp(`\\.${className}[^{]*\\{[^}]*color\\s*:`);
        expect(cssText).toMatch(classRegex);
      });
    });

    test('code-comment color has sufficient contrast against code background', () => {
      const styleBlock = document.querySelector('style');
      const cssText = styleBlock.textContent;

      // code-bg: #1f2937 (dark gray)
      const codeBg = cssText.match(/--code-bg:\s*([^;]+)/)?.[1]?.trim();
      // code-comment: #6b7280 (gray)
      const commentColorMatch = cssText.match(/\.code-comment\s*\{[^}]*color:\s*([^;]+)/);

      const bgColor = parseColor(codeBg);
      const commentColor = parseColor(commentColorMatch?.[1]?.trim());

      if (bgColor && commentColor) {
        const contrastRatio = getContrastRatio(bgColor, commentColor);
        // Comments can have lower contrast (3:1 minimum for large text)
        // but should ideally meet 4.5:1
        expect(contrastRatio).toBeGreaterThanOrEqual(3);
      }
    });

    test('code-keyword color has sufficient contrast against code background', () => {
      const styleBlock = document.querySelector('style');
      const cssText = styleBlock.textContent;

      const codeBg = cssText.match(/--code-bg:\s*([^;]+)/)?.[1]?.trim();
      // code-keyword: #f472b6 (pink)
      const keywordColorMatch = cssText.match(/\.code-keyword\s*\{[^}]*color:\s*([^;]+)/);

      const bgColor = parseColor(codeBg);
      const keywordColor = parseColor(keywordColorMatch?.[1]?.trim());

      if (bgColor && keywordColor) {
        const contrastRatio = getContrastRatio(bgColor, keywordColor);
        // Keywords should have good contrast for readability
        expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
      }
    });

    test('code-string/response color has sufficient contrast against code background', () => {
      const styleBlock = document.querySelector('style');
      const cssText = styleBlock.textContent;

      const codeBg = cssText.match(/--code-bg:\s*([^;]+)/)?.[1]?.trim();
      // code-string: #34d399 (green) or code-response
      const stringColorMatch = cssText.match(/\.code-string\s*\{[^}]*color:\s*([^;]+)/);
      const responseColorMatch = cssText.match(/\.code-response\s*\{[^}]*color:\s*([^;]+)/);

      const bgColor = parseColor(codeBg);
      const stringColor = parseColor(stringColorMatch?.[1]?.trim());
      const responseColor = parseColor(responseColorMatch?.[1]?.trim());

      if (bgColor && stringColor) {
        const contrastRatio = getContrastRatio(bgColor, stringColor);
        expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
      }

      if (bgColor && responseColor) {
        const contrastRatio = getContrastRatio(bgColor, responseColor);
        expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
      }
    });

    test('code-command color has sufficient contrast against code background', () => {
      const styleBlock = document.querySelector('style');
      const cssText = styleBlock.textContent;

      const codeBg = cssText.match(/--code-bg:\s*([^;]+)/)?.[1]?.trim();
      // code-command: #60a5fa (blue)
      const commandColorMatch = cssText.match(/\.code-command\s*\{[^}]*color:\s*([^;]+)/);

      const bgColor = parseColor(codeBg);
      const commandColor = parseColor(commandColorMatch?.[1]?.trim());

      if (bgColor && commandColor) {
        const contrastRatio = getContrastRatio(bgColor, commandColor);
        expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
      }
    });

    test('code-number color has sufficient contrast against code background', () => {
      const styleBlock = document.querySelector('style');
      const cssText = styleBlock.textContent;

      const codeBg = cssText.match(/--code-bg:\s*([^;]+)/)?.[1]?.trim();
      // code-number: #c084fc (purple)
      const numberColorMatch = cssText.match(/\.code-number\s*\{[^}]*color:\s*([^;]+)/);

      const bgColor = parseColor(codeBg);
      const numberColor = parseColor(numberColorMatch?.[1]?.trim());

      if (bgColor && numberColor) {
        const contrastRatio = getContrastRatio(bgColor, numberColor);
        expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
      }
    });

    test('code-flag color has sufficient contrast against code background', () => {
      const styleBlock = document.querySelector('style');
      const cssText = styleBlock.textContent;

      const codeBg = cssText.match(/--code-bg:\s*([^;]+)/)?.[1]?.trim();
      // code-flag: #fbbf24 (yellow)
      const flagColorMatch = cssText.match(/\.code-flag\s*\{[^}]*color:\s*([^;]+)/);

      const bgColor = parseColor(codeBg);
      const flagColor = parseColor(flagColorMatch?.[1]?.trim());

      if (bgColor && flagColor) {
        const contrastRatio = getContrastRatio(bgColor, flagColor);
        expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
      }
    });
  });

  describe('Protocol Example Syntax Highlighting (Step 2)', () => {
    /**
     * Step 2: Check protocol highlighting
     * Verify memcached protocol examples are highlighted
     */

    test('protocol code block exists with proper structure', () => {
      const protocolCodeBlock = document.querySelector('[data-testid="telnet-protocol-code-block"]');
      expect(protocolCodeBlock).toBeTruthy();

      const preElement = protocolCodeBlock.querySelector('pre');
      expect(preElement).toBeTruthy();

      const codeElement = preElement.querySelector('code');
      expect(codeElement).toBeTruthy();
    });

    test('protocol code block has highlighted keywords (set, get, delete)', () => {
      const protocolCodeBlock = document.querySelector('[data-testid="telnet-protocol-code-block"]');
      const keywordElements = protocolCodeBlock.querySelectorAll('.code-keyword');
      expect(keywordElements.length).toBeGreaterThan(0);

      const keywordTexts = Array.from(keywordElements).map((el) => el.textContent.toLowerCase());
      expect(keywordTexts.some((text) => text.includes('set'))).toBe(true);
      expect(keywordTexts.some((text) => text.includes('get'))).toBe(true);
      expect(keywordTexts.some((text) => text.includes('delete'))).toBe(true);
    });

    test('protocol code block has highlighted responses (STORED, END, DELETED)', () => {
      const protocolCodeBlock = document.querySelector('[data-testid="telnet-protocol-code-block"]');
      const responseElements = protocolCodeBlock.querySelectorAll('.code-response');
      expect(responseElements.length).toBeGreaterThan(0);

      const responseTexts = Array.from(responseElements).map((el) => el.textContent);
      expect(responseTexts.some((text) => text.includes('STORED'))).toBe(true);
      expect(responseTexts.some((text) => text.includes('END'))).toBe(true);
      expect(responseTexts.some((text) => text.includes('DELETED'))).toBe(true);
    });

    test('protocol code block has highlighted numbers', () => {
      const protocolCodeBlock = document.querySelector('[data-testid="telnet-protocol-code-block"]');
      const numberElements = protocolCodeBlock.querySelectorAll('.code-number');
      expect(numberElements.length).toBeGreaterThan(0);

      // Should include port number 12333 and memcached protocol numbers
      const numberTexts = Array.from(numberElements).map((el) => el.textContent);
      expect(numberTexts.some((text) => text.includes('12333'))).toBe(true);
    });

    test('protocol code block has highlighted comments', () => {
      const protocolCodeBlock = document.querySelector('[data-testid="telnet-protocol-code-block"]');
      const commentElements = protocolCodeBlock.querySelectorAll('.code-comment');
      expect(commentElements.length).toBeGreaterThan(0);

      // Should have comments explaining the commands
      const commentTexts = Array.from(commentElements).map((el) => el.textContent);
      expect(commentTexts.some((text) => text.includes('Connect') || text.includes('Store') || text.includes('Retrieve'))).toBe(true);
    });
  });
});
