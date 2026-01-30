/**
 * Copy to Clipboard Integration Tests
 * Owner: Scenario 4 - Usage Section with Code Examples
 *
 * Tests for copy-to-clipboard functionality
 * Using Jest's built-in jsdom environment
 */
const fs = require('fs');
const path = require('path');

describe('Copy to Clipboard Integration Tests', () => {
  let htmlContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Set up DOM using jest-environment-jsdom's global document
    document.body.innerHTML = htmlContent;
  });

  describe('TC4: Syntax highlighting library references', () => {
    test('HTML contains Prism.js CSS reference', () => {
      expect(htmlContent).toMatch(/prism.*\.css/i);
    });

    test('HTML contains Prism.js JavaScript reference', () => {
      expect(htmlContent).toMatch(/prism\.min\.js/i);
    });

    test('HTML contains Prism bash language component', () => {
      expect(htmlContent).toMatch(/prism-bash.*\.js/i);
    });
  });

  describe('Copy button DOM structure', () => {
    test('Copy buttons exist in the DOM', () => {
      const copyButtons = document.querySelectorAll('.code-block__copy');
      expect(copyButtons.length).toBeGreaterThan(0);
    });

    test('Each code block has a copy button', () => {
      const codeBlocks = document.querySelectorAll('.code-block');
      const copyButtons = document.querySelectorAll('.code-block__copy');

      // Each code block should have a copy button
      expect(copyButtons.length).toBe(codeBlocks.length);
    });

    test('Copy buttons have correct aria-label', () => {
      const copyButtons = document.querySelectorAll('.code-block__copy');

      copyButtons.forEach(button => {
        expect(button.getAttribute('aria-label')).toBe('Copy code to clipboard');
      });
    });
  });

  describe('Code block content structure', () => {
    test('Code blocks contain pre and code elements', () => {
      const codeBlocks = document.querySelectorAll('.code-block');

      codeBlocks.forEach(block => {
        const pre = block.querySelector('pre');
        const code = block.querySelector('code');

        expect(pre).not.toBeNull();
        expect(code).not.toBeNull();
      });
    });

    test('Code elements have language class for syntax highlighting', () => {
      const codeElements = document.querySelectorAll('.code-block code');

      codeElements.forEach(code => {
        const hasLanguageClass = code.classList.contains('language-bash') ||
                                  Array.from(code.classList).some(c => c.startsWith('language-'));
        expect(hasLanguageClass).toBe(true);
      });
    });

    test('Code blocks contain actual code content', () => {
      const codeElements = document.querySelectorAll('.code-block code');

      codeElements.forEach(code => {
        const content = code.textContent.trim();
        expect(content.length).toBeGreaterThan(0);
      });
    });
  });

  describe('Copy functionality JS structure', () => {
    test('copy-to-clipboard.js exports initCopyButtons function', () => {
      const jsPath = path.join(__dirname, '../../js/components/copy-to-clipboard.js');
      const jsContent = fs.readFileSync(jsPath, 'utf-8');

      expect(jsContent).toMatch(/export\s+function\s+initCopyButtons\s*\(\)/);
    });

    test('copy-to-clipboard.js exports copyText function', () => {
      const jsPath = path.join(__dirname, '../../js/components/copy-to-clipboard.js');
      const jsContent = fs.readFileSync(jsPath, 'utf-8');

      expect(jsContent).toMatch(/export\s+async\s+function\s+copyText\s*\(/);
    });

    test('copy-to-clipboard.js contains showCopyFeedback function', () => {
      const jsPath = path.join(__dirname, '../../js/components/copy-to-clipboard.js');
      const jsContent = fs.readFileSync(jsPath, 'utf-8');

      expect(jsContent).toMatch(/function\s+showCopyFeedback\s*\(/);
    });

    test('main.js imports and calls initCopyButtons', () => {
      const jsPath = path.join(__dirname, '../../js/main.js');
      const jsContent = fs.readFileSync(jsPath, 'utf-8');

      // Check that initCopyButtons is imported
      expect(jsContent).toMatch(/import\s*{\s*initCopyButtons\s*}/);
      // Check that initCopyButtons is called
      expect(jsContent).toMatch(/initCopyButtons\(\)/);
    });

    test('copyText uses navigator.clipboard API', () => {
      const jsPath = path.join(__dirname, '../../js/components/copy-to-clipboard.js');
      const jsContent = fs.readFileSync(jsPath, 'utf-8');

      expect(jsContent).toMatch(/navigator\.clipboard/);
    });

    test('copyText has fallback for older browsers', () => {
      const jsPath = path.join(__dirname, '../../js/components/copy-to-clipboard.js');
      const jsContent = fs.readFileSync(jsPath, 'utf-8');

      // Should have fallbackCopyText function
      expect(jsContent).toMatch(/fallbackCopyText/);
      // Should use execCommand as fallback
      expect(jsContent).toMatch(/execCommand.*copy/);
    });
  });

  describe('Visual feedback structure', () => {
    test('Copy button has copy icon', () => {
      const copyIcons = document.querySelectorAll('.code-block__copy .copy-icon');
      expect(copyIcons.length).toBeGreaterThan(0);
    });

    test('Copy button has check icon (for success state)', () => {
      const checkIcons = document.querySelectorAll('.code-block__copy .check-icon');
      expect(checkIcons.length).toBeGreaterThan(0);
    });

    test('Copy button has text element', () => {
      const copyTextElements = document.querySelectorAll('.code-block__copy .copy-text');
      expect(copyTextElements.length).toBeGreaterThan(0);

      copyTextElements.forEach(el => {
        expect(el.textContent).toBe('Copy');
      });
    });
  });

  describe('Code content verification', () => {
    test('Server start command is present', () => {
      const firstCodeBlock = document.querySelector('.code-block code');
      expect(firstCodeBlock.textContent).toContain('mirdb');
      expect(firstCodeBlock.textContent).toContain('--port');
    });

    test('SET command example is present', () => {
      const codeContent = document.body.innerHTML;
      expect(codeContent).toContain('SET');
      expect(codeContent).toContain('STORED');
    });

    test('GET command example is present', () => {
      const codeContent = document.body.innerHTML;
      expect(codeContent).toContain('GET');
      expect(codeContent).toContain('VALUE');
    });
  });
});
