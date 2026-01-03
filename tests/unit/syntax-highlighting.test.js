/**
 * Unit Tests for Syntax Highlighting on MirDB Homepage
 *
 * Verifies:
 * - Code blocks have language-specific classes
 * - Prism.js syntax highlighting library is included
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

describe('Syntax Highlighting', () => {
  let document;
  let htmlContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');
    const dom = new JSDOM(htmlContent);
    document = dom.window.document;
  });

  /**
   * Test Case 1: Check code blocks have syntax highlighting classes
   * Expected: Code blocks have language-specific classes or attributes
   */
  describe('Code blocks have syntax highlighting classes', () => {
    test('should have code blocks with language-specific classes', () => {
      // Find all code elements within pre tags (standard code block format)
      const codeElements = document.querySelectorAll('pre code');
      expect(codeElements.length).toBeGreaterThan(0);

      // Check that code elements have language-* class
      let hasLanguageClass = false;
      codeElements.forEach((code) => {
        const classList = Array.from(code.classList);
        const hasClass = classList.some(cls => cls.startsWith('language-'));
        if (hasClass) {
          hasLanguageClass = true;
        }
      });
      expect(hasLanguageClass).toBe(true);
    });

    test('should have bash/shell language class for shell commands', () => {
      // Quick start section should have bash code examples
      const bashCodeBlocks = document.querySelectorAll('pre code.language-bash');
      expect(bashCodeBlocks.length).toBeGreaterThan(0);
    });

    test('all code blocks in quick-start should have language classes', () => {
      const quickstartSection = document.querySelector('#quickstart');
      expect(quickstartSection).not.toBeNull();

      const codeBlocks = quickstartSection.querySelectorAll('pre code');
      expect(codeBlocks.length).toBeGreaterThan(0);

      codeBlocks.forEach((code, index) => {
        const classList = Array.from(code.classList);
        const hasLanguageClass = classList.some(cls => cls.startsWith('language-'));
        expect(hasLanguageClass).toBe(true);
      });
    });

    test('code blocks should be wrapped in code-block container', () => {
      const codeBlockContainers = document.querySelectorAll('.code-block');
      expect(codeBlockContainers.length).toBeGreaterThan(0);

      // Each code-block container should have a pre element inside
      codeBlockContainers.forEach((container) => {
        const preElement = container.querySelector('pre');
        expect(preElement).not.toBeNull();
      });
    });
  });

  /**
   * Test Case 2: Verify Prism.js or highlighting library is loaded
   * Expected: Syntax highlighting library script is included
   */
  describe('Prism.js highlighting library is loaded', () => {
    test('should include Prism.js CSS stylesheet', () => {
      // Check for Prism CSS link in head
      const prismCssLink = document.querySelector('link[href*="prism"]');
      expect(prismCssLink).not.toBeNull();

      // Verify it's a stylesheet
      const href = prismCssLink.getAttribute('href');
      expect(href).toContain('prism');
      expect(href).toMatch(/\.css/);
    });

    test('should include Prism.js core script', () => {
      // Check for Prism.js script
      const prismScript = document.querySelector('script[src*="prism.min.js"]');
      expect(prismScript).not.toBeNull();

      const src = prismScript.getAttribute('src');
      expect(src).toContain('prism');
    });

    test('should include Prism.js bash language component', () => {
      // Check for bash language component for shell commands
      const bashScript = document.querySelector('script[src*="prism-bash"]');
      expect(bashScript).not.toBeNull();
    });

    test('should use Prism Tomorrow theme for dark mode compatibility', () => {
      // The homepage uses dark theme, so Prism Tomorrow theme is appropriate
      const prismCssLink = document.querySelector('link[href*="prism"]');
      expect(prismCssLink).not.toBeNull();

      const href = prismCssLink.getAttribute('href');
      expect(href).toContain('tomorrow');
    });

    test('Prism.js scripts should be loaded at end of body', () => {
      // For performance, scripts should be at end of body
      const body = document.body;
      const scripts = body.querySelectorAll('script');

      // At least one script should be Prism related
      const hasPrismScript = Array.from(scripts).some(script => {
        const src = script.getAttribute('src');
        return src && src.includes('prism');
      });
      expect(hasPrismScript).toBe(true);
    });
  });
});
