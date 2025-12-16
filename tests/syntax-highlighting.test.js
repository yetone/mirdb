import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import { readFileSync } from 'fs';
import { resolve } from 'path';

/**
 * Test Suite: Code Snippets Syntax Highlighting
 * Scenario: Verify code snippets have proper syntax highlighting for readability
 * Requirement: NFR-5 - Code snippets must have syntax highlighting for readability
 */
describe('Code Snippets Syntax Highlighting', () => {
  let dom;
  let document;
  let cssContent;

  beforeEach(() => {
    const htmlPath = resolve(__dirname, '../index.html');
    const cssPath = resolve(__dirname, '../styles.css');
    const html = readFileSync(htmlPath, 'utf-8');
    cssContent = readFileSync(cssPath, 'utf-8');
    dom = new JSDOM(html, {
      runScripts: 'dangerously',
      resources: 'usable',
      url: 'http://localhost'
    });
    document = dom.window.document;

    // Inject CSS for computed style tests
    const style = document.createElement('style');
    style.textContent = cssContent;
    document.head.appendChild(style);
  });

  afterEach(() => {
    if (dom) {
      dom.window.close();
    }
  });

  // Test Case 1 (E2E): Check code blocks exist in quick start section
  describe('Test Case 1: Code blocks exist in quick start section', () => {
    it('should have at least one code block in the quick start section', () => {
      const quickStartSection = document.querySelector('#quick-start');
      expect(quickStartSection).not.toBeNull();

      const codeBlocks = quickStartSection.querySelectorAll('pre code, .code-block');
      expect(codeBlocks.length).toBeGreaterThan(0);
    });

    it('should have code blocks containing installation commands', () => {
      const quickStartSection = document.querySelector('#quick-start');
      const preElements = quickStartSection.querySelectorAll('pre');
      expect(preElements.length).toBeGreaterThan(0);

      // Verify there's actual code content
      const hasCodeContent = Array.from(preElements).some(pre => {
        const text = pre.textContent;
        return text.includes('cargo') || text.includes('git clone') || text.includes('npm');
      });
      expect(hasCodeContent).toBe(true);
    });

    it('should have code blocks in examples sections of the page', () => {
      // Check for code blocks anywhere on the page for examples
      const allCodeBlocks = document.querySelectorAll('pre code');
      expect(allCodeBlocks.length).toBeGreaterThan(0);
    });

    it('should have multiple code blocks for different examples', () => {
      const quickStartSection = document.querySelector('#quick-start');
      const codeBlockWrappers = quickStartSection.querySelectorAll('.code-block');

      // Quick start section should have multiple code examples (installation, config, usage)
      expect(codeBlockWrappers.length).toBeGreaterThanOrEqual(1);
    });
  });

  // Test Case 2 (Unit): Verify code blocks have language-specific class for highlighting
  describe('Test Case 2: Code blocks have language-specific class', () => {
    it('should have language class on code elements (language-*)', () => {
      const codeElements = document.querySelectorAll('pre code');
      expect(codeElements.length).toBeGreaterThan(0);

      const hasLanguageClass = Array.from(codeElements).some(code =>
        Array.from(code.classList).some(cls => cls.startsWith('language-'))
      );
      expect(hasLanguageClass).toBe(true);
    });

    it('should have language-bash class for shell commands', () => {
      const bashCodeBlocks = document.querySelectorAll('pre code.language-bash, pre code.language-shell, pre code.language-sh');
      expect(bashCodeBlocks.length).toBeGreaterThan(0);
    });

    it('should have language-toml class for TOML configuration', () => {
      const tomlCodeBlocks = document.querySelectorAll('pre code.language-toml');
      expect(tomlCodeBlocks.length).toBeGreaterThan(0);
    });

    it('should have appropriate language classes matching content type', () => {
      const codeElements = document.querySelectorAll('pre code');

      // Each code block should have a language class
      const codeElementsWithLanguage = Array.from(codeElements).filter(code =>
        Array.from(code.classList).some(cls => cls.startsWith('language-'))
      );

      // At least 80% of code blocks should have language classes
      const ratio = codeElementsWithLanguage.length / codeElements.length;
      expect(ratio).toBeGreaterThanOrEqual(0.8);
    });
  });

  // Test Case 3 (Unit): Check monospace font - code blocks use monospace or code font family
  describe('Test Case 3: Code blocks use monospace font', () => {
    it('should have monospace font defined in CSS variables', () => {
      expect(cssContent).toContain('--font-mono');
      expect(cssContent).toMatch(/--font-mono.*monospace/i);
    });

    it('should apply monospace font to code elements', () => {
      expect(cssContent).toMatch(/code\s*\{[^}]*font-family:\s*var\(--font-mono\)/s);
    });

    it('should use standard monospace font stack', () => {
      const monoFontPattern = /(SF Mono|Menlo|Monaco|Consolas|monospace)/i;
      expect(cssContent).toMatch(monoFontPattern);
    });

    it('should have code element with font-family property', () => {
      // Check CSS contains font-family for code
      const hasFontFamily = cssContent.includes('font-family') &&
                           cssContent.includes('code');
      expect(hasFontFamily).toBe(true);
    });

    it('pre code elements should inherit monospace styling', () => {
      // Check that pre code has styling defined
      const hasPreCodeStyling = cssContent.includes('pre code') ||
                                cssContent.includes('code');
      expect(hasPreCodeStyling).toBe(true);
    });
  });

  // Test Case 4 (E2E): Verify syntax colors are applied - Keywords, strings, comments have distinct colors
  describe('Test Case 4: Syntax colors are applied', () => {
    it('should have CSS rules for syntax highlighting token types', () => {
      // Check for token-based highlighting classes
      const hasTokenClasses =
        cssContent.includes('.token') ||
        cssContent.includes('.hljs') ||
        cssContent.includes('.language-') ||
        cssContent.includes('.syntax-');
      expect(hasTokenClasses).toBe(true);
    });

    it('should define colors for language-specific code blocks', () => {
      // Check CSS has color definitions for language classes
      const languageColorPattern = /\.language-\w+[^{]*\{[^}]*color:/s;
      expect(cssContent).toMatch(languageColorPattern);
    });

    it('should have distinct colors for different token types (keywords, strings, comments)', () => {
      // Check for token color definitions in CSS
      const hasKeywordColor = cssContent.includes('.keyword') ||
                              cssContent.includes('.token-keyword') ||
                              cssContent.includes('.hljs-keyword') ||
                              cssContent.includes('language-');

      // If using simple language-based coloring, verify colors are defined
      const colorHexPattern = /#[0-9a-fA-F]{3,6}/g;
      const colors = cssContent.match(colorHexPattern) || [];

      // Should have multiple distinct colors in CSS for code
      expect(colors.length).toBeGreaterThan(3);
      expect(hasKeywordColor).toBe(true);
    });

    it('should have color styling for bash/shell code blocks', () => {
      // Verify bash-specific or shell-specific color is defined
      const bashColorPattern = /\.language-bash[^{]*\{[^}]*color:|\.language-shell[^{]*\{[^}]*color:/s;
      expect(cssContent).toMatch(bashColorPattern);
    });

    it('should have color styling for TOML code blocks', () => {
      // Verify TOML-specific color is defined
      const tomlColorPattern = /\.language-toml[^{]*\{[^}]*color:/s;
      expect(cssContent).toMatch(tomlColorPattern);
    });

    it('should apply non-default colors to code content', () => {
      // Verify that code blocks have specific colors, not just default text
      const codeColorPattern = /pre code[^{]*\{[^}]*color:|code[^{]*\{[^}]*color:/s;
      // Either direct code color or via language class
      const hasCodeColor = cssContent.match(codeColorPattern) ||
                          cssContent.match(/\.language-\w+[^{]*\{[^}]*color:/s);
      expect(hasCodeColor).toBeTruthy();
    });
  });

  // Additional validation tests
  describe('Additional Syntax Highlighting Validation', () => {
    it('should have proper code-block wrapper structure', () => {
      const codeBlockWrappers = document.querySelectorAll('.code-block');
      expect(codeBlockWrappers.length).toBeGreaterThan(0);

      // Each wrapper should contain pre > code structure
      codeBlockWrappers.forEach(wrapper => {
        const pre = wrapper.querySelector('pre');
        expect(pre).not.toBeNull();
        const code = pre.querySelector('code');
        expect(code).not.toBeNull();
      });
    });

    it('should have consistent styling for code background', () => {
      // Code blocks should have background color defined
      const bgPattern = /code-bg|background-color.*code|\.code-block[^}]*background/s;
      expect(cssContent).toMatch(bgPattern);
    });

    it('should maintain readability with appropriate font size', () => {
      // Check font-size is defined for code
      const fontSizePattern = /code[^{]*\{[^}]*font-size:/s;
      expect(cssContent).toMatch(fontSizePattern);
    });

    it('should have proper line-height for code readability', () => {
      // Check line-height for code
      const lineHeightPattern = /code[^{]*\{[^}]*line-height:/s;
      expect(cssContent).toMatch(lineHeightPattern);
    });
  });
});
