import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import { readFileSync } from 'fs';
import { resolve } from 'path';

describe('Quick Start Installation Instructions', () => {
  let dom;
  let document;

  beforeEach(() => {
    const htmlPath = resolve(__dirname, '../index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    dom = new JSDOM(html, { runScripts: 'dangerously', resources: 'usable' });
    document = dom.window.document;
  });

  afterEach(() => {
    if (dom) {
      dom.window.close();
    }
  });

  // Test Case 1: Check quick start section exists
  describe('Test Case 1: Quick start section exists', () => {
    it('should have a section with id "quick-start" or "quickstart" or "getting-started"', () => {
      const quickStartSection = document.querySelector(
        '#quick-start, #quickstart, #getting-started, [id*="quick-start"], [id*="quickstart"], [id*="getting-started"]'
      );
      expect(quickStartSection).not.toBeNull();
    });

    it('should have a visible quick start heading', () => {
      const headings = document.querySelectorAll('h1, h2, h3');
      const quickStartHeading = Array.from(headings).find(
        h => h.textContent.toLowerCase().includes('quick start') ||
             h.textContent.toLowerCase().includes('quickstart') ||
             h.textContent.toLowerCase().includes('getting started') ||
             h.textContent.toLowerCase().includes('installation')
      );
      expect(quickStartHeading).not.toBeNull();
    });
  });

  // Test Case 2: Check for cargo installation command
  describe('Test Case 2: Cargo installation command', () => {
    it('should contain cargo build or cargo install command', () => {
      const codeBlocks = document.querySelectorAll('pre, code');
      const hasCargoCommand = Array.from(codeBlocks).some(
        block => block.textContent.includes('cargo build') ||
                 block.textContent.includes('cargo install') ||
                 block.textContent.includes('cargo run')
      );
      expect(hasCargoCommand).toBe(true);
    });

    it('should display the cargo command prominently in a code block', () => {
      const preElements = document.querySelectorAll('pre');
      const hasCargoInPre = Array.from(preElements).some(
        pre => pre.textContent.includes('cargo')
      );
      expect(hasCargoInPre).toBe(true);
    });
  });

  // Test Case 3: Verify code blocks have pre/code elements
  describe('Test Case 3: Code blocks use semantic elements', () => {
    it('should wrap installation commands in pre and code elements', () => {
      const preCodeElements = document.querySelectorAll('pre code, pre > code');
      expect(preCodeElements.length).toBeGreaterThan(0);
    });

    it('should have proper code element structure', () => {
      const codeElements = document.querySelectorAll('code');
      expect(codeElements.length).toBeGreaterThan(0);

      // Check that at least one code element is inside a pre element
      const preElements = document.querySelectorAll('pre');
      expect(preElements.length).toBeGreaterThan(0);
    });

    it('code blocks should have language class for syntax highlighting', () => {
      const codeElements = document.querySelectorAll('pre code');
      const hasLanguageClass = Array.from(codeElements).some(
        code => code.className.includes('language-') ||
                code.className.includes('hljs') ||
                code.hasAttribute('data-language') ||
                code.parentElement.hasAttribute('data-language')
      );
      expect(hasLanguageClass).toBe(true);
    });
  });

  // Test Case 4: Check for configuration example
  describe('Test Case 4: Configuration example', () => {
    it('should include TOML configuration example or reference', () => {
      const pageText = document.body.textContent;
      const codeBlocks = document.querySelectorAll('pre, code');

      const hasTomlConfig = Array.from(codeBlocks).some(block => {
        const text = block.textContent;
        return text.includes('toml') ||
               text.includes('.toml') ||
               text.includes('addr =') ||
               text.includes('work_dir =') ||
               text.includes('mirdb.toml');
      }) || pageText.toLowerCase().includes('config') ||
           pageText.toLowerCase().includes('configuration') ||
           pageText.toLowerCase().includes('.toml');

      expect(hasTomlConfig).toBe(true);
    });

    it('should mention configuration file path or example', () => {
      const pageText = document.body.textContent.toLowerCase();
      const hasConfigReference = pageText.includes('mirdb.toml') ||
                                  pageText.includes('config.toml') ||
                                  pageText.includes('configuration') ||
                                  pageText.includes('etc/mirdb');
      expect(hasConfigReference).toBe(true);
    });
  });

  // Test Case 5: Check for basic usage example
  describe('Test Case 5: Basic usage example', () => {
    it('should include code example showing how to connect', () => {
      const codeBlocks = document.querySelectorAll('pre, code');
      const hasUsageExample = Array.from(codeBlocks).some(block => {
        const text = block.textContent.toLowerCase();
        return text.includes('telnet') ||
               text.includes('connect') ||
               text.includes('set ') ||
               text.includes('get ') ||
               text.includes('memcached') ||
               text.includes('localhost') ||
               text.includes('127.0.0.1') ||
               text.includes(':12333');
      });
      expect(hasUsageExample).toBe(true);
    });

    it('should show basic memcached commands (get/set)', () => {
      const codeBlocks = document.querySelectorAll('pre, code');
      const pageText = document.body.textContent.toLowerCase();

      const hasBasicCommands = Array.from(codeBlocks).some(block => {
        const text = block.textContent.toLowerCase();
        return (text.includes('set') && text.includes('get')) ||
               text.includes('memcached');
      }) || (pageText.includes('set') && pageText.includes('get'));

      expect(hasBasicCommands).toBe(true);
    });
  });

  // Test Case 6: Verify copy button functionality exists
  describe('Test Case 6: Copy button functionality', () => {
    it('should have copy buttons or copy-to-clipboard functionality for code blocks', () => {
      // Check for copy buttons
      const copyButtons = document.querySelectorAll(
        'button[class*="copy"], ' +
        '.copy-button, ' +
        '.copy-btn, ' +
        '[data-copy], ' +
        '[onclick*="copy"], ' +
        '.code-copy, ' +
        '[aria-label*="copy"], ' +
        '[title*="copy"], ' +
        '[title*="Copy"]'
      );

      // Check for copy functionality in the code wrapper
      const codeWrappers = document.querySelectorAll('.code-block, .code-wrapper, .code-container');
      const hasWrapperWithCopy = Array.from(codeWrappers).some(
        wrapper => wrapper.querySelector('button') !== null
      );

      // Check for pre elements that might have copy functionality
      const preElements = document.querySelectorAll('pre');
      const preWithCopyButton = Array.from(preElements).some(pre => {
        const parent = pre.parentElement;
        return parent && (
          parent.querySelector('button') !== null ||
          parent.classList.contains('code-block')
        );
      });

      expect(copyButtons.length > 0 || hasWrapperWithCopy || preWithCopyButton).toBe(true);
    });

    it('should have copy functionality associated with code snippets', () => {
      // Check for onclick handlers related to copying
      const elementsWithCopyHandler = document.querySelectorAll('[onclick*="clipboard"], [onclick*="copy"]');

      // Check for data attributes that indicate copy functionality
      const elementsWithCopyData = document.querySelectorAll('[data-clipboard-target], [data-clipboard-text], [data-copy]');

      // Check for copy buttons near code blocks
      const codeBlocks = document.querySelectorAll('.code-block, .code-wrapper');
      const hasButtonNearCode = Array.from(codeBlocks).some(block => {
        return block.querySelector('button') !== null;
      });

      // Check for global copy function in scripts
      const scripts = document.querySelectorAll('script');
      const hasCopyFunction = Array.from(scripts).some(script => {
        const content = script.textContent || '';
        return content.includes('clipboard') ||
               content.includes('copyToClipboard') ||
               content.includes('navigator.clipboard') ||
               content.includes('execCommand');
      });

      expect(
        elementsWithCopyHandler.length > 0 ||
        elementsWithCopyData.length > 0 ||
        hasButtonNearCode ||
        hasCopyFunction
      ).toBe(true);
    });
  });
});
