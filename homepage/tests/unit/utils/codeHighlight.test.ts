/**
 * Unit tests for the codeHighlight utility.
 * Owner: Scenario 11 - Code Syntax Highlighting
 *
 * Tests syntax highlighting functionality using Prism.js
 * for shell, rust, and other languages.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  highlightCode,
  highlightAllCodeBlocks,
  initHighlighting,
  isLanguageSupported,
  createHighlightedCodeElement,
  getSupportedLanguages,
} from '../../../src/utils/codeHighlight';

describe('codeHighlight utility', () => {
  describe('highlightCode', () => {
    describe('shell/bash commands', () => {
      it('should highlight shell commands with syntax classes', () => {
        const code = 'cargo install mirdb';
        const result = highlightCode(code, 'bash');

        // Should contain token classes from Prism
        expect(result).toContain('class="token');
        expect(result).toContain('cargo');
        expect(result).toContain('install');
        expect(result).toContain('mirdb');
      });

      it('should highlight bash comments', () => {
        const code = '# This is a comment\necho "hello"';
        const result = highlightCode(code, 'bash');

        expect(result).toContain('class="token comment"');
        expect(result).toContain('This is a comment');
      });

      it('should handle shell language alias', () => {
        const code = 'ls -la';
        const result = highlightCode(code, 'shell');

        expect(result).toContain('class="token');
      });

      it('should handle sh language alias', () => {
        const code = 'pwd';
        const result = highlightCode(code, 'sh');

        expect(result).toContain('class="token');
      });

      it('should highlight complex shell commands', () => {
        const code = `nc localhost 12333
set mykey 0 0 5
hello
STORED`;
        const result = highlightCode(code, 'bash');

        // Verify the result contains the code content
        expect(result).toContain('localhost');
        expect(result).toContain('12333');
        expect(result).toContain('mykey');
      });
    });

    describe('Rust code', () => {
      it('should highlight Rust code with syntax classes', () => {
        const code = `fn main() {
    println!("Hello, world!");
}`;
        const result = highlightCode(code, 'rust');

        // Should contain token classes for Rust
        expect(result).toContain('class="token');
        expect(result).toContain('fn');
        expect(result).toContain('main');
      });

      it('should highlight Rust keywords', () => {
        const code = 'let mut x: u32 = 42;';
        const result = highlightCode(code, 'rust');

        expect(result).toContain('class="token keyword"');
        expect(result).toContain('let');
      });

      it('should handle rs language alias', () => {
        const code = 'let x = 5;';
        const result = highlightCode(code, 'rs');

        expect(result).toContain('class="token');
      });

      it('should highlight Rust string literals', () => {
        const code = 'let s = "hello world";';
        const result = highlightCode(code, 'rust');

        expect(result).toContain('class="token string"');
        expect(result).toContain('hello world');
      });

      it('should highlight Rust comments', () => {
        const code = '// This is a comment\nlet x = 5;';
        const result = highlightCode(code, 'rust');

        expect(result).toContain('class="token comment"');
      });
    });

    describe('TOML configuration', () => {
      it('should highlight TOML configuration', () => {
        const code = `[server]
address = "127.0.0.1"
port = 12333`;
        const result = highlightCode(code, 'toml');

        expect(result).toContain('class="token');
        expect(result).toContain('server');
      });
    });

    describe('edge cases', () => {
      it('should return empty string for empty input', () => {
        expect(highlightCode('', 'bash')).toBe('');
      });

      it('should return empty string for null-like input', () => {
        expect(highlightCode(null as unknown as string, 'bash')).toBe('');
        expect(highlightCode(undefined as unknown as string, 'bash')).toBe('');
      });

      it('should escape HTML for unsupported languages', () => {
        const code = '<script>alert("xss")</script>';
        const result = highlightCode(code, 'unsupported-lang-xyz');

        expect(result).not.toContain('<script>');
        expect(result).toContain('&lt;script&gt;');
      });

      it('should escape special HTML characters', () => {
        const code = '& < > " \'';
        const result = highlightCode(code, 'unknown');

        expect(result).toContain('&amp;');
        expect(result).toContain('&lt;');
        expect(result).toContain('&gt;');
        expect(result).toContain('&quot;');
        expect(result).toContain('&#39;');
      });

      it('should handle multiline code', () => {
        const code = `line1
line2
line3`;
        const result = highlightCode(code, 'bash');

        expect(result).toContain('line1');
        expect(result).toContain('line2');
        expect(result).toContain('line3');
      });
    });
  });

  describe('isLanguageSupported', () => {
    it('should return true for bash', () => {
      expect(isLanguageSupported('bash')).toBe(true);
    });

    it('should return true for shell alias', () => {
      expect(isLanguageSupported('shell')).toBe(true);
    });

    it('should return true for rust', () => {
      expect(isLanguageSupported('rust')).toBe(true);
    });

    it('should return true for rs alias', () => {
      expect(isLanguageSupported('rs')).toBe(true);
    });

    it('should return true for toml', () => {
      expect(isLanguageSupported('toml')).toBe(true);
    });

    it('should return false for unsupported language', () => {
      expect(isLanguageSupported('unsupported-lang-xyz')).toBe(false);
    });

    it('should be case-insensitive', () => {
      expect(isLanguageSupported('BASH')).toBe(true);
      expect(isLanguageSupported('Rust')).toBe(true);
    });
  });

  describe('getSupportedLanguages', () => {
    it('should return an array of supported languages', () => {
      const languages = getSupportedLanguages();

      expect(Array.isArray(languages)).toBe(true);
      expect(languages.length).toBeGreaterThan(0);
    });

    it('should include bash in supported languages', () => {
      const languages = getSupportedLanguages();
      expect(languages).toContain('bash');
    });

    it('should include rust in supported languages', () => {
      const languages = getSupportedLanguages();
      expect(languages).toContain('rust');
    });
  });

  describe('highlightAllCodeBlocks', () => {
    let container: HTMLElement;

    beforeEach(() => {
      container = document.createElement('div');
      document.body.appendChild(container);
    });

    afterEach(() => {
      document.body.removeChild(container);
    });

    it('should highlight code blocks with language class', () => {
      container.innerHTML = `
        <pre><code class="language-bash">echo "hello"</code></pre>
      `;

      highlightAllCodeBlocks(container);

      const codeEl = container.querySelector('code');
      expect(codeEl?.innerHTML).toContain('class="token');
      expect(codeEl?.classList.contains('prism-highlighted')).toBe(true);
    });

    it('should highlight multiple code blocks', () => {
      container.innerHTML = `
        <pre><code class="language-bash">ls -la</code></pre>
        <pre><code class="language-rust">fn main() {}</code></pre>
      `;

      highlightAllCodeBlocks(container);

      const codeEls = container.querySelectorAll('code');
      codeEls.forEach((el) => {
        expect(el.classList.contains('prism-highlighted')).toBe(true);
      });
    });

    it('should not re-highlight already highlighted blocks', () => {
      container.innerHTML = `
        <pre><code class="language-bash prism-highlighted">echo "hello"</code></pre>
      `;

      const originalHtml = container.querySelector('code')?.innerHTML;
      highlightAllCodeBlocks(container);

      expect(container.querySelector('code')?.innerHTML).toBe(originalHtml);
    });

    it('should handle code blocks without language class', () => {
      container.innerHTML = `
        <pre><code>plain code</code></pre>
      `;

      highlightAllCodeBlocks(container);

      const codeEl = container.querySelector('code');
      expect(codeEl?.classList.contains('prism-highlighted')).toBe(false);
    });
  });

  describe('createHighlightedCodeElement', () => {
    it('should create a code element with highlighted content', () => {
      const el = createHighlightedCodeElement('cargo install mirdb', 'bash');

      expect(el.tagName).toBe('CODE');
      expect(el.className).toContain('language-bash');
      expect(el.className).toContain('prism-highlighted');
      expect(el.innerHTML).toContain('class="token');
    });

    it('should create element for rust code', () => {
      const el = createHighlightedCodeElement('let x = 5;', 'rust');

      expect(el.className).toContain('language-rust');
      expect(el.innerHTML).toContain('class="token');
    });
  });

  describe('initHighlighting', () => {
    let container: HTMLElement;

    beforeEach(() => {
      container = document.createElement('div');
      container.id = 'test-container';
      document.body.appendChild(container);
    });

    afterEach(() => {
      document.body.removeChild(container);
    });

    it('should highlight existing code blocks on init', () => {
      container.innerHTML = `
        <pre><code class="language-bash">echo "test"</code></pre>
      `;

      initHighlighting();

      const codeEl = container.querySelector('code');
      expect(codeEl?.classList.contains('prism-highlighted')).toBe(true);
    });
  });
});
