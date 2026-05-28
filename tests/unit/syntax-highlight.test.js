/**
 * Unit Tests: Syntax Highlighting Module
 * Owner: Scenario 12 - Visual Design and Theming
 *
 * Expected test coverage:
 * - highlightAll finds and highlights all code blocks
 * - highlightElement wraps tokens in span elements with correct classes
 * - Handles memcached protocol syntax
 * - Handles TOML configuration syntax
 */

import { describe, it, expect, beforeEach } from 'vitest';

function loadModule() {
  const code = require('fs').readFileSync('src/scripts/syntax-highlight.js', 'utf8');

  // Create a minimal module-like environment
  const fakeModule = { exports: {} };
  const fakeWindow = {
    addEventListener: () => {},
    SyntaxHighlight: undefined,
  };

  // Use vm to run the script
  const vm = require('vm');
  const context = vm.createContext({
    module: fakeModule,
    exports: fakeModule.exports,
    window: fakeWindow,
    document: {
      addEventListener: () => {},
      querySelectorAll: () => [],
    },
  });

  vm.runInContext(code, context);

  // The IIFE sets module.exports when module is available
  return context.module.exports || context.exports || context.window.SyntaxHighlight;
}

describe('Syntax Highlighting', () => {
  let SyntaxHighlight;

  beforeEach(() => {
    SyntaxHighlight = loadModule();
  });

  describe('escapeHtml', () => {
    it('escapes HTML special characters', () => {
      expect(SyntaxHighlight.escapeHtml('<div>test</div>')).toBe('&lt;div&gt;test&lt;/div&gt;');
      expect(SyntaxHighlight.escapeHtml('a & b')).toBe('a &amp; b');
      expect(SyntaxHighlight.escapeHtml('foo > bar')).toBe('foo &gt; bar');
    });

    it('returns plain text unchanged', () => {
      expect(SyntaxHighlight.escapeHtml('hello world')).toBe('hello world');
    });
  });

  describe('detectLanguage', () => {
    it('detects bash from class name', () => {
      expect(SyntaxHighlight.detectLanguage('', 'language-bash')).toBe('bash');
      expect(SyntaxHighlight.detectLanguage('', 'language-sh')).toBe('bash');
    });

    it('detects toml from class name', () => {
      expect(SyntaxHighlight.detectLanguage('', 'language-toml')).toBe('toml');
    });

    it('detects toml from content', () => {
      expect(SyntaxHighlight.detectLanguage('key = "value"\n[section]', '')).toBe('toml');
    });

    it('detects memcached from content', () => {
      expect(SyntaxHighlight.detectLanguage('set key 0 900 5\nhello', '')).toBe('memcached');
    });

    it('defaults to bash', () => {
      expect(SyntaxHighlight.detectLanguage('echo hello', '')).toBe('bash');
    });
  });

  describe('highlightElement', () => {
    it('wraps bash keywords in spans', () => {
      const el = { textContent: 'if [ -d "dir" ]; then echo hello; fi', className: '', dataset: {} };
      SyntaxHighlight.highlightElement(el);

      expect(el.dataset.highlighted).toBe('true');
      expect(el.innerHTML).toContain('syntax-keyword');
    });

    it('wraps bash strings in spans', () => {
      const el = { textContent: 'echo "hello world"', className: '', dataset: {} };
      SyntaxHighlight.highlightElement(el);

      expect(el.innerHTML).toContain('syntax-string');
    });

    it('wraps bash comments in spans', () => {
      const el = { textContent: '# This is a comment\necho hello', className: '', dataset: {} };
      SyntaxHighlight.highlightElement(el);

      expect(el.innerHTML).toContain('syntax-comment');
    });

    it('wraps bash functions/commands in spans', () => {
      const el = { textContent: 'cargo build --release', className: '', dataset: {} };
      SyntaxHighlight.highlightElement(el);

      expect(el.innerHTML).toContain('syntax-function');
    });

    it('handles TOML syntax', () => {
      const el = {
        textContent: 'addr = "0.0.0.0:12333"\n# comment\nmax_level = 7',
        className: 'language-toml',
        dataset: {},
      };
      SyntaxHighlight.highlightElement(el);

      expect(el.innerHTML).toContain('syntax-string');
      expect(el.innerHTML).toContain('syntax-comment');
      expect(el.innerHTML).toContain('syntax-number');
    });

    it('handles memcached protocol syntax', () => {
      const el = {
        textContent: 'set mykey 0 900 5\nhello',
        className: 'language-memcached',
        dataset: {},
      };
      SyntaxHighlight.highlightElement(el);

      expect(el.innerHTML).toContain('syntax-keyword');
    });

    it('does not re-highlight already highlighted blocks', () => {
      const el = { textContent: 'echo test', className: '', dataset: { highlighted: 'true' }, innerHTML: 'echo test' };
      const originalHtml = el.innerHTML;

      SyntaxHighlight.highlightElement(el);
      expect(el.innerHTML).toBe(originalHtml);
    });

    it('handles empty code blocks', () => {
      const el = { textContent: '', className: '', dataset: {} };
      SyntaxHighlight.highlightElement(el);
      expect(el.dataset.highlighted).toBe('true');
    });

    it('returns early for null element', () => {
      expect(() => SyntaxHighlight.highlightElement(null)).not.toThrow();
    });
  });

  describe('highlightAll', () => {
    it('finds and highlights all pre>code blocks', () => {
      const codeBlocks = [];
      const fakeDocument = {
        querySelectorAll: (selector) => {
          if (selector === 'pre code') {
            const c1 = { textContent: 'echo "hello"', className: '', dataset: {} };
            const c2 = { textContent: 'git clone repo', className: '', dataset: {} };
            codeBlocks.push(c1, c2);
            return [c1, c2];
          }
          return [];
        },
      };

      // Re-run the module with our fake document
      const code = require('fs').readFileSync('src/scripts/syntax-highlight.js', 'utf8');
      const vm = require('vm');
      const context = vm.createContext({
        module: { exports: {} },
        exports: {},
        window: { addEventListener: () => {} },
        document: fakeDocument,
      });
      vm.runInContext(code, context);
      const SH = context.module.exports;

      SH.highlightAll();

      expect(codeBlocks[0].dataset.highlighted).toBe('true');
      expect(codeBlocks[1].dataset.highlighted).toBe('true');
    });

    it('ignores code blocks without pre parent when using querySelectorAll', () => {
      const captured = [];
      const fakeDocument = {
        querySelectorAll: (selector) => {
          if (selector === 'pre code') {
            // Only returns pre>code blocks
            return [];
          }
          return [];
        },
      };

      const code = require('fs').readFileSync('src/scripts/syntax-highlight.js', 'utf8');
      const vm = require('vm');
      const context = vm.createContext({
        module: { exports: {} },
        exports: {},
        window: { addEventListener: () => {} },
        document: fakeDocument,
      });
      vm.runInContext(code, context);
      const SH = context.module.exports;

      // highlightAll with no matching blocks should not throw
      expect(() => SH.highlightAll()).not.toThrow();
    });
  });
});
