/**
 * Syntax Highlighter Utility Unit Tests.
 * Owner: Scenario 3 - Usage Examples with Syntax Highlighting
 *
 * Tests:
 * - tokenizeMemcached correctly identifies tokens
 * - highlightCode returns HTML with syntax highlighting
 * - createCodeBlock creates proper DOM structure
 * - Commands, keys, and values have distinct colors/classes
 */

import { describe, it, expect } from 'vitest';
import {
  tokenizeMemcached,
  highlightCode,
  createCodeBlock,
  type HighlightToken,
} from '@/utils/syntax-highlighter';

describe('Syntax Highlighter Utility', () => {
  describe('tokenizeMemcached', () => {
    it('identifies set command as command token', () => {
      const tokens = tokenizeMemcached('set key 0 0 5');
      const commandToken = tokens.find(t => t.type === 'command' && t.value === 'set');
      expect(commandToken).not.toBeUndefined();
    });

    it('identifies get command as command token', () => {
      const tokens = tokenizeMemcached('get key');
      const commandToken = tokens.find(t => t.type === 'command' && t.value === 'get');
      expect(commandToken).not.toBeUndefined();
    });

    it('identifies delete command as command token', () => {
      const tokens = tokenizeMemcached('delete key');
      const commandToken = tokens.find(t => t.type === 'command' && t.value === 'delete');
      expect(commandToken).not.toBeUndefined();
    });

    it('identifies key as key token', () => {
      const tokens = tokenizeMemcached('get mykey');
      const keyToken = tokens.find(t => t.type === 'key' && t.value === 'mykey');
      expect(keyToken).not.toBeUndefined();
    });

    it('identifies numbers as number tokens', () => {
      const tokens = tokenizeMemcached('set key 0 0 5');
      const numberTokens = tokens.filter(t => t.type === 'number');
      expect(numberTokens.length).toBeGreaterThanOrEqual(3);
      expect(numberTokens.map(t => t.value)).toContain('0');
      expect(numberTokens.map(t => t.value)).toContain('5');
    });

    it('identifies response keywords as response tokens', () => {
      const tokens = tokenizeMemcached('STORED');
      const responseToken = tokens.find(t => t.type === 'response' && t.value === 'STORED');
      expect(responseToken).not.toBeUndefined();
    });

    it('identifies VALUE response keyword', () => {
      const tokens = tokenizeMemcached('VALUE key 0 5');
      const responseToken = tokens.find(t => t.type === 'response' && t.value === 'VALUE');
      expect(responseToken).not.toBeUndefined();
    });

    it('identifies END response keyword', () => {
      const tokens = tokenizeMemcached('END');
      const responseToken = tokens.find(t => t.type === 'response' && t.value === 'END');
      expect(responseToken).not.toBeUndefined();
    });

    it('identifies DELETED response keyword', () => {
      const tokens = tokenizeMemcached('DELETED');
      const responseToken = tokens.find(t => t.type === 'response' && t.value === 'DELETED');
      expect(responseToken).not.toBeUndefined();
    });

    it('handles multiline code', () => {
      const tokens = tokenizeMemcached('set key 0 0 5\nvalue');
      const newlineTokens = tokens.filter(t => t.type === 'newline');
      expect(newlineTokens.length).toBeGreaterThan(0);
    });

    it('handles carriage return newlines', () => {
      const tokens = tokenizeMemcached('set key 0 0 5\r\nvalue');
      expect(tokens.some(t => t.type === 'newline')).toBe(true);
    });
  });

  describe('highlightCode', () => {
    it('returns HTML with syntax-command class for commands', () => {
      const html = highlightCode('set key 0 0 5', 'memcached');
      expect(html).toContain('syntax-command');
      expect(html).toContain('set');
    });

    it('returns HTML with syntax-key class for keys', () => {
      const html = highlightCode('get mykey', 'memcached');
      expect(html).toContain('syntax-key');
      expect(html).toContain('mykey');
    });

    it('returns HTML with syntax-number class for numbers', () => {
      const html = highlightCode('set key 0 0 5', 'memcached');
      expect(html).toContain('syntax-number');
    });

    it('returns HTML with syntax-response class for responses', () => {
      const html = highlightCode('STORED', 'memcached');
      expect(html).toContain('syntax-response');
      expect(html).toContain('STORED');
    });

    it('escapes HTML special characters', () => {
      const html = highlightCode('set <script> 0 0 5', 'memcached');
      expect(html).not.toContain('<script>');
      expect(html).toContain('&lt;script&gt;');
    });

    it('returns plain escaped text for unknown language', () => {
      const html = highlightCode('some code', 'unknown');
      expect(html).toBe('some code');
    });

    it('code elements have distinct colors for commands, keys, and values', () => {
      const html = highlightCode('set mykey 0 0 5\nvalue', 'memcached');

      // Verify different token types are present with their classes
      expect(html).toContain('syntax-command');
      expect(html).toContain('syntax-key');
      expect(html).toContain('syntax-number');
    });
  });

  describe('createCodeBlock', () => {
    it('creates pre element with code-block class', () => {
      const block = createCodeBlock('get key', 'memcached');
      expect(block.tagName).toBe('PRE');
      expect(block.classList.contains('code-block')).toBe(true);
    });

    it('creates code element with code-content class', () => {
      const block = createCodeBlock('get key', 'memcached');
      const codeEl = block.querySelector('code');
      expect(codeEl).not.toBeNull();
      expect(codeEl?.classList.contains('code-content')).toBe(true);
    });

    it('sets data-language attribute', () => {
      const block = createCodeBlock('get key', 'memcached');
      expect(block.getAttribute('data-language')).toBe('memcached');
    });

    it('contains highlighted code in code element', () => {
      const block = createCodeBlock('get key', 'memcached');
      const codeEl = block.querySelector('code');
      expect(codeEl?.innerHTML).toContain('syntax-command');
    });

    it('code block uses monospace font family', () => {
      const block = createCodeBlock('get key', 'memcached');
      const codeEl = block.querySelector('code');
      // The code-content class applies monospace font family via CSS
      expect(codeEl?.classList.contains('code-content')).toBe(true);
    });
  });

  describe('Syntax highlighting completeness', () => {
    it('highlights complete set command example', () => {
      const code = 'set key 0 0 5\r\nvalue\r\n';
      const html = highlightCode(code, 'memcached');

      // Command
      expect(html).toMatch(/<span class="syntax-command">set<\/span>/);
      // Key
      expect(html).toMatch(/<span class="syntax-key">key<\/span>/);
      // Numbers
      expect(html).toMatch(/<span class="syntax-number">0<\/span>/);
      expect(html).toMatch(/<span class="syntax-number">5<\/span>/);
    });

    it('highlights complete get response example', () => {
      const code = 'VALUE key 0 5\r\nvalue\r\nEND';
      const html = highlightCode(code, 'memcached');

      // Response keywords
      expect(html).toMatch(/<span class="syntax-response">VALUE<\/span>/);
      expect(html).toMatch(/<span class="syntax-response">END<\/span>/);
    });
  });
});
