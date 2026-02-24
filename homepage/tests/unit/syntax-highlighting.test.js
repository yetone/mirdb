/**
 * Syntax Highlighting Unit Tests
 * Owner: Scenario 15 - Code Syntax Highlighting
 *
 * Test cases:
 * - Code blocks have syntax highlighting classes applied
 * - Rust keywords (fn, let, mut) are highlighted
 * - Shell/bash syntax is highlighted appropriately
 * - Syntax highlighting tokens have proper styling
 */

import { describe, test, expect, beforeAll } from 'vitest';
import { readFileSync } from 'fs';
import { join } from 'path';
import { renderHTML } from '../setup/test-utils.js';

describe('Syntax Highlighting', () => {
  let document;

  beforeAll(() => {
    // Load the actual index.html from filesystem
    const htmlPath = join(process.cwd(), 'index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    document = renderHTML(html);
  });

  describe('Code Block Structure', () => {
    test('TC1: Code blocks have syntax highlighting classes applied', () => {
      const codeBlocks = document.querySelectorAll('.code-block');
      expect(codeBlocks.length).toBeGreaterThan(0);

      // Each code block should have data-language attribute
      codeBlocks.forEach(block => {
        expect(block.hasAttribute('data-language')).toBe(true);
      });

      // Code blocks should contain code elements with language classes
      const bashCode = document.querySelector('code.language-bash');
      expect(bashCode).not.toBeNull();

      const rustCode = document.querySelector('code.language-rust');
      expect(rustCode).not.toBeNull();
    });

    test('Code blocks have proper header with language indicator', () => {
      const codeBlockHeaders = document.querySelectorAll('.code-block-header');
      expect(codeBlockHeaders.length).toBeGreaterThan(0);

      // Each header should have language indicator
      codeBlockHeaders.forEach(header => {
        const langIndicator = header.querySelector('.code-block-lang');
        expect(langIndicator).not.toBeNull();
        expect(langIndicator.textContent.trim()).toBeTruthy();
      });
    });
  });

  describe('Rust Syntax Highlighting', () => {
    test('TC2: Rust keywords (fn, let, mut) are highlighted', () => {
      const rustCode = document.querySelector('code.language-rust');
      expect(rustCode).not.toBeNull();

      // Check for keyword tokens
      const keywordTokens = rustCode.querySelectorAll('.token.keyword');
      expect(keywordTokens.length).toBeGreaterThan(0);

      // Verify specific Rust keywords are present
      const keywordTexts = Array.from(keywordTokens).map(t => t.textContent);
      expect(keywordTexts).toContain('fn');
      expect(keywordTexts).toContain('let');
      expect(keywordTexts).toContain('mut');
      expect(keywordTexts).toContain('use');
    });

    test('Rust function names are highlighted', () => {
      const rustCode = document.querySelector('code.language-rust');
      const functionTokens = rustCode.querySelectorAll('.token.function');
      expect(functionTokens.length).toBeGreaterThan(0);

      const functionNames = Array.from(functionTokens).map(t => t.textContent);
      expect(functionNames).toContain('main');
    });

    test('Rust strings are highlighted', () => {
      const rustCode = document.querySelector('code.language-rust');
      const stringTokens = rustCode.querySelectorAll('.token.string');
      expect(stringTokens.length).toBeGreaterThan(0);

      // Verify string content
      const hasConnectionString = Array.from(stringTokens).some(t =>
        t.textContent.includes('memcache://127.0.0.1:12333')
      );
      expect(hasConnectionString).toBe(true);
    });

    test('Rust types are highlighted', () => {
      const rustCode = document.querySelector('code.language-rust');
      const typeTokens = rustCode.querySelectorAll('.token.type');
      expect(typeTokens.length).toBeGreaterThan(0);

      const typeNames = Array.from(typeTokens).map(t => t.textContent);
      expect(typeNames).toContain('Option');
      expect(typeNames).toContain('String');
    });

    test('Rust macros are highlighted', () => {
      const rustCode = document.querySelector('code.language-rust');
      const macroTokens = rustCode.querySelectorAll('.token.macro');
      expect(macroTokens.length).toBeGreaterThan(0);

      const macroNames = Array.from(macroTokens).map(t => t.textContent);
      expect(macroNames).toContain('println!');
    });

    test('Rust comments are highlighted', () => {
      const rustCode = document.querySelector('code.language-rust');
      const commentTokens = rustCode.querySelectorAll('.token.comment');
      expect(commentTokens.length).toBeGreaterThan(0);

      // Verify comment content
      const hasComment = Array.from(commentTokens).some(t =>
        t.textContent.includes('Connect to MirDB')
      );
      expect(hasComment).toBe(true);
    });

    test('Rust punctuation is highlighted', () => {
      const rustCode = document.querySelector('code.language-rust');
      const punctuationTokens = rustCode.querySelectorAll('.token.punctuation');
      expect(punctuationTokens.length).toBeGreaterThan(0);
    });

    test('Rust operators are highlighted', () => {
      const rustCode = document.querySelector('code.language-rust');
      const operatorTokens = rustCode.querySelectorAll('.token.operator');
      expect(operatorTokens.length).toBeGreaterThan(0);
    });

    test('Rust numbers are highlighted', () => {
      const rustCode = document.querySelector('code.language-rust');
      const numberTokens = rustCode.querySelectorAll('.token.number');
      expect(numberTokens.length).toBeGreaterThan(0);

      const numberValues = Array.from(numberTokens).map(t => t.textContent);
      expect(numberValues).toContain('0');
    });

    test('Rust variables are highlighted', () => {
      const rustCode = document.querySelector('code.language-rust');
      const variableTokens = rustCode.querySelectorAll('.token.variable');
      expect(variableTokens.length).toBeGreaterThan(0);

      const variableNames = Array.from(variableTokens).map(t => t.textContent);
      expect(variableNames).toContain('client');
      expect(variableNames).toContain('result');
    });
  });

  describe('Shell/Bash Syntax Highlighting', () => {
    test('TC3: Shell/bash syntax is highlighted appropriately', () => {
      const bashCode = document.querySelector('code.language-bash');
      expect(bashCode).not.toBeNull();

      // Check for comment tokens (# comments)
      const commentTokens = bashCode.querySelectorAll('.token.comment');
      expect(commentTokens.length).toBeGreaterThan(0);

      // Verify shell comments start with #
      const hasShellComment = Array.from(commentTokens).some(t =>
        t.textContent.includes('#')
      );
      expect(hasShellComment).toBe(true);
    });

    test('Shell commands are highlighted', () => {
      const bashCodes = document.querySelectorAll('code.language-bash');
      expect(bashCodes.length).toBeGreaterThan(0);

      // Check first bash code block
      const bashCode = bashCodes[0];
      const commandTokens = bashCode.querySelectorAll('.token.command');
      expect(commandTokens.length).toBeGreaterThan(0);

      const commandNames = Array.from(commandTokens).map(t => t.textContent);
      expect(commandNames).toContain('cargo');
    });

    test('Shell/bash comments have # prefix', () => {
      const bashCodes = document.querySelectorAll('code.language-bash');

      bashCodes.forEach(bashCode => {
        const commentTokens = bashCode.querySelectorAll('.token.comment');
        commentTokens.forEach(comment => {
          expect(comment.textContent.trim().startsWith('#')).toBe(true);
        });
      });
    });

    test('Install command is properly highlighted', () => {
      const installCode = document.querySelector('#install-code');
      expect(installCode).not.toBeNull();

      // Should contain cargo command
      const commandTokens = installCode.querySelectorAll('.token.command');
      const commandNames = Array.from(commandTokens).map(t => t.textContent);
      expect(commandNames).toContain('cargo');
    });

    test('Usage example memcached commands are highlighted', () => {
      const usageCode = document.querySelector('#usage-code');
      expect(usageCode).not.toBeNull();

      // Should contain mirdb, set, get commands
      const commandTokens = usageCode.querySelectorAll('.token.command');
      const commandNames = Array.from(commandTokens).map(t => t.textContent);
      expect(commandNames).toContain('mirdb');
      expect(commandNames).toContain('set');
      expect(commandNames).toContain('get');
    });
  });

  describe('Token Classes', () => {
    test('All token elements have token class', () => {
      const allTokens = document.querySelectorAll('[class*="token"]');
      expect(allTokens.length).toBeGreaterThan(0);

      allTokens.forEach(token => {
        expect(token.classList.contains('token')).toBe(true);
      });
    });

    test('Token elements have specific type class', () => {
      const validTokenTypes = [
        'comment', 'keyword', 'function', 'string', 'number',
        'operator', 'punctuation', 'type', 'variable', 'macro',
        'command', 'attribute', 'module', 'namespace', 'lifetime'
      ];

      const allTokens = document.querySelectorAll('[class*="token"]');
      allTokens.forEach(token => {
        const classes = Array.from(token.classList);
        const hasTypeClass = classes.some(cls =>
          cls !== 'token' && validTokenTypes.includes(cls)
        );
        expect(hasTypeClass).toBe(true);
      });
    });
  });
});
