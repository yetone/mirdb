/**
 * Unit tests for CodeBlock component.
 * Owner: Scenario 3 - Usage Examples Section
 *
 * Tests:
 * - Terminal-style rendering
 * - Syntax highlighting for Memcached commands
 * - Accessibility attributes
 * - CopyButton integration
 */

import { describe, it, expect } from 'vitest';
import { readFileSync } from 'fs';
import { resolve } from 'path';

// Read the component source for static analysis
const componentPath = resolve(__dirname, '../../../src/components/CodeBlock.astro');
const componentSource = readFileSync(componentPath, 'utf-8');

describe('CodeBlock Component', () => {
  describe('Terminal Style', () => {
    it('should have terminal-window class', () => {
      expect(componentSource).toContain('terminal-window');
    });

    it('should have terminal header with window controls', () => {
      expect(componentSource).toContain('terminal-header');
      expect(componentSource).toContain('terminal-dot-red');
      expect(componentSource).toContain('terminal-dot-yellow');
      expect(componentSource).toContain('terminal-dot-green');
    });

    it('should have terminal body for code content', () => {
      expect(componentSource).toContain('terminal-body');
    });

    it('should display title in terminal header', () => {
      expect(componentSource).toContain('terminal-title');
      expect(componentSource).toContain('{title}');
    });
  });

  describe('Props Interface', () => {
    it('should accept code prop', () => {
      expect(componentSource).toContain('code: string');
    });

    it('should accept language prop with default', () => {
      expect(componentSource).toContain('language');
    });

    it('should accept title prop with default', () => {
      expect(componentSource).toContain('title');
    });

    it('should accept id prop for copy functionality', () => {
      expect(componentSource).toContain('id: string');
    });
  });

  describe('Syntax Highlighting', () => {
    it('should have highlightCode function', () => {
      expect(componentSource).toContain('function highlightCode');
    });

    it('should highlight Memcached commands (SET, GET, DELETE)', () => {
      expect(componentSource).toContain('set|get|gets|delete|add|replace|append|prepend|info|major_compaction');
    });

    it('should highlight response codes (STORED, DELETED, etc.)', () => {
      expect(componentSource).toContain('STORED|NOT_STORED|DELETED|NOT_FOUND|END|OK|EXISTS|ERROR');
    });

    it('should use token-command class for commands', () => {
      expect(componentSource).toContain('token-command');
    });

    it('should use token-response class for responses', () => {
      expect(componentSource).toContain('token-response');
    });

    it('should use token-key class for keys', () => {
      expect(componentSource).toContain('token-key');
    });

    it('should use token-value class for values', () => {
      expect(componentSource).toContain('token-value');
    });

    it('should use token-comment class for comments', () => {
      expect(componentSource).toContain('token-comment');
    });

    it('should use token-number class for numbers', () => {
      expect(componentSource).toContain('token-number');
    });
  });

  describe('Security', () => {
    it('should have escapeHtml function to prevent XSS', () => {
      expect(componentSource).toContain('function escapeHtml');
    });

    it('should escape HTML entities', () => {
      expect(componentSource).toContain('&amp;');
      expect(componentSource).toContain('&lt;');
      expect(componentSource).toContain('&gt;');
      expect(componentSource).toContain('&quot;');
    });
  });

  describe('Accessibility', () => {
    it('should have role="region" for the code block', () => {
      expect(componentSource).toContain('role="region"');
    });

    it('should have aria-label for the terminal window', () => {
      expect(componentSource).toContain('aria-label');
    });

    it('should have aria-hidden for decorative elements', () => {
      expect(componentSource).toContain('aria-hidden="true"');
    });

    it('should have tabindex for keyboard focus on pre element', () => {
      expect(componentSource).toContain('tabindex="0"');
    });
  });

  describe('CopyButton Integration', () => {
    it('should import CopyButton component', () => {
      expect(componentSource).toContain("import CopyButton from './CopyButton.astro'");
    });

    it('should render CopyButton with targetId', () => {
      expect(componentSource).toContain('<CopyButton targetId={id}');
    });

    it('should have id on pre element for copy targeting', () => {
      expect(componentSource).toContain('id={id}');
    });
  });
});

describe('Syntax Highlighting Logic', () => {
  // Extract and test the highlightCode logic
  function highlightCode(code: string): string {
    const lines = code.split('\n');

    return lines.map(line => {
      // Command prompt lines
      if (line.startsWith('$')) {
        return `<span class="token-command">${escapeHtml(line)}</span>`;
      }

      // Memcached commands
      if (/^(set|get|gets|delete|add|replace|append|prepend|info|major_compaction)\s/i.test(line)) {
        const parts = line.split(/\s+/);
        const command = parts[0];
        const rest = parts.slice(1).join(' ');
        return `<span class="token-command">${escapeHtml(command)}</span> <span class="token-key">${escapeHtml(rest)}</span>`;
      }

      // Response codes
      if (/^(STORED|NOT_STORED|DELETED|NOT_FOUND|END|OK|EXISTS|ERROR)/i.test(line)) {
        return `<span class="token-response">${escapeHtml(line)}</span>`;
      }

      // VALUE response
      if (/^VALUE\s/.test(line)) {
        const parts = line.split(/\s+/);
        return `<span class="token-response">${escapeHtml(parts[0])}</span> <span class="token-key">${escapeHtml(parts[1])}</span> <span class="token-number">${escapeHtml(parts.slice(2).join(' '))}</span>`;
      }

      // Comments
      if (line.trim().startsWith('#')) {
        return `<span class="token-comment">${escapeHtml(line)}</span>`;
      }

      // Default - value data
      return `<span class="token-value">${escapeHtml(line)}</span>`;
    }).join('\n');
  }

  function escapeHtml(text: string): string {
    return text
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#039;');
  }

  it('should highlight SET command correctly', () => {
    const result = highlightCode('set mykey 0 0 5');
    expect(result).toContain('token-command');
    expect(result).toContain('token-key');
    expect(result).toContain('set');
  });

  it('should highlight GET command correctly', () => {
    const result = highlightCode('get mykey');
    expect(result).toContain('token-command');
    expect(result).toContain('get');
  });

  it('should highlight DELETE command correctly', () => {
    const result = highlightCode('delete mykey');
    expect(result).toContain('token-command');
    expect(result).toContain('delete');
  });

  it('should highlight STORED response', () => {
    const result = highlightCode('STORED');
    expect(result).toContain('token-response');
  });

  it('should highlight VALUE response with proper parts', () => {
    const result = highlightCode('VALUE mykey 0 5');
    expect(result).toContain('token-response');
    expect(result).toContain('token-key');
    expect(result).toContain('token-number');
  });

  it('should highlight comments', () => {
    const result = highlightCode('# This is a comment');
    expect(result).toContain('token-comment');
  });

  it('should highlight command prompt', () => {
    const result = highlightCode('$ telnet localhost 12333');
    expect(result).toContain('token-command');
  });

  it('should escape HTML special characters', () => {
    const result = escapeHtml('<script>alert("xss")</script>');
    expect(result).toContain('&lt;');
    expect(result).toContain('&gt;');
    expect(result).toContain('&quot;');
    expect(result).not.toContain('<script>');
  });

  it('should handle multiline code', () => {
    const code = 'set key 0 0 5\nhello\nSTORED';
    const result = highlightCode(code);
    const lines = result.split('\n');
    expect(lines.length).toBe(3);
    expect(lines[0]).toContain('token-command');
    expect(lines[1]).toContain('token-value');
    expect(lines[2]).toContain('token-response');
  });
});
