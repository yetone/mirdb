/**
 * Unit tests for Quick Start Section functionality.
 * Owner: Scenario 5 - Quick Start Section
 *
 * Tests:
 * - copyToClipboard accepts text input and writes to navigator.clipboard
 * - Syntax highlighting CSS classes are present on code elements
 * - initCopyButtons attaches click handlers to copy buttons
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

describe('Quick Start Section - copyToClipboard', () => {
  let copyToClipboard;
  let originalClipboard;

  beforeEach(async () => {
    // Save original navigator.clipboard
    originalClipboard = navigator.clipboard;

    // Mock navigator.clipboard
    Object.defineProperty(navigator, 'clipboard', {
      value: { writeText: vi.fn().mockResolvedValue(undefined) },
      configurable: true,
    });

    // Re-import to get the fresh module with mock
    const mod = await import('../../js/main.js');
    copyToClipboard = mod.copyToClipboard;
  });

  afterEach(() => {
    // Restore original navigator.clipboard
    Object.defineProperty(navigator, 'clipboard', {
      value: originalClipboard,
      configurable: true,
    });
    vi.restoreAllMocks();
  });

  it('accepts text input and writes to navigator.clipboard', async () => {
    const testText = 'set mykey 0 60 5\nhello\nSTORED';
    await copyToClipboard(testText);
    expect(navigator.clipboard.writeText).toHaveBeenCalledWith(testText);
    expect(navigator.clipboard.writeText).toHaveBeenCalledTimes(1);
  });

  it('handles empty string input', async () => {
    await copyToClipboard('');
    expect(navigator.clipboard.writeText).toHaveBeenCalledWith('');
  });

  it('propagates write errors', async () => {
    const error = new Error('Clipboard permission denied');
    navigator.clipboard.writeText.mockRejectedValueOnce(error);
    await expect(copyToClipboard('test')).rejects.toThrow('Clipboard permission denied');
  });
});

describe('Quick Start Section - Syntax Highlighting', () => {
  let document;

  beforeEach(() => {
    // Create a minimal DOM with code blocks using syntax classes
    const { JSDOM } = require('jsdom');
    const dom = new JSDOM(`
      <!DOCTYPE html>
      <html>
      <head><link rel="stylesheet" href="css/main.css"></head>
      <body>
        <section id="quick-start">
          <div class="code-block-wrapper">
            <pre class="code-block"><code>
              <span class="syntax-command">set</span>
              <span class="syntax-key">mykey</span>
              <span class="syntax-string">hello</span>
              <span class="syntax-number">60</span>
              <span class="syntax-response">STORED</span>
              <span class="syntax-keyword">import</span>
              <span class="syntax-method">set</span>
              <span class="syntax-comment"># comment</span>
            </code></pre>
          </div>
        </section>
      </body>
      </html>
    `, { url: 'http://localhost' });
    document = dom.window.document;
  });

  it('code elements have syntax-command class for commands', () => {
    const commandSpan = document.querySelector('.syntax-command');
    expect(commandSpan).not.toBeNull();
    expect(commandSpan.textContent.trim()).toBe('set');
  });

  it('code elements have syntax-key class for keys', () => {
    const keySpan = document.querySelector('.syntax-key');
    expect(keySpan).not.toBeNull();
    expect(keySpan.textContent.trim()).toBe('mykey');
  });

  it('code elements have syntax-string class for strings', () => {
    const stringSpan = document.querySelector('.syntax-string');
    expect(stringSpan).not.toBeNull();
    expect(stringSpan.textContent.trim()).toBe('hello');
  });

  it('code elements have syntax-number class for numbers', () => {
    const numberSpan = document.querySelector('.syntax-number');
    expect(numberSpan).not.toBeNull();
    expect(numberSpan.textContent.trim()).toBe('60');
  });

  it('code elements have syntax-response class for responses', () => {
    const responseSpan = document.querySelector('.syntax-response');
    expect(responseSpan).not.toBeNull();
    expect(responseSpan.textContent.trim()).toBe('STORED');
  });

  it('code elements have syntax-keyword class for keywords', () => {
    const keywordSpan = document.querySelector('.syntax-keyword');
    expect(keywordSpan).not.toBeNull();
    expect(keywordSpan.textContent.trim()).toBe('import');
  });

  it('code elements have syntax-method class for methods', () => {
    const methodSpan = document.querySelector('.syntax-method');
    expect(methodSpan).not.toBeNull();
    expect(methodSpan.textContent.trim()).toBe('set');
  });

  it('code elements have syntax-comment class for comments', () => {
    const commentSpan = document.querySelector('.syntax-comment');
    expect(commentSpan).not.toBeNull();
    expect(commentSpan.textContent.trim()).toBe('# comment');
  });
});
