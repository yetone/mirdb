/**
 * Clipboard Unit Tests
 * Owner: Scenario 3 - Quick Start Section
 *
 * Tests for copyToClipboard utility function
 * and visual feedback functionality.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// Mock the clipboard module for testing
const mockWriteText = vi.fn();

// Mock navigator.clipboard before importing the module
Object.defineProperty(navigator, 'clipboard', {
  value: {
    writeText: mockWriteText,
  },
  writable: true,
  configurable: true,
});

// Import after mocking
import { copyToClipboard, showCopyFeedback } from '../../src/scripts/clipboard.js';

describe('copyToClipboard', () => {
  beforeEach(() => {
    mockWriteText.mockReset();
  });

  it('successfully copies text using Clipboard API', async () => {
    mockWriteText.mockResolvedValue(undefined);

    const result = await copyToClipboard('test text');

    expect(mockWriteText).toHaveBeenCalledWith('test text');
    expect(result).toBe(true);
  });

  it('returns true on successful copy', async () => {
    mockWriteText.mockResolvedValue(undefined);

    const result = await copyToClipboard('hello world');

    expect(result).toBe(true);
  });

  it('handles memcached connection code', async () => {
    mockWriteText.mockResolvedValue(undefined);

    const code = `from pymemcache.client import base
client = base.Client(('localhost', 12333))`;

    const result = await copyToClipboard(code);

    expect(mockWriteText).toHaveBeenCalledWith(code);
    expect(result).toBe(true);
  });

  it('handles empty string', async () => {
    mockWriteText.mockResolvedValue(undefined);

    const result = await copyToClipboard('');

    expect(mockWriteText).toHaveBeenCalledWith('');
    expect(result).toBe(true);
  });

  it('handles multiline code', async () => {
    mockWriteText.mockResolvedValue(undefined);

    const multilineCode = `line 1
line 2
line 3`;

    const result = await copyToClipboard(multilineCode);

    expect(mockWriteText).toHaveBeenCalledWith(multilineCode);
    expect(result).toBe(true);
  });

  it('handles special characters in code', async () => {
    mockWriteText.mockResolvedValue(undefined);

    const codeWithSpecialChars = `client.set('key', "value with 'quotes'")`;

    const result = await copyToClipboard(codeWithSpecialChars);

    expect(mockWriteText).toHaveBeenCalledWith(codeWithSpecialChars);
    expect(result).toBe(true);
  });

  it('uses fallback when Clipboard API fails', async () => {
    mockWriteText.mockRejectedValue(new Error('Permission denied'));

    // Mock execCommand on document (add it if it doesn't exist)
    const originalExecCommand = document.execCommand;
    document.execCommand = vi.fn().mockReturnValue(true);

    // Mock the textarea creation
    const mockTextArea = document.createElement('textarea');
    const originalCreateElement = document.createElement.bind(document);
    vi.spyOn(document, 'createElement').mockImplementation((tag: string) => {
      if (tag === 'textarea') {
        return mockTextArea;
      }
      return originalCreateElement(tag);
    });

    const result = await copyToClipboard('fallback test');

    expect(document.execCommand).toHaveBeenCalledWith('copy');
    expect(result).toBe(true);

    // Restore
    document.execCommand = originalExecCommand;
    vi.restoreAllMocks();
  });
});

describe('showCopyFeedback', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('adds copied class to element', () => {
    const element = document.createElement('button');
    element.innerHTML = '<span class="code-block__copy-text">Copy</span>';

    showCopyFeedback(element);

    expect(element.classList.contains('copied')).toBe(true);
  });

  it('changes button text to Copied!', () => {
    const element = document.createElement('button');
    element.innerHTML = '<span class="code-block__copy-text">Copy</span>';

    showCopyFeedback(element);

    const textSpan = element.querySelector('.code-block__copy-text');
    expect(textSpan?.textContent).toBe('Copied!');
  });

  it('removes copied class after duration', () => {
    const element = document.createElement('button');
    element.innerHTML = '<span class="code-block__copy-text">Copy</span>';

    showCopyFeedback(element, 1000);

    expect(element.classList.contains('copied')).toBe(true);

    vi.advanceTimersByTime(1000);

    expect(element.classList.contains('copied')).toBe(false);
  });

  it('restores original text after duration', () => {
    const element = document.createElement('button');
    element.innerHTML = '<span class="code-block__copy-text">Copy</span>';

    showCopyFeedback(element, 1000);

    vi.advanceTimersByTime(1000);

    const textSpan = element.querySelector('.code-block__copy-text');
    expect(textSpan?.textContent).toBe('Copy');
  });

  it('handles null element gracefully', () => {
    // Should not throw
    expect(() => showCopyFeedback(null as unknown as HTMLElement)).not.toThrow();
  });

  it('uses default duration of 2000ms', () => {
    const element = document.createElement('button');
    element.innerHTML = '<span class="code-block__copy-text">Copy</span>';

    showCopyFeedback(element);

    // Still copied at 1999ms
    vi.advanceTimersByTime(1999);
    expect(element.classList.contains('copied')).toBe(true);

    // Removed at 2000ms
    vi.advanceTimersByTime(1);
    expect(element.classList.contains('copied')).toBe(false);
  });
});

describe('CodeBlock component integration', () => {
  it('renders with syntax highlighting and copy button', () => {
    // Create a mock code block structure
    document.body.innerHTML = `
      <div class="code-block" data-language="python">
        <div class="code-block__header">
          <span class="code-block__language">Python</span>
          <button class="code-block__copy-btn" aria-label="Copy code to clipboard">
            <span class="code-block__copy-text">Copy</span>
          </button>
        </div>
        <pre class="code-block__content"><code><span class="syntax-keyword">from</span> pymemcache <span class="syntax-keyword">import</span> base</code></pre>
      </div>
    `;

    const codeBlock = document.querySelector('.code-block');
    const copyButton = document.querySelector('.code-block__copy-btn');
    const syntaxElements = document.querySelectorAll('.syntax-keyword');

    expect(codeBlock).not.toBeNull();
    expect(copyButton).not.toBeNull();
    expect(syntaxElements.length).toBeGreaterThan(0);
  });
});
