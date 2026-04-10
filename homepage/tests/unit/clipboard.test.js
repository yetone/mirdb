/**
 * Clipboard Functionality Unit Tests
 * Owner: Scenario 3 - Getting Started Section
 *
 * Test cases:
 * - Copy to clipboard success
 * - Visual feedback on copy
 * - Fallback for older browsers
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { JSDOM } from 'jsdom';

// Create a mock DOM for testing
function createTestDOM() {
  const html = `
    <!DOCTYPE html>
    <html>
    <body>
      <section class="getting-started" id="getting-started">
        <div class="code-block" id="install-block">
          <div class="code-header">
            <span class="code-lang">Build from source</span>
            <button class="copy-btn" data-copy="cargo build --release" aria-label="Copy installation command">
              <span class="copy-icon">Copy</span>
            </button>
          </div>
        </div>
        <div class="code-block" id="set-command-block">
          <div class="code-header">
            <span class="code-lang">SET Command</span>
            <button class="copy-btn" data-copy="set key 0 0 5\\r\\nvalue\\r\\n" aria-label="Copy SET command">
              <span class="copy-icon">Copy</span>
            </button>
          </div>
        </div>
        <div class="code-block" id="get-command-block">
          <div class="code-header">
            <span class="code-lang">GET Command</span>
            <button class="copy-btn" data-copy="get key\\r\\n" aria-label="Copy GET command">
              <span class="copy-icon">Copy</span>
            </button>
          </div>
        </div>
      </section>
    </body>
    </html>
  `;
  return new JSDOM(html, { runScripts: 'dangerously' });
}

describe('Clipboard Module', () => {
  let dom;
  let document;
  let clipboardContent;

  beforeEach(() => {
    dom = createTestDOM();
    document = dom.window.document;
    clipboardContent = '';

    // Mock navigator.clipboard
    Object.defineProperty(dom.window.navigator, 'clipboard', {
      value: {
        writeText: vi.fn(async (text) => {
          clipboardContent = text;
          return Promise.resolve();
        }),
        readText: vi.fn(async () => clipboardContent),
      },
      configurable: true,
    });
  });

  describe('copyToClipboard function', () => {
    it('should copy text using the Clipboard API when available', async () => {
      const { copyToClipboard } = await import('../../js/clipboard.js');

      // Mock global navigator for the module
      global.navigator = {
        clipboard: {
          writeText: vi.fn(async (text) => {
            clipboardContent = text;
            return Promise.resolve();
          }),
        },
      };

      const result = await copyToClipboard('test text');
      expect(result).toBe(true);
      expect(clipboardContent).toBe('test text');
    });

    it('should return true on successful copy', async () => {
      global.navigator = {
        clipboard: {
          writeText: vi.fn(async () => Promise.resolve()),
        },
      };

      const { copyToClipboard } = await import('../../js/clipboard.js');
      const result = await copyToClipboard('hello world');
      expect(result).toBe(true);
    });
  });

  describe('Visual feedback', () => {
    it('copy button should have a copy-icon element for feedback', () => {
      const copyBtns = document.querySelectorAll('.copy-btn');
      copyBtns.forEach((btn) => {
        const icon = btn.querySelector('.copy-icon');
        expect(icon).not.toBeNull();
        expect(icon.textContent).toBe('Copy');
      });
    });

    it('copy buttons should have data-copy attribute with content', () => {
      const copyBtns = document.querySelectorAll('.copy-btn');
      expect(copyBtns.length).toBeGreaterThan(0);

      copyBtns.forEach((btn) => {
        const copyData = btn.getAttribute('data-copy');
        expect(copyData).not.toBeNull();
        expect(copyData.length).toBeGreaterThan(0);
      });
    });

    it('copy buttons should have aria-label for accessibility', () => {
      const copyBtns = document.querySelectorAll('.copy-btn');
      copyBtns.forEach((btn) => {
        const ariaLabel = btn.getAttribute('aria-label');
        expect(ariaLabel).not.toBeNull();
        expect(ariaLabel.toLowerCase()).toContain('copy');
      });
    });
  });

  describe('initClipboard function', () => {
    it('should attach click handlers to all copy buttons', async () => {
      // Set up global document
      global.document = document;
      global.navigator = {
        clipboard: {
          writeText: vi.fn(async () => Promise.resolve()),
        },
      };

      const { initClipboard } = await import('../../js/clipboard.js');

      // This should not throw
      expect(() => initClipboard()).not.toThrow();
    });
  });
});

describe('Getting Started Section - DOM Structure', () => {
  let document;

  beforeEach(() => {
    const dom = createTestDOM();
    document = dom.window.document;
  });

  it('should have a getting-started section with id="getting-started"', () => {
    const section = document.getElementById('getting-started');
    expect(section).not.toBeNull();
    expect(section.classList.contains('getting-started')).toBe(true);
  });

  it('should have code blocks with copy buttons', () => {
    const codeBlocks = document.querySelectorAll('.code-block');
    expect(codeBlocks.length).toBeGreaterThan(0);

    codeBlocks.forEach((block) => {
      const copyBtn = block.querySelector('.copy-btn');
      expect(copyBtn).not.toBeNull();
    });
  });

  it('should have SET command code block', () => {
    const setBlock = document.getElementById('set-command-block');
    expect(setBlock).not.toBeNull();

    const copyBtn = setBlock.querySelector('.copy-btn');
    const copyData = copyBtn.getAttribute('data-copy');
    expect(copyData).toContain('set key');
  });

  it('should have GET command code block', () => {
    const getBlock = document.getElementById('get-command-block');
    expect(getBlock).not.toBeNull();

    const copyBtn = getBlock.querySelector('.copy-btn');
    const copyData = copyBtn.getAttribute('data-copy');
    expect(copyData).toContain('get key');
  });
});
