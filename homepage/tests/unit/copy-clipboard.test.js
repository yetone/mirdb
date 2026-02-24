/**
 * Copy to Clipboard Unit Tests
 * Owner: Scenario 4 - Quick Start Section
 *
 * Test cases:
 * - copyToClipboard copies text using Clipboard API
 * - Visual feedback is shown after copy
 * - Fallback works when Clipboard API unavailable
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { JSDOM } from 'jsdom';

// Copy clipboard module functions will be imported after DOM setup
let copyClipboardModule;

/**
 * Create a minimal Quick Start HTML structure for testing
 */
function createQuickStartHTML() {
  return `
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8">
      <title>Test</title>
    </head>
    <body>
      <section id="quickstart" class="quickstart">
        <div class="container">
          <h2>Quick Start</h2>
          <div class="code-block" data-language="bash">
            <div class="code-block-header">
              <span class="code-block-lang">bash</span>
              <button type="button" class="copy-btn" aria-label="Copy to clipboard" data-copy-target="install-code">
                <svg class="copy-icon" width="16" height="16" aria-hidden="true">
                  <rect x="9" y="9" width="13" height="13"/>
                </svg>
                <svg class="check-icon" width="16" height="16" aria-hidden="true" style="display: none;">
                  <polyline points="20 6 9 17 4 12"/>
                </svg>
                <span class="copy-btn-text">Copy</span>
              </button>
            </div>
            <pre><code id="install-code" class="language-bash"># Install from cargo
cargo install mirdb</code></pre>
          </div>
          <div class="code-block" data-language="bash">
            <div class="code-block-header">
              <span class="code-block-lang">bash</span>
              <button type="button" class="copy-btn" aria-label="Copy to clipboard" data-copy-target="usage-code">
                <svg class="copy-icon" width="16" height="16" aria-hidden="true">
                  <rect x="9" y="9" width="13" height="13"/>
                </svg>
                <svg class="check-icon" width="16" height="16" aria-hidden="true" style="display: none;">
                  <polyline points="20 6 9 17 4 12"/>
                </svg>
                <span class="copy-btn-text">Copy</span>
              </button>
            </div>
            <pre><code id="usage-code" class="language-bash"># Start the server
mirdb --listen 0.0.0.0:12333

# Set a value
set mykey 0 0 5
hello

# Get the value
get mykey</code></pre>
          </div>
        </div>
      </section>
    </body>
    </html>
  `;
}

describe('Copy to Clipboard', () => {
  let dom;
  let document;
  let window;
  let copyBtn;
  let installCode;

  beforeEach(async () => {
    // Create fresh DOM for each test
    dom = new JSDOM(createQuickStartHTML(), {
      url: 'http://localhost',
      runScripts: 'dangerously',
    });

    window = dom.window;
    document = window.document;

    // Set up global references for the module
    global.window = window;
    global.document = document;
    global.MouseEvent = window.MouseEvent;
    global.navigator = {
      clipboard: {
        writeText: vi.fn().mockResolvedValue(undefined),
        readText: vi.fn().mockResolvedValue(''),
      },
    };

    // Import module fresh for each test
    vi.resetModules();
    copyClipboardModule = await import('../../assets/js/copy-clipboard.js');

    copyBtn = document.querySelector('.copy-btn');
    installCode = document.getElementById('install-code');
  });

  afterEach(() => {
    // Clean up global references
    delete global.window;
    delete global.document;
    delete global.MouseEvent;
    delete global.navigator;
    vi.resetModules();
  });

  describe('copyToClipboard', () => {
    it('should copy text using Clipboard API when available', async () => {
      const testText = 'cargo install mirdb';
      const result = await copyClipboardModule.copyToClipboard(testText);

      expect(result).toBe(true);
      expect(global.navigator.clipboard.writeText).toHaveBeenCalledWith(testText);
    });

    it('should return true on successful copy', async () => {
      const result = await copyClipboardModule.copyToClipboard('test');
      expect(result).toBe(true);
    });

    it('should handle Clipboard API errors gracefully', async () => {
      // Make Clipboard API fail
      global.navigator.clipboard.writeText = vi.fn().mockRejectedValue(new Error('Permission denied'));

      // Mock execCommand fallback
      document.execCommand = vi.fn().mockReturnValue(true);

      const result = await copyClipboardModule.copyToClipboard('test');
      expect(result).toBe(true);
    });

    it('should use fallback when Clipboard API is not available', async () => {
      // Remove Clipboard API
      global.navigator.clipboard = null;

      // Mock execCommand fallback
      document.execCommand = vi.fn().mockReturnValue(true);

      const result = await copyClipboardModule.copyToClipboard('test fallback');
      expect(result).toBe(true);
      expect(document.execCommand).toHaveBeenCalledWith('copy');
    });

    it('should return false when both methods fail', async () => {
      // Make Clipboard API unavailable
      global.navigator.clipboard = null;

      // Make execCommand fail
      document.execCommand = vi.fn().mockImplementation(() => {
        throw new Error('Copy failed');
      });

      const result = await copyClipboardModule.copyToClipboard('test');
      expect(result).toBe(false);
    });
  });

  describe('initCopyButtons', () => {
    it('should initialize without errors', () => {
      expect(() => copyClipboardModule.initCopyButtons()).not.toThrow();
    });

    it('should attach click handlers to all copy buttons', () => {
      const copyButtons = document.querySelectorAll('.copy-btn');
      expect(copyButtons.length).toBe(2);

      copyClipboardModule.initCopyButtons();

      // Both buttons should now have click handlers
      copyButtons.forEach(btn => {
        // Verify that clicking doesn't throw
        expect(() => btn.click()).not.toThrow();
      });
    });

    it('should handle pages with no copy buttons gracefully', () => {
      // Remove all copy buttons
      document.querySelectorAll('.copy-btn').forEach(btn => btn.remove());

      expect(() => copyClipboardModule.initCopyButtons()).not.toThrow();
    });
  });

  describe('Visual Feedback', () => {
    beforeEach(() => {
      copyClipboardModule.initCopyButtons();
    });

    it('should show success state after successful copy', async () => {
      const copyIcon = copyBtn.querySelector('.copy-icon');
      const checkIcon = copyBtn.querySelector('.check-icon');
      const btnText = copyBtn.querySelector('.copy-btn-text');

      // Initial state
      expect(checkIcon.style.display).toBe('none');
      expect(btnText.textContent).toBe('Copy');

      // Trigger copy and wait for async operation
      copyBtn.click();
      await vi.waitFor(() => {
        expect(btnText.textContent).toBe('Copied!');
      });

      // Success state
      expect(copyIcon.style.display).toBe('none');
      expect(checkIcon.style.display).toBe('block');
      expect(copyBtn.classList.contains('copied')).toBe(true);
    });

    it('should update aria-label during copy feedback', async () => {
      expect(copyBtn.getAttribute('aria-label')).toBe('Copy to clipboard');

      copyBtn.click();
      await vi.waitFor(() => {
        expect(copyBtn.getAttribute('aria-label')).toBe('Copied to clipboard');
      });
    });

    it('should reset to initial state after 2 seconds', async () => {
      const copyIcon = copyBtn.querySelector('.copy-icon');
      const checkIcon = copyBtn.querySelector('.check-icon');
      const btnText = copyBtn.querySelector('.copy-btn-text');

      copyBtn.click();
      await vi.waitFor(() => {
        expect(btnText.textContent).toBe('Copied!');
      });

      // Wait for reset (2 seconds + buffer)
      await new Promise(resolve => setTimeout(resolve, 2100));

      // Should be reset
      expect(copyIcon.style.display).toBe('block');
      expect(checkIcon.style.display).toBe('none');
      expect(btnText.textContent).toBe('Copy');
      expect(copyBtn.classList.contains('copied')).toBe(false);
      expect(copyBtn.getAttribute('aria-label')).toBe('Copy to clipboard');
    }, 5000);

    it('should show failed state when copy fails', async () => {
      // Make clipboard API fail
      global.navigator.clipboard = null;
      document.execCommand = vi.fn().mockReturnValue(false);

      const btnText = copyBtn.querySelector('.copy-btn-text');

      copyBtn.click();
      await vi.waitFor(() => {
        expect(btnText.textContent).toBe('Failed');
      });

      expect(copyBtn.classList.contains('copy-failed')).toBe(true);
    });

    it('should reset from failed state after 2 seconds', async () => {
      // Make clipboard fail
      global.navigator.clipboard = null;
      document.execCommand = vi.fn().mockReturnValue(false);

      const btnText = copyBtn.querySelector('.copy-btn-text');

      copyBtn.click();
      await vi.waitFor(() => {
        expect(btnText.textContent).toBe('Failed');
      });

      // Wait for reset (2 seconds + buffer)
      await new Promise(resolve => setTimeout(resolve, 2100));

      expect(btnText.textContent).toBe('Copy');
      expect(copyBtn.classList.contains('copy-failed')).toBe(false);
    }, 5000);
  });

  describe('Code Content Extraction', () => {
    it('should get plain text content from code element', async () => {
      const expectedContent = `# Install from cargo
cargo install mirdb`;

      // We need to trigger copy and check what was copied
      copyClipboardModule.initCopyButtons();
      await copyBtn.click();

      expect(global.navigator.clipboard.writeText).toHaveBeenCalledWith(expectedContent);
    });

    it('should handle code with syntax highlighting spans', async () => {
      // Add some syntax highlighting spans
      installCode.innerHTML = `<span class="token comment"># Install from cargo</span>
<span class="token command">cargo</span> install mirdb`;

      copyClipboardModule.initCopyButtons();
      await copyBtn.click();

      // Should get plain text without HTML
      expect(global.navigator.clipboard.writeText).toHaveBeenCalledWith(
        expect.stringContaining('cargo install mirdb')
      );
    });

    it('should handle missing target element gracefully', async () => {
      // Remove the target code element
      installCode.remove();

      copyClipboardModule.initCopyButtons();

      // Should not throw
      expect(() => copyBtn.click()).not.toThrow();
    });
  });

  describe('Multiple Copy Buttons', () => {
    it('should handle multiple copy buttons independently', async () => {
      const buttons = document.querySelectorAll('.copy-btn');
      copyClipboardModule.initCopyButtons();

      // Click first button
      buttons[0].click();
      await vi.waitFor(() => {
        expect(buttons[0].classList.contains('copied')).toBe(true);
      });
      expect(buttons[1].classList.contains('copied')).toBe(false);

      // Click second button
      buttons[1].click();
      await vi.waitFor(() => {
        expect(buttons[1].classList.contains('copied')).toBe(true);
      });
    });

    it('should copy correct content for each button', async () => {
      copyClipboardModule.initCopyButtons();

      const buttons = document.querySelectorAll('.copy-btn');

      // Click first button (install command)
      buttons[0].click();
      await vi.waitFor(() => {
        expect(global.navigator.clipboard.writeText).toHaveBeenLastCalledWith(
          expect.stringContaining('cargo install mirdb')
        );
      });

      // Click second button (usage example)
      buttons[1].click();
      await vi.waitFor(() => {
        expect(global.navigator.clipboard.writeText).toHaveBeenLastCalledWith(
          expect.stringContaining('set mykey')
        );
      });
    });
  });

  describe('Error Handling', () => {
    it('should handle button without data-copy-target', async () => {
      // Remove target attribute
      copyBtn.removeAttribute('data-copy-target');

      copyClipboardModule.initCopyButtons();

      // Should not throw
      expect(() => copyBtn.click()).not.toThrow();
    });

    it('should handle invalid target ID', async () => {
      copyBtn.setAttribute('data-copy-target', 'nonexistent-element');

      copyClipboardModule.initCopyButtons();

      // Should not throw
      expect(() => copyBtn.click()).not.toThrow();
    });
  });
});
