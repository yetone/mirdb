/**
 * Scenario 3 - Copy-to-Clipboard Unit Tests
 * Owner: Scenario 3
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { JSDOM } from 'jsdom';
import { initCopyButtons } from '../../js/copy.js';

function createDom(html) {
  const dom = new JSDOM(html, {
    url: 'http://localhost:3000',
    runScripts: 'dangerously',
  });
  global.document = dom.window.document;
  global.window = dom.window;
  Object.defineProperty(global, 'navigator', {
    value: dom.window.navigator,
    writable: true,
    configurable: true,
  });
  return dom;
}

function resetGlobals() {
  delete global.document;
  delete global.window;
  Object.defineProperty(global, 'navigator', {
    value: undefined,
    writable: true,
    configurable: true,
  });
}

const TEST_HTML = `
<!DOCTYPE html>
<html>
<body>
  <section id="install">
    <div class="code-block">
      <pre><code data-copy>cargo install mirdb</code></pre>
      <button class="copy-btn" aria-label="Copy">Copy</button>
    </div>
    <div class="code-block">
      <pre><code data-copy>curl -sSL https://github.com/yetone/mirdb/releases/latest/download/mirdb-linux-x64.tar.gz | tar xz
./mirdb</code></pre>
      <button class="copy-btn" aria-label="Copy">Copy</button>
    </div>
  </section>
</body>
</html>
`;

describe('copy.js', () => {
  beforeEach(() => {
    createDom(TEST_HTML);
  });

  afterEach(() => {
    resetGlobals();
    vi.restoreAllMocks();
  });

  describe('initCopyButtons', () => {
    it('calls navigator.clipboard.writeText with the code block content when clicked', async () => {
      const writeText = vi.fn().mockResolvedValue(undefined);
      global.navigator.clipboard = { writeText };

      initCopyButtons();

      const firstButton = document.querySelector('.copy-btn');
      firstButton.click();

      // Wait for async handler
      await new Promise(r => setTimeout(r, 10));

      expect(writeText).toHaveBeenCalledTimes(1);
      expect(writeText).toHaveBeenCalledWith('cargo install mirdb');
    });

    it('falls back to document.execCommand when clipboard API is unavailable', async () => {
      global.navigator.clipboard = undefined;
      const execCommand = vi.fn().mockReturnValue(true);
      global.document.execCommand = execCommand;

      initCopyButtons();

      const firstButton = document.querySelector('.copy-btn');

      expect(() => firstButton.click()).not.toThrow();

      // Wait for async handler
      await new Promise(r => setTimeout(r, 10));

      expect(execCommand).toHaveBeenCalledWith('copy');
    });

    it('does not register duplicate handlers when called twice', async () => {
      const writeText = vi.fn().mockResolvedValue(undefined);
      global.navigator.clipboard = { writeText };

      initCopyButtons();
      initCopyButtons(); // Call twice

      const firstButton = document.querySelector('.copy-btn');
      firstButton.click();

      await new Promise(r => setTimeout(r, 10));

      expect(writeText).toHaveBeenCalledTimes(1);
    });

    it('copies the correct content for the second code block', async () => {
      const writeText = vi.fn().mockResolvedValue(undefined);
      global.navigator.clipboard = { writeText };

      initCopyButtons();

      const buttons = document.querySelectorAll('.copy-btn');
      buttons[1].click();

      await new Promise(r => setTimeout(r, 10));

      expect(writeText).toHaveBeenCalledTimes(1);
      expect(writeText).toHaveBeenCalledWith(
        'curl -sSL https://github.com/yetone/mirdb/releases/latest/download/mirdb-linux-x64.tar.gz | tar xz\n./mirdb'
      );
    });

    it('adds "copied" class after successful copy and removes it after timeout', async () => {
      const writeText = vi.fn().mockResolvedValue(undefined);
      global.navigator.clipboard = { writeText };

      initCopyButtons();

      const firstButton = document.querySelector('.copy-btn');
      firstButton.click();

      await new Promise(r => setTimeout(r, 50));

      expect(firstButton.classList.contains('copied')).toBe(true);

      await new Promise(r => setTimeout(r, 2100));

      expect(firstButton.classList.contains('copied')).toBe(false);
    });
  });
});
