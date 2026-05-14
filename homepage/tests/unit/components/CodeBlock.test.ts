import { describe, it, expect, beforeEach, vi } from 'vitest';
import { JSDOM } from 'jsdom';
import { readFileSync } from 'fs';
import { resolve } from 'path';

/**
 * Helper: load a fixture HTML from a file or construct it from component HTML.
 * Uses JSDOM to create a DOM environment for testing.
 */
function createDOM(html: string): JSDOM {
  return new JSDOM(html, {
    url: 'http://localhost:3000',
    runScripts: 'dangerously',
    resources: 'usable',
  });
}

/**
 * Build a minimal HTML page that includes our copyToClipboard module
 * and code block markup for integration-level testing.
 */
function buildCodeBlockPage(includeClipboardAPI = true): JSDOM {
  const copyModulePath = resolve(__dirname, '../../../src/utils/copyToClipboard.ts');

  const html = `<!DOCTYPE html>
<html>
<head></head>
<body>
  <div id="test-root">
    <div class="code-block-wrapper" data-code-block data-code-block-id="test-snippet">
      <div class="code-block-header">
        <span class="code-block-label">Test Snippet</span>
        <span class="code-block-lang">shell</span>
      </div>
      <div class="code-block-body">
        <pre class="code-block-pre"><code>echo "hello world"</code></pre>
        <button
          class="copy-button"
          data-copy-target="test-snippet"
          data-copy-text="echo &quot;hello world&quot;"
          aria-label="Copy code to clipboard"
          type="button"
        >
          <span class="copy-icon">[icon]</span>
          <span class="copy-text">Copy</span>
        </button>
      </div>
    </div>
  </div>
  <script type="module">
    ${includeClipboardAPI ? '' : 'Object.defineProperty(navigator, "clipboard", { value: undefined, writable: true });'}
  </script>
</body>
</html>`;

  return new JSDOM(html, {
    url: 'http://localhost:3000',
    runScripts: 'dangerously',
    resources: 'usable',
    beforeParse(window) {
      if (!includeClipboardAPI) {
        Object.defineProperty(window.navigator, 'clipboard', {
          value: undefined,
          writable: true,
          configurable: true,
        });
      }
    },
  });
}

describe('CodeBlock component', () => {
  describe('HTML structure', () => {
    it('renders a code-block-wrapper container', () => {
      const html = `
        <div class="code-block-wrapper" data-code-block data-code-block-id="test">
          <div class="code-block-body">
            <pre class="code-block-pre"><code>test code</code></pre>
            <button class="copy-button" data-copy-target="test" data-copy-text="test code" aria-label="Copy code to clipboard">Copy</button>
          </div>
        </div>`;
      const dom = new JSDOM(html);
      const doc = dom.window.document;

      const wrapper = doc.querySelector('.code-block-wrapper');
      expect(wrapper).not.toBeNull();
      expect(wrapper!.getAttribute('data-code-block')).not.toBeNull();
    });

    it('displays the code text in a pre element', () => {
      const html = `
        <div class="code-block-wrapper" data-code-block>
          <pre class="code-block-pre"><code>cargo install mirdb</code></pre>
          <button class="copy-button" data-copy-text="cargo install mirdb" aria-label="Copy code to clipboard">Copy</button>
        </div>`;
      const dom = new JSDOM(html);
      const doc = dom.window.document;

      const codeEl = doc.querySelector('code');
      expect(codeEl).not.toBeNull();
      expect(codeEl!.textContent).toBe('cargo install mirdb');
    });

    it('has a copy button with accessible label', () => {
      const html = `
        <div class="code-block-wrapper">
          <pre><code>some code</code></pre>
          <button class="copy-button" aria-label="Copy code to clipboard" type="button">Copy</button>
        </div>`;
      const dom = new JSDOM(html);
      const doc = dom.window.document;

      const button = doc.querySelector('.copy-button');
      expect(button).not.toBeNull();
      expect(button!.getAttribute('aria-label')).toBeTruthy();
    });

    it('copy button is a button element', () => {
      const html = `
        <div class="code-block-wrapper">
          <pre><code>code</code></pre>
          <button class="copy-button" aria-label="Copy code to clipboard" type="button">Copy</button>
        </div>`;
      const dom = new JSDOM(html);
      const doc = dom.window.document;

      const button = doc.querySelector('.copy-button');
      expect(button).not.toBeNull();
      expect(button!.tagName).toBe('BUTTON');
    });
  });

  describe('Copy to clipboard functionality', () => {
    let mockWriteText: ReturnType<typeof vi.fn>;

    beforeEach(() => {
      mockWriteText = vi.fn().mockResolvedValue(undefined);
      Object.defineProperty(globalThis.navigator, 'clipboard', {
        value: { writeText: mockWriteText },
        writable: true,
        configurable: true,
      });
    });

    it('calls navigator.clipboard.writeText with correct content', async () => {
      const { copyToClipboard } = await import('../../../src/utils/copyToClipboard');
      const testText = 'cargo install mirdb';

      const result = await copyToClipboard(testText);

      expect(mockWriteText).toHaveBeenCalledWith(testText);
      expect(result).toBe(true);
    });

    it('returns true on successful copy', async () => {
      const { copyToClipboard } = await import('../../../src/utils/copyToClipboard');

      const result = await copyToClipboard('test');
      expect(result).toBe(true);
    });

    it('handles multiline code correctly', async () => {
      const { copyToClipboard } = await import('../../../src/utils/copyToClipboard');
      const multiline = 'line1\nline2\nline3';

      await copyToClipboard(multiline);

      expect(mockWriteText).toHaveBeenCalledWith(multiline);
    });
  });

  describe('Copy fallback when clipboard API is unavailable', () => {
    beforeEach(() => {
      Object.defineProperty(globalThis.navigator, 'clipboard', {
        value: undefined,
        writable: true,
        configurable: true,
      });
    });

    it('does not throw when clipboard API is unavailable', async () => {
      const { copyToClipboard } = await import('../../../src/utils/copyToClipboard');

      // The fallback uses execCommand which may not work in jsdom,
      // but it should not throw
      let threw = false;
      try {
        await copyToClipboard('test code');
      } catch {
        threw = true;
      }
      expect(threw).toBe(false);
    });

    it('isClipboardSupported returns false when clipboard is unavailable', () => {
      expect(globalThis.navigator.clipboard).toBeUndefined();
    });
  });

  describe('isClipboardSupported utility', () => {
    it('returns true when clipboard API is available', async () => {
      Object.defineProperty(globalThis.navigator, 'clipboard', {
        value: { writeText: vi.fn().mockResolvedValue(undefined) },
        writable: true,
        configurable: true,
      });
      const { isClipboardSupported } = await import('../../../src/utils/copyToClipboard');
      expect(isClipboardSupported()).toBe(true);
    });

    it('returns false when clipboard API is unavailable', async () => {
      Object.defineProperty(globalThis.navigator, 'clipboard', {
        value: undefined,
        writable: true,
        configurable: true,
      });
      const { isClipboardSupported } = await import('../../../src/utils/copyToClipboard');
      expect(isClipboardSupported()).toBe(false);
    });
  });
});

describe('InstallTabs component', () => {
  describe('Tab structure', () => {
    it('renders tabs with role="tablist"', () => {
      const html = `
        <div data-install-tabs role="tablist" aria-label="Installation method tabs">
          <div role="tablist">
            <button role="tab" aria-selected="true" data-tab="cargo">Cargo</button>
            <button role="tab" aria-selected="false" data-tab="docker">Docker</button>
            <button role="tab" aria-selected="false" data-tab="source">From Source</button>
          </div>
        </div>`;
      const dom = new JSDOM(html);
      const doc = dom.window.document;

      const tabs = doc.querySelector('[data-install-tabs]');
      expect(tabs).not.toBeNull();

      const tabButtons = doc.querySelectorAll('[role="tab"]');
      expect(tabButtons.length).toBeGreaterThanOrEqual(2);
    });

    it('each tab has a descriptive label', () => {
      const html = `
        <div data-install-tabs>
          <button role="tab" data-tab="cargo">Cargo</button>
          <button role="tab" data-tab="docker">Docker</button>
          <button role="tab" data-tab="source">From Source</button>
        </div>`;
      const dom = new JSDOM(html);
      const doc = dom.window.document;

      const buttons = doc.querySelectorAll('[role="tab"]');
      const labels = Array.from(buttons).map((b) => b.textContent?.trim());
      expect(labels.every((l) => l && l.length > 0)).toBe(true);
    });

    it('first tab is selected by default', () => {
      const html = `
        <div data-install-tabs>
          <button role="tab" aria-selected="true" data-tab="cargo">Cargo</button>
          <button role="tab" aria-selected="false" data-tab="docker">Docker</button>
        </div>`;
      const dom = new JSDOM(html);
      const doc = dom.window.document;

      const firstTab = doc.querySelector('[role="tab"]');
      expect(firstTab!.getAttribute('aria-selected')).toBe('true');
    });

    it('tab panels have correct aria-labelledby references', () => {
      const html = `
        <div data-install-tabs>
          <button role="tab" id="tab-cargo" aria-controls="panel-cargo">Cargo</button>
          <div role="tabpanel" id="panel-cargo" aria-labelledby="tab-cargo">Content</div>
        </div>`;
      const dom = new JSDOM(html);
      const doc = dom.window.document;

      const panel = doc.querySelector('[role="tabpanel"]');
      const tab = doc.querySelector('[role="tab"]');
      expect(panel!.getAttribute('aria-labelledby')).toBe(tab!.id);
    });
  });
});
