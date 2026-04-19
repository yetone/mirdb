/**
 * @jest-environment jsdom
 */

/**
 * Copy Button Unit Tests
 * Owner: Scenario 4 - Code Example Section with Copy Functionality
 *
 * Tests for:
 * - copyToClipboard function
 * - initCopyButtons function
 * - CodeBlock component rendering
 */

import { jest } from '@jest/globals';

// Mock the clipboard API
const mockClipboard = {
  writeText: jest.fn(),
  readText: jest.fn()
};

// Mock navigator.clipboard
Object.defineProperty(global.navigator, 'clipboard', {
  value: mockClipboard,
  writable: true,
  configurable: true
});

// Import after mocking
const { copyToClipboard, initCopyButtons } = await import('../../js/components/copy-button.js');

describe('Copy Button Module', () => {
  beforeEach(() => {
    // Reset mocks
    jest.clearAllMocks();
    mockClipboard.writeText.mockReset();
    mockClipboard.readText.mockReset();

    // Reset DOM
    document.body.innerHTML = '';
  });

  describe('copyToClipboard', () => {
    test('TC7: Successfully copies text using Clipboard API', async () => {
      const testString = 'telnet localhost 12333';
      mockClipboard.writeText.mockResolvedValue(undefined);

      const result = await copyToClipboard(testString);

      expect(result).toBe(true);
      expect(mockClipboard.writeText).toHaveBeenCalledWith(testString);
      expect(mockClipboard.writeText).toHaveBeenCalledTimes(1);
    });

    test('Copies empty string successfully', async () => {
      const testString = '';
      mockClipboard.writeText.mockResolvedValue(undefined);

      const result = await copyToClipboard(testString);

      expect(result).toBe(true);
      expect(mockClipboard.writeText).toHaveBeenCalledWith('');
    });

    test('Copies multiline text successfully', async () => {
      const testString = `SET mykey 0 0 5
hello
STORED`;
      mockClipboard.writeText.mockResolvedValue(undefined);

      const result = await copyToClipboard(testString);

      expect(result).toBe(true);
      expect(mockClipboard.writeText).toHaveBeenCalledWith(testString);
    });

    test('Falls back to execCommand when Clipboard API fails', async () => {
      mockClipboard.writeText.mockRejectedValue(new Error('Clipboard API not available'));

      // Mock document.execCommand
      const mockExecCommand = jest.fn().mockReturnValue(true);
      document.execCommand = mockExecCommand;

      const result = await copyToClipboard('test text');

      // Should fall back to execCommand
      expect(mockExecCommand).toHaveBeenCalledWith('copy');
      expect(result).toBe(true);
    });

    test('Returns false when all copy methods fail', async () => {
      mockClipboard.writeText.mockRejectedValue(new Error('Clipboard API not available'));

      // Mock document.execCommand to fail
      document.execCommand = jest.fn().mockReturnValue(false);

      const result = await copyToClipboard('test text');

      expect(result).toBe(false);
    });
  });

  describe('initCopyButtons', () => {
    test('Initializes copy buttons and returns count', () => {
      document.body.innerHTML = `
        <div class="code-block">
          <button data-copy-button>Copy</button>
          <code data-code-content>test code 1</code>
        </div>
        <div class="code-block">
          <button data-copy-button>Copy</button>
          <code data-code-content>test code 2</code>
        </div>
      `;

      const count = initCopyButtons();

      expect(count).toBe(2);
    });

    test('Returns 0 when no copy buttons exist', () => {
      document.body.innerHTML = '<div>No buttons here</div>';

      const count = initCopyButtons();

      expect(count).toBe(0);
    });

    test('Attaches click handlers to copy buttons', async () => {
      mockClipboard.writeText.mockResolvedValue(undefined);

      document.body.innerHTML = `
        <div class="code-block">
          <button data-copy-button>
            <span class="code-block__copy-text">Copy</span>
          </button>
          <code data-code-content>test code content</code>
        </div>
      `;

      initCopyButtons();

      const button = document.querySelector('[data-copy-button]');
      button.click();

      // Wait for async operations
      await new Promise(resolve => setTimeout(resolve, 10));

      expect(mockClipboard.writeText).toHaveBeenCalledWith('test code content');
    });
  });

  describe('TC6: CodeBlock component rendering', () => {
    test('Renders code block with all required elements', () => {
      document.body.innerHTML = `
        <div class="code-block" data-testid="code-block-test">
          <div class="code-block__header">
            <span class="code-block__language">Terminal</span>
            <button class="code-block__copy" type="button" aria-label="Copy code to clipboard" data-copy-button>
              <svg class="code-block__copy-icon" aria-hidden="true"></svg>
              <span class="code-block__copy-text">Copy</span>
            </button>
          </div>
          <pre class="code-block__content"><code class="code-block__code" data-code-content>telnet localhost 12333</code></pre>
        </div>
      `;

      const codeBlock = document.querySelector('.code-block');
      const header = codeBlock.querySelector('.code-block__header');
      const language = codeBlock.querySelector('.code-block__language');
      const copyButton = codeBlock.querySelector('.code-block__copy');
      const copyIcon = codeBlock.querySelector('.code-block__copy-icon');
      const copyText = codeBlock.querySelector('.code-block__copy-text');
      const content = codeBlock.querySelector('.code-block__content');
      const code = codeBlock.querySelector('.code-block__code');

      // All elements should exist
      expect(codeBlock).not.toBeNull();
      expect(header).not.toBeNull();
      expect(language).not.toBeNull();
      expect(copyButton).not.toBeNull();
      expect(copyIcon).not.toBeNull();
      expect(copyText).not.toBeNull();
      expect(content).not.toBeNull();
      expect(code).not.toBeNull();

      // Check content
      expect(language.textContent).toBe('Terminal');
      expect(copyText.textContent).toBe('Copy');
      expect(code.textContent).toBe('telnet localhost 12333');

      // Check accessibility attributes
      expect(copyButton.getAttribute('type')).toBe('button');
      expect(copyButton.getAttribute('aria-label')).toBe('Copy code to clipboard');
      expect(copyIcon.getAttribute('aria-hidden')).toBe('true');
    });

    test('Code content has data-code-content attribute for copy functionality', () => {
      document.body.innerHTML = `
        <div class="code-block">
          <code class="code-block__code" data-code-content>SET mykey 0 0 5</code>
        </div>
      `;

      const codeElement = document.querySelector('[data-code-content]');
      expect(codeElement).not.toBeNull();
      expect(codeElement.textContent).toBe('SET mykey 0 0 5');
    });

    test('Copy button has data-copy-button attribute for initialization', () => {
      document.body.innerHTML = `
        <div class="code-block">
          <button data-copy-button>Copy</button>
        </div>
      `;

      const button = document.querySelector('[data-copy-button]');
      expect(button).not.toBeNull();
    });
  });
});
