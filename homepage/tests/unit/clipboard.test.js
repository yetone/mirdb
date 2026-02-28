/**
 * Unit tests for clipboard functionality
 * Owner: Scenario 3 - Usage Examples Section
 *
 * Expected test cases:
 * - copyToClipboard copies text successfully
 * - copyToClipboard handles errors gracefully
 * - showCopyFeedback shows success indicator
 * - showCopyFeedback shows error indicator
 * - initCopyButtons attaches event listeners
 */

// Mock the clipboard module
const clipboardModule = require('../../src/js/clipboard.js');
const { copyToClipboard, showCopyFeedback, initCopyButtons } = clipboardModule;

describe('Clipboard functionality', () => {
  // Mock navigator.clipboard
  const mockWriteText = jest.fn();

  beforeEach(() => {
    // Reset mocks
    jest.clearAllMocks();

    // Setup clipboard mock
    Object.defineProperty(global.navigator, 'clipboard', {
      value: {
        writeText: mockWriteText
      },
      writable: true,
      configurable: true
    });
  });

  describe('copyToClipboard', () => {
    test('copies text successfully using Clipboard API', async () => {
      mockWriteText.mockResolvedValueOnce(undefined);

      const result = await copyToClipboard('test text');

      expect(result).toBe(true);
      expect(mockWriteText).toHaveBeenCalledWith('test text');
    });

    test('returns false when clipboard API fails', async () => {
      mockWriteText.mockRejectedValueOnce(new Error('Clipboard error'));

      const result = await copyToClipboard('test text');

      expect(result).toBe(false);
    });

    test('returns false for empty string', async () => {
      const result = await copyToClipboard('');

      expect(result).toBe(false);
      expect(mockWriteText).not.toHaveBeenCalled();
    });

    test('returns false for null input', async () => {
      const result = await copyToClipboard(null);

      expect(result).toBe(false);
      expect(mockWriteText).not.toHaveBeenCalled();
    });

    test('returns false for undefined input', async () => {
      const result = await copyToClipboard(undefined);

      expect(result).toBe(false);
      expect(mockWriteText).not.toHaveBeenCalled();
    });

    test('returns false for non-string input', async () => {
      const result = await copyToClipboard(123);

      expect(result).toBe(false);
      expect(mockWriteText).not.toHaveBeenCalled();
    });

    test('handles clipboard API error gracefully without throwing', async () => {
      mockWriteText.mockRejectedValueOnce(new Error('Permission denied'));

      // This should not throw
      await expect(copyToClipboard('test')).resolves.toBe(false);
    });
  });

  describe('showCopyFeedback', () => {
    beforeEach(() => {
      jest.useFakeTimers();
    });

    afterEach(() => {
      jest.useRealTimers();
    });

    test('shows success indicator by adding copied class', () => {
      document.body.innerHTML = `
        <button class="copy-btn">
          <span class="copy-text">Copy</span>
        </button>
      `;

      const button = document.querySelector('.copy-btn');
      showCopyFeedback(button, true);

      expect(button.classList.contains('copied')).toBe(true);
      expect(button.querySelector('.copy-text').textContent).toBe('Copied!');
    });

    test('shows error indicator by adding error class', () => {
      document.body.innerHTML = `
        <button class="copy-btn">
          <span class="copy-text">Copy</span>
        </button>
      `;

      const button = document.querySelector('.copy-btn');
      showCopyFeedback(button, false);

      expect(button.classList.contains('error')).toBe(true);
      expect(button.querySelector('.copy-text').textContent).toBe('Failed');
    });

    test('resets feedback after delay', () => {
      document.body.innerHTML = `
        <button class="copy-btn">
          <span class="copy-text">Copy</span>
        </button>
      `;

      const button = document.querySelector('.copy-btn');
      showCopyFeedback(button, true);

      expect(button.classList.contains('copied')).toBe(true);

      // Fast-forward time
      jest.advanceTimersByTime(2000);

      expect(button.classList.contains('copied')).toBe(false);
      expect(button.querySelector('.copy-text').textContent).toBe('Copy');
    });

    test('handles null button gracefully', () => {
      // Should not throw
      expect(() => showCopyFeedback(null, true)).not.toThrow();
    });

    test('handles button without copy-text element', () => {
      document.body.innerHTML = `
        <button class="copy-btn"></button>
      `;

      const button = document.querySelector('.copy-btn');

      // Should not throw
      expect(() => showCopyFeedback(button, true)).not.toThrow();
      expect(button.classList.contains('copied')).toBe(true);
    });
  });

  describe('initCopyButtons', () => {
    test('attaches click event listeners to copy buttons', async () => {
      document.body.innerHTML = `
        <div class="code-block-container">
          <button class="copy-btn" data-code="test code">
            <span class="copy-text">Copy</span>
          </button>
          <div class="code-block">
            <code>test code</code>
          </div>
        </div>
      `;

      initCopyButtons();

      const button = document.querySelector('.copy-btn');

      // Mock copyToClipboard for this test
      mockWriteText.mockResolvedValueOnce(undefined);

      // Simulate click
      button.click();

      // Wait for async click handler to complete
      await Promise.resolve();
      await Promise.resolve();

      expect(mockWriteText).toHaveBeenCalledWith('test code');
    });

    test('handles multiple copy buttons', () => {
      document.body.innerHTML = `
        <button class="copy-btn" data-code="code1"></button>
        <button class="copy-btn" data-code="code2"></button>
        <button class="copy-btn" data-code="code3"></button>
      `;

      // Should not throw
      expect(() => initCopyButtons()).not.toThrow();

      const buttons = document.querySelectorAll('.copy-btn');
      expect(buttons.length).toBe(3);
    });

    test('decodes HTML entities in data-code', async () => {
      document.body.innerHTML = `
        <button class="copy-btn" data-code="line1&#10;line2">
          <span class="copy-text">Copy</span>
        </button>
      `;

      mockWriteText.mockResolvedValueOnce(undefined);
      initCopyButtons();

      const button = document.querySelector('.copy-btn');
      button.click();

      // Wait for async click handler to complete
      await Promise.resolve();
      await Promise.resolve();

      expect(mockWriteText).toHaveBeenCalledWith('line1\nline2');
    });
  });
});
