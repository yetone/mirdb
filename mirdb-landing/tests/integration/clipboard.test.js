/**
 * Clipboard Integration Tests
 * Owner: Scenario 3 - Code Examples Section
 *
 * Tests:
 * - Clipboard API mocking
 * - Copy button event handling
 * - Success/error feedback
 */

import { jest } from '@jest/globals';

// Mock DOM environment setup
const createMockDOM = () => {
  document.body.innerHTML = `
    <div class="code-example">
      <button class="copy-btn" data-copy-target="test-code" aria-label="Copy code">
        <svg class="copy-icon"></svg>
        <svg class="check-icon" style="display: none;"></svg>
        <span class="copy-text">Copy</span>
      </button>
      <pre class="code-block">
        <code id="test-code"><span class="code-keyword">const</span> <span class="code-variable">x</span> = <span class="code-number">42</span>;</code>
      </pre>
    </div>
  `;
};

describe('Clipboard Module', () => {
  let clipboard;

  beforeEach(async () => {
    createMockDOM();

    // Reset modules
    jest.resetModules();

    // Mock navigator.clipboard
    const mockClipboard = {
      writeText: jest.fn().mockResolvedValue(undefined),
      readText: jest.fn().mockResolvedValue(''),
    };

    Object.defineProperty(navigator, 'clipboard', {
      value: mockClipboard,
      writable: true,
      configurable: true,
    });

    // Import module after setting up mocks
    clipboard = await import('../../js/modules/clipboard.js');
  });

  afterEach(() => {
    document.body.innerHTML = '';
    jest.clearAllMocks();
  });

  describe('extractCodeText', () => {
    test('extracts plain text from code element with HTML spans', () => {
      const codeElement = document.getElementById('test-code');
      const text = clipboard.extractCodeText(codeElement);

      expect(text).toBe('const x = 42;');
    });

    test('handles empty code element', () => {
      const emptyCode = document.createElement('code');
      emptyCode.id = 'empty-code';
      document.body.appendChild(emptyCode);

      const text = clipboard.extractCodeText(emptyCode);
      expect(text).toBe('');
    });

    test('preserves whitespace and newlines', () => {
      const multilineCode = document.createElement('code');
      multilineCode.innerHTML = `<span>line1</span>
<span>line2</span>`;
      document.body.appendChild(multilineCode);

      const text = clipboard.extractCodeText(multilineCode);
      expect(text).toContain('line1');
      expect(text).toContain('line2');
    });
  });

  describe('copyToClipboard', () => {
    test('TC5: Clipboard API is called with correct code content', async () => {
      const testText = 'test code content';
      const success = await clipboard.copyToClipboard(testText);

      expect(navigator.clipboard.writeText).toHaveBeenCalledWith(testText);
      expect(success).toBe(true);
    });

    test('returns true on successful copy', async () => {
      const success = await clipboard.copyToClipboard('test');
      expect(success).toBe(true);
    });

    test('handles clipboard API errors gracefully', async () => {
      navigator.clipboard.writeText.mockRejectedValueOnce(new Error('Permission denied'));

      // Mock execCommand for fallback
      document.execCommand = jest.fn().mockReturnValue(true);

      const success = await clipboard.copyToClipboard('test');

      // Should attempt fallback
      expect(document.execCommand).toHaveBeenCalledWith('copy');
    });
  });

  describe('showFeedback', () => {
    test('adds copied class to button on success', () => {
      const button = document.querySelector('.copy-btn');

      clipboard.showFeedback(button, true);

      expect(button.classList.contains('copied')).toBe(true);
    });

    test('updates button text to Copied!', () => {
      const button = document.querySelector('.copy-btn');
      const copyText = button.querySelector('.copy-text');

      clipboard.showFeedback(button, true);

      expect(copyText.textContent).toBe('Copied!');
    });

    test('shows check icon and hides copy icon', () => {
      const button = document.querySelector('.copy-btn');
      const copyIcon = button.querySelector('.copy-icon');
      const checkIcon = button.querySelector('.check-icon');

      clipboard.showFeedback(button, true);

      expect(copyIcon.style.display).toBe('none');
      expect(checkIcon.style.display).toBe('block');
    });

    test('resets state after timeout', async () => {
      jest.useFakeTimers();

      const button = document.querySelector('.copy-btn');
      const copyText = button.querySelector('.copy-text');

      clipboard.showFeedback(button, true);

      expect(button.classList.contains('copied')).toBe(true);

      jest.advanceTimersByTime(2000);

      expect(button.classList.contains('copied')).toBe(false);
      expect(copyText.textContent).toBe('Copy');

      jest.useRealTimers();
    });

    test('does not add class on failure', () => {
      const button = document.querySelector('.copy-btn');

      clipboard.showFeedback(button, false);

      expect(button.classList.contains('copied')).toBe(false);
    });
  });

  describe('handleCopyClick', () => {
    test('copies code content when button is clicked', async () => {
      const button = document.querySelector('.copy-btn');

      const event = { currentTarget: button };
      await clipboard.handleCopyClick(event);

      expect(navigator.clipboard.writeText).toHaveBeenCalledWith('const x = 42;');
    });

    test('logs error when target element is missing', async () => {
      const consoleSpy = jest.spyOn(console, 'error').mockImplementation(() => {});

      const button = document.createElement('button');
      button.className = 'copy-btn';
      button.dataset.copyTarget = 'nonexistent';

      const event = { currentTarget: button };
      await clipboard.handleCopyClick(event);

      expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('not found'));

      consoleSpy.mockRestore();
    });

    test('logs error when data-copy-target is missing', async () => {
      const consoleSpy = jest.spyOn(console, 'error').mockImplementation(() => {});

      const button = document.createElement('button');
      button.className = 'copy-btn';

      const event = { currentTarget: button };
      await clipboard.handleCopyClick(event);

      expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('missing'));

      consoleSpy.mockRestore();
    });
  });

  describe('init', () => {
    test('attaches click handlers to all copy buttons', () => {
      // Add another button
      const newButton = document.createElement('button');
      newButton.className = 'copy-btn';
      newButton.dataset.copyTarget = 'test-code';
      document.body.appendChild(newButton);

      const addEventListenerSpy = jest.spyOn(HTMLButtonElement.prototype, 'addEventListener');

      clipboard.init();

      // Should have attached click handlers
      expect(addEventListenerSpy).toHaveBeenCalledWith('click', expect.any(Function));

      addEventListenerSpy.mockRestore();
    });
  });
});
