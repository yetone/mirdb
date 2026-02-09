/**
 * Unit tests for clipboard utility.
 * Owner: Scenario 3 - Quick Start Section
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { copyToClipboard, attachCopyButton } from '@/utils/clipboard';

describe('clipboard utility', () => {
  beforeEach(() => {
    // Reset mocks before each test
    vi.restoreAllMocks();
  });

  afterEach(() => {
    document.body.innerHTML = '';
  });

  describe('copyToClipboard', () => {
    it('should copy text to clipboard using navigator.clipboard API', async () => {
      const mockWriteText = vi.fn().mockResolvedValue(undefined);
      Object.defineProperty(navigator, 'clipboard', {
        value: { writeText: mockWriteText },
        writable: true,
        configurable: true,
      });

      const result = await copyToClipboard('test text');

      expect(result).toBe(true);
      expect(mockWriteText).toHaveBeenCalledWith('test text');
    });

    it('should return true on successful copy', async () => {
      const mockWriteText = vi.fn().mockResolvedValue(undefined);
      Object.defineProperty(navigator, 'clipboard', {
        value: { writeText: mockWriteText },
        writable: true,
        configurable: true,
      });

      const result = await copyToClipboard('hello world');

      expect(result).toBe(true);
    });

    it('should return false when clipboard API fails', async () => {
      const mockWriteText = vi.fn().mockRejectedValue(new Error('Permission denied'));
      Object.defineProperty(navigator, 'clipboard', {
        value: { writeText: mockWriteText },
        writable: true,
        configurable: true,
      });

      const result = await copyToClipboard('test text');

      expect(result).toBe(false);
    });

    it('should use fallback when navigator.clipboard is not available', async () => {
      Object.defineProperty(navigator, 'clipboard', {
        value: undefined,
        writable: true,
        configurable: true,
      });

      const mockExecCommand = vi.fn().mockReturnValue(true);
      document.execCommand = mockExecCommand;

      const result = await copyToClipboard('fallback test');

      expect(result).toBe(true);
      expect(mockExecCommand).toHaveBeenCalledWith('copy');
    });

    it('should handle empty string', async () => {
      const mockWriteText = vi.fn().mockResolvedValue(undefined);
      Object.defineProperty(navigator, 'clipboard', {
        value: { writeText: mockWriteText },
        writable: true,
        configurable: true,
      });

      const result = await copyToClipboard('');

      expect(result).toBe(true);
      expect(mockWriteText).toHaveBeenCalledWith('');
    });

    it('should handle multi-line text', async () => {
      const mockWriteText = vi.fn().mockResolvedValue(undefined);
      Object.defineProperty(navigator, 'clipboard', {
        value: { writeText: mockWriteText },
        writable: true,
        configurable: true,
      });

      const multiLineText = 'line 1\nline 2\nline 3';
      const result = await copyToClipboard(multiLineText);

      expect(result).toBe(true);
      expect(mockWriteText).toHaveBeenCalledWith(multiLineText);
    });
  });

  describe('attachCopyButton', () => {
    it('should attach a copy button to a pre element', () => {
      const pre = document.createElement('pre');
      const code = document.createElement('code');
      code.textContent = 'test code';
      pre.appendChild(code);
      document.body.appendChild(pre);

      attachCopyButton(pre);

      const wrapper = document.querySelector('.code-block-wrapper');
      expect(wrapper).not.toBeNull();

      const button = document.querySelector('.copy-button');
      expect(button).not.toBeNull();
      expect(button?.getAttribute('aria-label')).toBe('Copy code to clipboard');
    });

    it('should attach a copy button to a code element', () => {
      const pre = document.createElement('pre');
      const code = document.createElement('code');
      code.textContent = 'test code';
      pre.appendChild(code);
      document.body.appendChild(pre);

      attachCopyButton(code);

      const wrapper = document.querySelector('.code-block-wrapper');
      expect(wrapper).not.toBeNull();

      const button = document.querySelector('.copy-button');
      expect(button).not.toBeNull();
    });

    it('should copy code content when button is clicked', async () => {
      const mockWriteText = vi.fn().mockResolvedValue(undefined);
      Object.defineProperty(navigator, 'clipboard', {
        value: { writeText: mockWriteText },
        writable: true,
        configurable: true,
      });

      const pre = document.createElement('pre');
      const code = document.createElement('code');
      code.textContent = 'cargo install mirdb';
      pre.appendChild(code);
      document.body.appendChild(pre);

      attachCopyButton(pre);

      const button = document.querySelector('.copy-button') as HTMLButtonElement;
      expect(button).not.toBeNull();

      button.click();

      // Wait for async clipboard operation
      await new Promise(resolve => setTimeout(resolve, 10));

      expect(mockWriteText).toHaveBeenCalledWith('cargo install mirdb');
    });

    it('should show visual feedback after copying', async () => {
      vi.useFakeTimers();
      const mockWriteText = vi.fn().mockResolvedValue(undefined);
      Object.defineProperty(navigator, 'clipboard', {
        value: { writeText: mockWriteText },
        writable: true,
        configurable: true,
      });

      const pre = document.createElement('pre');
      const code = document.createElement('code');
      code.textContent = 'test';
      pre.appendChild(code);
      document.body.appendChild(pre);

      attachCopyButton(pre);

      const button = document.querySelector('.copy-button') as HTMLButtonElement;
      button.click();

      // Wait for click handler
      await vi.advanceTimersByTimeAsync(10);

      // Check visual feedback
      expect(button.classList.contains('copied')).toBe(true);
      const copyText = button.querySelector('.copy-text');
      expect(copyText?.textContent).toBe('Copied!');

      // Wait for reset
      await vi.advanceTimersByTimeAsync(2000);

      expect(button.classList.contains('copied')).toBe(false);
      expect(copyText?.textContent).toBe('Copy');

      vi.useRealTimers();
    });

    it('should have copy and check icons', () => {
      const pre = document.createElement('pre');
      const code = document.createElement('code');
      code.textContent = 'test';
      pre.appendChild(code);
      document.body.appendChild(pre);

      attachCopyButton(pre);

      const button = document.querySelector('.copy-button');
      const copyIcon = button?.querySelector('.copy-icon');
      const checkIcon = button?.querySelector('.check-icon');

      expect(copyIcon).not.toBeNull();
      expect(checkIcon).not.toBeNull();
    });
  });
});
