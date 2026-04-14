/**
 * Clipboard Utility Unit Tests
 * Owner: Scenario 14 - Code Copy Functionality
 *
 * Tests for:
 * - copyToClipboard function
 * - isClipboardSupported function
 * - Fallback behavior when Clipboard API is unavailable
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { copyToClipboard, isClipboardSupported } from '../../src/js/utils/clipboard.js';

describe('Clipboard Utility', () => {
  describe('isClipboardSupported', () => {
    const originalNavigator = global.navigator;

    afterEach(() => {
      // @ts-ignore - restore original navigator
      global.navigator = originalNavigator;
    });

    it('should return true when Clipboard API is available', () => {
      // @ts-ignore - mock navigator
      global.navigator = {
        clipboard: {
          writeText: vi.fn(),
        },
      };

      expect(isClipboardSupported()).toBe(true);
    });

    it('should return false when navigator is undefined', () => {
      // @ts-ignore - mock navigator
      global.navigator = undefined;

      expect(isClipboardSupported()).toBe(false);
    });

    it('should return false when clipboard is undefined', () => {
      // @ts-ignore - mock navigator
      global.navigator = {};

      expect(isClipboardSupported()).toBe(false);
    });

    it('should return false when writeText is not a function', () => {
      // @ts-ignore - mock navigator
      global.navigator = {
        clipboard: {
          writeText: 'not a function',
        },
      };

      expect(isClipboardSupported()).toBe(false);
    });
  });

  describe('copyToClipboard', () => {
    const originalNavigator = global.navigator;
    let mockWriteText: ReturnType<typeof vi.fn>;

    beforeEach(() => {
      mockWriteText = vi.fn().mockResolvedValue(undefined);
      // @ts-ignore - mock navigator
      global.navigator = {
        clipboard: {
          writeText: mockWriteText,
        },
      };
    });

    afterEach(() => {
      // @ts-ignore - restore original navigator
      global.navigator = originalNavigator;
      vi.restoreAllMocks();
    });

    it('should copy text to clipboard and return true on success', async () => {
      const text = 'Hello, World!';
      const result = await copyToClipboard(text);

      expect(mockWriteText).toHaveBeenCalledWith(text);
      expect(result).toBe(true);
    });

    it('should handle empty string', async () => {
      const result = await copyToClipboard('');

      expect(mockWriteText).toHaveBeenCalledWith('');
      expect(result).toBe(true);
    });

    it('should handle multiline text', async () => {
      const text = 'Line 1\nLine 2\nLine 3';
      const result = await copyToClipboard(text);

      expect(mockWriteText).toHaveBeenCalledWith(text);
      expect(result).toBe(true);
    });

    it('should handle special characters', async () => {
      const text = '<script>alert("xss")</script>';
      const result = await copyToClipboard(text);

      expect(mockWriteText).toHaveBeenCalledWith(text);
      expect(result).toBe(true);
    });

    it('should return false when Clipboard API fails', async () => {
      mockWriteText.mockRejectedValue(new Error('Clipboard error'));

      // Mock the fallback method - execCommand returns false
      const mockExecCommand = vi.fn().mockReturnValue(false);
      document.execCommand = mockExecCommand;

      const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {});
      const result = await copyToClipboard('test');

      expect(result).toBe(false);
      consoleError.mockRestore();
    });

    it('should return false for non-string input', async () => {
      const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {});

      // @ts-ignore - intentionally passing non-string
      const result = await copyToClipboard(123);

      expect(result).toBe(false);
      expect(consoleError).toHaveBeenCalledWith('copyToClipboard: text must be a string');
      consoleError.mockRestore();
    });

    it('should return false for null input', async () => {
      const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {});

      // @ts-ignore - intentionally passing null
      const result = await copyToClipboard(null);

      expect(result).toBe(false);
      consoleError.mockRestore();
    });

    it('should return false for undefined input', async () => {
      const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {});

      // @ts-ignore - intentionally passing undefined
      const result = await copyToClipboard(undefined);

      expect(result).toBe(false);
      consoleError.mockRestore();
    });

    describe('fallback method', () => {
      beforeEach(() => {
        // @ts-ignore - remove clipboard API
        global.navigator = {};
      });

      it('should use execCommand fallback when Clipboard API unavailable', async () => {
        const mockExecCommand = vi.fn().mockReturnValue(true);
        document.execCommand = mockExecCommand;

        // Mock createElement and appendChild
        const mockTextArea = {
          value: '',
          style: {} as CSSStyleDeclaration,
          focus: vi.fn(),
          select: vi.fn(),
          setAttribute: vi.fn(),
        };
        vi.spyOn(document, 'createElement').mockReturnValue(mockTextArea as unknown as HTMLTextAreaElement);
        vi.spyOn(document.body, 'appendChild').mockImplementation(() => mockTextArea as unknown as HTMLTextAreaElement);
        vi.spyOn(document.body, 'removeChild').mockImplementation(() => mockTextArea as unknown as HTMLTextAreaElement);

        const result = await copyToClipboard('test text');

        expect(mockExecCommand).toHaveBeenCalledWith('copy');
        expect(result).toBe(true);
      });

      it('should return false when fallback method fails', async () => {
        const mockExecCommand = vi.fn().mockReturnValue(false);
        document.execCommand = mockExecCommand;

        // Mock createElement
        const mockTextArea = {
          value: '',
          style: {} as CSSStyleDeclaration,
          focus: vi.fn(),
          select: vi.fn(),
          setAttribute: vi.fn(),
        };
        vi.spyOn(document, 'createElement').mockReturnValue(mockTextArea as unknown as HTMLTextAreaElement);
        vi.spyOn(document.body, 'appendChild').mockImplementation(() => mockTextArea as unknown as HTMLTextAreaElement);
        vi.spyOn(document.body, 'removeChild').mockImplementation(() => mockTextArea as unknown as HTMLTextAreaElement);

        const result = await copyToClipboard('test text');

        expect(result).toBe(false);
      });
    });
  });
});
