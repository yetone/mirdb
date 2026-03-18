/**
 * Clipboard Utility Tests.
 * Owner: Scenario 3 - Usage Examples and Code Blocks
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { copyToClipboard } from '../../../src/utils/clipboard';

describe('copyToClipboard', () => {
  const originalNavigator = global.navigator;
  const originalExecCommand = document.execCommand;

  beforeEach(() => {
    vi.resetAllMocks();
  });

  afterEach(() => {
    // Restore original navigator
    Object.defineProperty(global, 'navigator', {
      value: originalNavigator,
      writable: true,
      configurable: true,
    });
    document.execCommand = originalExecCommand;
  });

  it('should copy text using navigator.clipboard.writeText', async () => {
    const mockWriteText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(global, 'navigator', {
      value: {
        clipboard: {
          writeText: mockWriteText,
        },
      },
      writable: true,
      configurable: true,
    });

    const result = await copyToClipboard('test text');

    expect(result).toBe(true);
    expect(mockWriteText).toHaveBeenCalledWith('test text');
  });

  it('should return false when clipboard API fails', async () => {
    const mockWriteText = vi.fn().mockRejectedValue(new Error('Failed'));
    Object.defineProperty(global, 'navigator', {
      value: {
        clipboard: {
          writeText: mockWriteText,
        },
      },
      writable: true,
      configurable: true,
    });

    // Mock execCommand for fallback
    document.execCommand = vi.fn().mockReturnValue(false);

    const result = await copyToClipboard('test text');

    expect(result).toBe(false);
  });

  it('should use fallback when clipboard API is not available', async () => {
    Object.defineProperty(global, 'navigator', {
      value: {
        clipboard: undefined,
      },
      writable: true,
      configurable: true,
    });

    document.execCommand = vi.fn().mockReturnValue(true);

    const result = await copyToClipboard('test text');

    expect(document.execCommand).toHaveBeenCalledWith('copy');
    expect(result).toBe(true);
  });
});
