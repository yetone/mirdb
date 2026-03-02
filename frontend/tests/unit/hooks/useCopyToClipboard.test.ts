/**
 * Unit tests for useCopyToClipboard hook.
 * Owner: Scenario 2 - Guest URL Creation Flow
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, act, waitFor } from '@testing-library/react';
import { useCopyToClipboard } from '../../../src/hooks/useCopyToClipboard';

describe('useCopyToClipboard', () => {
  const mockWriteText = vi.fn();

  beforeEach(() => {
    vi.useFakeTimers();
    // Reset clipboard mock
    Object.defineProperty(navigator, 'clipboard', {
      value: {
        writeText: mockWriteText,
      },
      writable: true,
      configurable: true,
    });
    mockWriteText.mockResolvedValue(undefined);
  });

  afterEach(() => {
    vi.clearAllMocks();
    vi.useRealTimers();
  });

  it('should initialize with default state', () => {
    const { result } = renderHook(() => useCopyToClipboard());

    expect(result.current.isCopied).toBe(false);
    expect(result.current.error).toBeNull();
    expect(typeof result.current.copy).toBe('function');
    expect(typeof result.current.reset).toBe('function');
  });

  it('should copy text to clipboard successfully', async () => {
    const { result } = renderHook(() => useCopyToClipboard());

    await act(async () => {
      const success = await result.current.copy('test-text');
      expect(success).toBe(true);
    });

    expect(mockWriteText).toHaveBeenCalledWith('test-text');
    expect(result.current.isCopied).toBe(true);
    expect(result.current.error).toBeNull();
  });

  it('should auto-reset isCopied after delay', async () => {
    const { result } = renderHook(() => useCopyToClipboard(1000));

    await act(async () => {
      await result.current.copy('test-text');
    });

    expect(result.current.isCopied).toBe(true);

    // Advance timer
    act(() => {
      vi.advanceTimersByTime(1000);
    });

    expect(result.current.isCopied).toBe(false);
  });

  it('should handle empty text error', async () => {
    const { result } = renderHook(() => useCopyToClipboard());

    await act(async () => {
      const success = await result.current.copy('');
      expect(success).toBe(false);
    });

    expect(result.current.isCopied).toBe(false);
    expect(result.current.error).toBe('No text to copy');
    expect(mockWriteText).not.toHaveBeenCalled();
  });

  it('should handle clipboard API error', async () => {
    mockWriteText.mockRejectedValueOnce(new Error('Clipboard error'));
    const { result } = renderHook(() => useCopyToClipboard());

    await act(async () => {
      const success = await result.current.copy('test-text');
      expect(success).toBe(false);
    });

    expect(result.current.isCopied).toBe(false);
    expect(result.current.error).toBe('Clipboard error');
  });

  it('should handle missing clipboard API', async () => {
    Object.defineProperty(navigator, 'clipboard', {
      value: undefined,
      writable: true,
      configurable: true,
    });

    const { result } = renderHook(() => useCopyToClipboard());

    await act(async () => {
      const success = await result.current.copy('test-text');
      expect(success).toBe(false);
    });

    expect(result.current.isCopied).toBe(false);
    expect(result.current.error).toBe('Clipboard API not available');
  });

  it('should reset state when reset is called', async () => {
    const { result } = renderHook(() => useCopyToClipboard());

    // First, copy something
    await act(async () => {
      await result.current.copy('test-text');
    });

    expect(result.current.isCopied).toBe(true);

    // Reset
    act(() => {
      result.current.reset();
    });

    expect(result.current.isCopied).toBe(false);
    expect(result.current.error).toBeNull();
  });

  it('should clear previous timeout on new copy', async () => {
    const { result } = renderHook(() => useCopyToClipboard(2000));

    // First copy
    await act(async () => {
      await result.current.copy('first-text');
    });

    expect(result.current.isCopied).toBe(true);

    // Advance partially
    act(() => {
      vi.advanceTimersByTime(1000);
    });

    // Second copy before first timeout
    await act(async () => {
      await result.current.copy('second-text');
    });

    expect(result.current.isCopied).toBe(true);

    // Advance by original remaining time (1000ms) - should still be copied
    act(() => {
      vi.advanceTimersByTime(1000);
    });

    // Should still be copied because new timeout starts from second copy
    expect(result.current.isCopied).toBe(true);

    // Advance by remaining new timeout (1000ms)
    act(() => {
      vi.advanceTimersByTime(1000);
    });

    expect(result.current.isCopied).toBe(false);
  });
});
