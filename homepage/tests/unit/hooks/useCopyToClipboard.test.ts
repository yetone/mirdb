import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, act, waitFor } from '@testing-library/react';
import { useCopyToClipboard } from '../../../src/hooks/useCopyToClipboard';

describe('useCopyToClipboard', () => {
  const mockWriteText = vi.fn();

  beforeEach(() => {
    vi.useFakeTimers();
    Object.assign(navigator, {
      clipboard: {
        writeText: mockWriteText,
      },
    });
    mockWriteText.mockResolvedValue(undefined);
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.clearAllMocks();
  });

  it('should return copy function and initial copied state as false', () => {
    const { result } = renderHook(() => useCopyToClipboard());

    expect(result.current.copy).toBeDefined();
    expect(typeof result.current.copy).toBe('function');
    expect(result.current.copied).toBe(false);
  });

  it('copy function returns Promise<boolean> and resolves to true on success', async () => {
    const { result } = renderHook(() => useCopyToClipboard());

    let copyResult: boolean;
    await act(async () => {
      copyResult = await result.current.copy('test text');
    });

    expect(copyResult!).toBe(true);
    expect(mockWriteText).toHaveBeenCalledWith('test text');
  });

  it('sets copied state to true after successful copy', async () => {
    const { result } = renderHook(() => useCopyToClipboard());

    expect(result.current.copied).toBe(false);

    await act(async () => {
      await result.current.copy('test text');
    });

    expect(result.current.copied).toBe(true);
  });

  it('resets copied state to false after default timeout (2 seconds)', async () => {
    const { result } = renderHook(() => useCopyToClipboard());

    await act(async () => {
      await result.current.copy('test text');
    });

    expect(result.current.copied).toBe(true);

    await act(async () => {
      vi.advanceTimersByTime(2000);
    });

    expect(result.current.copied).toBe(false);
  });

  it('resets copied state to false after custom timeout', async () => {
    const { result } = renderHook(() => useCopyToClipboard(1000));

    await act(async () => {
      await result.current.copy('test text');
    });

    expect(result.current.copied).toBe(true);

    await act(async () => {
      vi.advanceTimersByTime(1000);
    });

    expect(result.current.copied).toBe(false);
  });

  it('returns false when Clipboard API is not available', async () => {
    // Remove clipboard API
    Object.assign(navigator, {
      clipboard: undefined,
    });

    const { result } = renderHook(() => useCopyToClipboard());

    let copyResult: boolean;
    await act(async () => {
      copyResult = await result.current.copy('test text');
    });

    expect(copyResult!).toBe(false);
    expect(result.current.copied).toBe(false);
  });

  it('returns false and keeps copied false when clipboard write fails', async () => {
    mockWriteText.mockRejectedValue(new Error('Permission denied'));

    const { result } = renderHook(() => useCopyToClipboard());

    let copyResult: boolean;
    await act(async () => {
      copyResult = await result.current.copy('test text');
    });

    expect(copyResult!).toBe(false);
    expect(result.current.copied).toBe(false);
  });

  it('clears previous timeout when copy is called multiple times', async () => {
    const { result } = renderHook(() => useCopyToClipboard(2000));

    await act(async () => {
      await result.current.copy('first');
    });

    expect(result.current.copied).toBe(true);

    // Advance time by 1 second
    await act(async () => {
      vi.advanceTimersByTime(1000);
    });

    // Copy again before timeout completes
    await act(async () => {
      await result.current.copy('second');
    });

    expect(result.current.copied).toBe(true);

    // Advance another 1.5 seconds (total 2.5s from first copy, 1.5s from second)
    await act(async () => {
      vi.advanceTimersByTime(1500);
    });

    // Should still be true since we copied again
    expect(result.current.copied).toBe(true);

    // Advance past the second copy's timeout
    await act(async () => {
      vi.advanceTimersByTime(500);
    });

    expect(result.current.copied).toBe(false);
  });
});
