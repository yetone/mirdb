import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { useCopyToClipboard } from '@/hooks/useCopyToClipboard';

describe('useCopyToClipboard', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    Object.assign(navigator, {
      clipboard: {
        writeText: vi.fn().mockResolvedValue(undefined),
      },
    });
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('should initialize with copied as false and no error', () => {
    const { result } = renderHook(() => useCopyToClipboard());

    expect(result.current.copied).toBe(false);
    expect(result.current.error).toBeNull();
    expect(typeof result.current.copy).toBe('function');
  });

  it('should copy text to clipboard and set copied to true', async () => {
    const { result } = renderHook(() => useCopyToClipboard());

    await act(async () => {
      await result.current.copy('test text');
    });

    expect(navigator.clipboard.writeText).toHaveBeenCalledWith('test text');
    expect(result.current.copied).toBe(true);
    expect(result.current.error).toBeNull();
  });

  it('should return true on successful copy', async () => {
    const { result } = renderHook(() => useCopyToClipboard());

    let success: boolean = false;
    await act(async () => {
      success = await result.current.copy('test text');
    });

    expect(success).toBe(true);
  });

  it('should reset copied state after timeout', async () => {
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

  it('should handle clipboard API errors', async () => {
    const error = new Error('Clipboard access denied');
    vi.spyOn(navigator.clipboard, 'writeText').mockRejectedValue(error);

    const { result } = renderHook(() => useCopyToClipboard());

    let success: boolean = true;
    await act(async () => {
      success = await result.current.copy('test text');
    });

    expect(success).toBe(false);
    expect(result.current.copied).toBe(false);
    expect(result.current.error).toEqual(error);
  });

  it('should clear previous error on successful copy', async () => {
    const error = new Error('Clipboard access denied');
    vi.spyOn(navigator.clipboard, 'writeText')
      .mockRejectedValueOnce(error)
      .mockResolvedValueOnce(undefined);

    const { result } = renderHook(() => useCopyToClipboard());

    await act(async () => {
      await result.current.copy('first attempt');
    });

    expect(result.current.error).toEqual(error);

    await act(async () => {
      await result.current.copy('second attempt');
    });

    expect(result.current.error).toBeNull();
    expect(result.current.copied).toBe(true);
  });

  it('should use default timeout of 2000ms', async () => {
    const { result } = renderHook(() => useCopyToClipboard());

    await act(async () => {
      await result.current.copy('test text');
    });

    expect(result.current.copied).toBe(true);

    await act(async () => {
      vi.advanceTimersByTime(1999);
    });

    expect(result.current.copied).toBe(true);

    await act(async () => {
      vi.advanceTimersByTime(1);
    });

    expect(result.current.copied).toBe(false);
  });
});
