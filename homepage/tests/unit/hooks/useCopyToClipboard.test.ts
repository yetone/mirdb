import { describe, it, expect, vi, beforeEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { useCopyToClipboard } from '../../../src/hooks/useCopyToClipboard';

describe('useCopyToClipboard', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should initialize with copied as false', () => {
    const { result } = renderHook(() => useCopyToClipboard());

    expect(result.current.copied).toBe(false);
    expect(result.current.error).toBeNull();
  });

  it('should return true and set copied state to true when copy succeeds', async () => {
    const writeTextMock = vi.fn().mockResolvedValue(undefined);
    Object.assign(navigator, {
      clipboard: {
        writeText: writeTextMock,
      },
    });

    const { result } = renderHook(() => useCopyToClipboard());

    let copyResult: boolean;
    await act(async () => {
      copyResult = await result.current.copy('test text');
    });

    expect(copyResult!).toBe(true);
    expect(result.current.copied).toBe(true);
    expect(writeTextMock).toHaveBeenCalledWith('test text');
  });

  it('should reset copied state after delay', async () => {
    const writeTextMock = vi.fn().mockResolvedValue(undefined);
    Object.assign(navigator, {
      clipboard: {
        writeText: writeTextMock,
      },
    });

    const { result } = renderHook(() => useCopyToClipboard(1000));

    await act(async () => {
      await result.current.copy('test text');
    });

    expect(result.current.copied).toBe(true);

    act(() => {
      vi.advanceTimersByTime(1000);
    });

    expect(result.current.copied).toBe(false);
  });

  it('should return false and set error when clipboard API is not available', async () => {
    // @ts-expect-error - Simulating missing clipboard API
    delete navigator.clipboard;
    Object.assign(navigator, { clipboard: undefined });

    const { result } = renderHook(() => useCopyToClipboard());

    let copyResult: boolean;
    await act(async () => {
      copyResult = await result.current.copy('test text');
    });

    expect(copyResult!).toBe(false);
    expect(result.current.copied).toBe(false);
    expect(result.current.error).toBeInstanceOf(Error);
    expect(result.current.error?.message).toBe('Clipboard API not available');
  });

  it('should handle clipboard write errors', async () => {
    const writeTextMock = vi.fn().mockRejectedValue(new Error('Write failed'));
    Object.assign(navigator, {
      clipboard: {
        writeText: writeTextMock,
      },
    });

    const { result } = renderHook(() => useCopyToClipboard());

    let copyResult: boolean;
    await act(async () => {
      copyResult = await result.current.copy('test text');
    });

    expect(copyResult!).toBe(false);
    expect(result.current.copied).toBe(false);
    expect(result.current.error).toBeInstanceOf(Error);
    expect(result.current.error?.message).toBe('Write failed');
  });

  it('should provide copy function that can be called multiple times', async () => {
    const writeTextMock = vi.fn().mockResolvedValue(undefined);
    Object.assign(navigator, {
      clipboard: {
        writeText: writeTextMock,
      },
    });

    const { result } = renderHook(() => useCopyToClipboard());

    await act(async () => {
      await result.current.copy('first');
    });
    expect(writeTextMock).toHaveBeenCalledWith('first');

    await act(async () => {
      await result.current.copy('second');
    });
    expect(writeTextMock).toHaveBeenCalledWith('second');
    expect(writeTextMock).toHaveBeenCalledTimes(2);
  });
});
