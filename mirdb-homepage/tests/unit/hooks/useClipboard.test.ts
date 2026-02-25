import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, act, waitFor } from '@testing-library/react';
import { useClipboard } from '../../../src/hooks/useClipboard';

describe('useClipboard', () => {
  const mockClipboard = {
    writeText: vi.fn(),
  };

  beforeEach(() => {
    vi.useFakeTimers();
    Object.defineProperty(navigator, 'clipboard', {
      value: mockClipboard,
      writable: true,
      configurable: true,
    });
    mockClipboard.writeText.mockResolvedValue(undefined);
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.clearAllMocks();
  });

  it('should initialize with copied as false', () => {
    const { result } = renderHook(() => useClipboard());
    expect(result.current.copied).toBe(false);
  });

  it('should copy text to clipboard and set copied to true', async () => {
    const { result } = renderHook(() => useClipboard());
    const textToCopy = 'Hello, World!';

    let copyResult: boolean;
    await act(async () => {
      copyResult = await result.current.copy(textToCopy);
    });

    expect(mockClipboard.writeText).toHaveBeenCalledWith(textToCopy);
    expect(copyResult!).toBe(true);
    expect(result.current.copied).toBe(true);
  });

  it('should reset copied to false after delay', async () => {
    const { result } = renderHook(() => useClipboard(1000));

    await act(async () => {
      await result.current.copy('test');
    });

    expect(result.current.copied).toBe(true);

    act(() => {
      vi.advanceTimersByTime(1000);
    });

    expect(result.current.copied).toBe(false);
  });

  it('should use default reset delay of 2000ms', async () => {
    const { result } = renderHook(() => useClipboard());

    await act(async () => {
      await result.current.copy('test');
    });

    expect(result.current.copied).toBe(true);

    act(() => {
      vi.advanceTimersByTime(1999);
    });

    expect(result.current.copied).toBe(true);

    act(() => {
      vi.advanceTimersByTime(1);
    });

    expect(result.current.copied).toBe(false);
  });

  it('should return false when clipboard API is not available', async () => {
    Object.defineProperty(navigator, 'clipboard', {
      value: undefined,
      writable: true,
      configurable: true,
    });

    const { result } = renderHook(() => useClipboard());

    let copyResult: boolean;
    await act(async () => {
      copyResult = await result.current.copy('test');
    });

    expect(copyResult!).toBe(false);
    expect(result.current.copied).toBe(false);
  });

  it('should return false and set copied to false when copy fails', async () => {
    mockClipboard.writeText.mockRejectedValue(new Error('Copy failed'));

    const { result } = renderHook(() => useClipboard());

    let copyResult: boolean;
    await act(async () => {
      copyResult = await result.current.copy('test');
    });

    expect(copyResult!).toBe(false);
    expect(result.current.copied).toBe(false);
  });
});
