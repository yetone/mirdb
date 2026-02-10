/**
 * Unit tests for useCopyToClipboard hook
 * Owner: Scenario 4 - Getting Started Section
 */

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
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.clearAllMocks();
  });

  it('should initialize with idle state', () => {
    const { result } = renderHook(() => useCopyToClipboard());

    expect(result.current.state).toBe('idle');
    expect(typeof result.current.copy).toBe('function');
    expect(typeof result.current.reset).toBe('function');
  });

  it('should set state to success when copy succeeds', async () => {
    mockWriteText.mockResolvedValue(undefined);

    const { result } = renderHook(() => useCopyToClipboard());

    await act(async () => {
      await result.current.copy('test text');
    });

    expect(result.current.state).toBe('success');
    expect(mockWriteText).toHaveBeenCalledWith('test text');
  });

  it('should set state to error when copy fails', async () => {
    mockWriteText.mockRejectedValue(new Error('Copy failed'));

    const { result } = renderHook(() => useCopyToClipboard());

    await act(async () => {
      await result.current.copy('test text');
    });

    expect(result.current.state).toBe('error');
  });

  it('should auto-reset to idle after default delay', async () => {
    mockWriteText.mockResolvedValue(undefined);

    const { result } = renderHook(() => useCopyToClipboard());

    await act(async () => {
      await result.current.copy('test text');
    });

    expect(result.current.state).toBe('success');

    act(() => {
      vi.advanceTimersByTime(2000);
    });

    expect(result.current.state).toBe('idle');
  });

  it('should auto-reset to idle after custom delay', async () => {
    mockWriteText.mockResolvedValue(undefined);

    const { result } = renderHook(() => useCopyToClipboard(1000));

    await act(async () => {
      await result.current.copy('test text');
    });

    expect(result.current.state).toBe('success');

    act(() => {
      vi.advanceTimersByTime(1000);
    });

    expect(result.current.state).toBe('idle');
  });

  it('should manually reset state when reset is called', async () => {
    mockWriteText.mockResolvedValue(undefined);

    const { result } = renderHook(() => useCopyToClipboard());

    await act(async () => {
      await result.current.copy('test text');
    });

    expect(result.current.state).toBe('success');

    act(() => {
      result.current.reset();
    });

    expect(result.current.state).toBe('idle');
  });

  it('should auto-reset to idle after error state', async () => {
    mockWriteText.mockRejectedValue(new Error('Copy failed'));

    const { result } = renderHook(() => useCopyToClipboard());

    await act(async () => {
      await result.current.copy('test text');
    });

    expect(result.current.state).toBe('error');

    act(() => {
      vi.advanceTimersByTime(2000);
    });

    expect(result.current.state).toBe('idle');
  });
});
