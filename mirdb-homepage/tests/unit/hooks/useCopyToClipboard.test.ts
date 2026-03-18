/**
 * useCopyToClipboard Hook Tests.
 * Owner: Scenario 3 - Usage Examples and Code Blocks
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { useCopyToClipboard } from '../../../src/hooks/useCopyToClipboard';

describe('useCopyToClipboard', () => {
  const originalNavigator = global.navigator;
  const originalExecCommand = document.execCommand;

  beforeEach(() => {
    vi.resetAllMocks();
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    Object.defineProperty(global, 'navigator', {
      value: originalNavigator,
      writable: true,
      configurable: true,
    });
    document.execCommand = originalExecCommand;
  });

  it('should return initial state with copied as false', () => {
    const mockWriteText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(global, 'navigator', {
      value: {
        clipboard: { writeText: mockWriteText },
      },
      writable: true,
      configurable: true,
    });

    const { result } = renderHook(() => useCopyToClipboard());

    expect(result.current.copied).toBe(false);
    expect(result.current.error).toBe(null);
    expect(typeof result.current.copy).toBe('function');
  });

  it('should set copied to true after successful copy', async () => {
    const mockWriteText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(global, 'navigator', {
      value: {
        clipboard: { writeText: mockWriteText },
      },
      writable: true,
      configurable: true,
    });

    const { result } = renderHook(() => useCopyToClipboard());

    await act(async () => {
      const success = await result.current.copy('hello world');
      expect(success).toBe(true);
    });

    expect(result.current.copied).toBe(true);
    expect(result.current.error).toBe(null);
  });

  it('should reset copied to false after timeout', async () => {
    const mockWriteText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(global, 'navigator', {
      value: {
        clipboard: { writeText: mockWriteText },
      },
      writable: true,
      configurable: true,
    });

    const { result } = renderHook(() => useCopyToClipboard(1000));

    await act(async () => {
      await result.current.copy('hello world');
    });

    expect(result.current.copied).toBe(true);

    act(() => {
      vi.advanceTimersByTime(1000);
    });

    expect(result.current.copied).toBe(false);
  });

  it('should set error on copy failure', async () => {
    const mockWriteText = vi.fn().mockRejectedValue(new Error('Failed'));
    Object.defineProperty(global, 'navigator', {
      value: {
        clipboard: { writeText: mockWriteText },
      },
      writable: true,
      configurable: true,
    });

    // Mock execCommand for fallback failure
    document.execCommand = vi.fn().mockReturnValue(false);

    const { result } = renderHook(() => useCopyToClipboard());

    await act(async () => {
      const success = await result.current.copy('hello world');
      expect(success).toBe(false);
    });

    expect(result.current.copied).toBe(false);
    expect(result.current.error).not.toBe(null);
  });
});
