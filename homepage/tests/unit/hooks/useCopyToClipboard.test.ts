/**
 * Tests for useCopyToClipboard hook and copyToClipboard utility.
 * Owner: Scenario 4 - Quick Start Section with Code Examples
 */
import { renderHook, act, waitFor } from '@testing-library/react';
import { useCopyToClipboard, copyToClipboard } from '@/hooks/useCopyToClipboard';

// Mock clipboard API
const mockWriteText = jest.fn();

describe('useCopyToClipboard', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    Object.assign(navigator, {
      clipboard: {
        writeText: mockWriteText,
      },
    });
  });

  it('should initialize with default values', () => {
    const { result } = renderHook(() => useCopyToClipboard());

    expect(result.current.copied).toBe(false);
    expect(result.current.error).toBeNull();
    expect(typeof result.current.copy).toBe('function');
  });

  it('should copy text to clipboard successfully', async () => {
    mockWriteText.mockResolvedValueOnce(undefined);

    const { result } = renderHook(() => useCopyToClipboard());

    let success: boolean = false;
    await act(async () => {
      success = await result.current.copy('test text');
    });

    expect(success).toBe(true);
    expect(mockWriteText).toHaveBeenCalledWith('test text');
    expect(result.current.copied).toBe(true);
    expect(result.current.error).toBeNull();
  });

  it('should handle copy failure', async () => {
    const error = new Error('Copy failed');
    mockWriteText.mockRejectedValueOnce(error);

    const { result } = renderHook(() => useCopyToClipboard());

    let success: boolean = true;
    await act(async () => {
      success = await result.current.copy('test text');
    });

    expect(success).toBe(false);
    expect(result.current.copied).toBe(false);
    expect(result.current.error).toEqual(error);
  });

  it('should reset copied state after 2 seconds', async () => {
    jest.useFakeTimers();
    mockWriteText.mockResolvedValueOnce(undefined);

    const { result } = renderHook(() => useCopyToClipboard());

    await act(async () => {
      await result.current.copy('test text');
    });

    expect(result.current.copied).toBe(true);

    act(() => {
      jest.advanceTimersByTime(2000);
    });

    expect(result.current.copied).toBe(false);

    jest.useRealTimers();
  });

  it('should handle missing clipboard API', async () => {
    Object.assign(navigator, {
      clipboard: undefined,
    });

    const { result } = renderHook(() => useCopyToClipboard());

    let success: boolean = true;
    await act(async () => {
      success = await result.current.copy('test text');
    });

    expect(success).toBe(false);
    expect(result.current.error).toEqual(new Error('Clipboard API not available'));
  });
});

describe('copyToClipboard utility function', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    Object.assign(navigator, {
      clipboard: {
        writeText: mockWriteText,
      },
    });
  });

  it('should copy text and return true on success', async () => {
    mockWriteText.mockResolvedValueOnce(undefined);

    const success = await copyToClipboard('test text');

    expect(success).toBe(true);
    expect(mockWriteText).toHaveBeenCalledWith('test text');
  });

  it('should return false on failure', async () => {
    mockWriteText.mockRejectedValueOnce(new Error('Copy failed'));

    const success = await copyToClipboard('test text');

    expect(success).toBe(false);
  });

  it('should return false when clipboard API is unavailable', async () => {
    Object.assign(navigator, {
      clipboard: undefined,
    });

    const success = await copyToClipboard('test text');

    expect(success).toBe(false);
  });
});
