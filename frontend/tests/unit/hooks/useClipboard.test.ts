/**
 * Unit Tests for useClipboard Hook
 * Owner: Scenario 5 - Copy to Clipboard Functionality
 *
 * Tests:
 * 1. navigator.clipboard.writeText is called with the short URL
 * 2. copied state changes to true after successful copy
 * 3. copied state reverts to false after 2 seconds
 * 4. Error handling when clipboard API fails
 */

import { renderHook, act, waitFor } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { useClipboard } from '@/hooks/useClipboard'

describe('useClipboard', () => {
  const mockWriteText = vi.fn()
  const originalClipboard = navigator.clipboard

  beforeEach(() => {
    vi.clearAllMocks()
    vi.useFakeTimers()

    // Mock clipboard API
    Object.defineProperty(navigator, 'clipboard', {
      value: {
        writeText: mockWriteText,
      },
      writable: true,
      configurable: true,
    })
  })

  afterEach(() => {
    vi.useRealTimers()

    // Restore original clipboard
    Object.defineProperty(navigator, 'clipboard', {
      value: originalClipboard,
      writable: true,
      configurable: true,
    })
  })

  it('should call navigator.clipboard.writeText with the provided text', async () => {
    const shortUrl = 'http://localhost/r/abc123'
    mockWriteText.mockResolvedValueOnce(undefined)

    const { result } = renderHook(() => useClipboard())

    await act(async () => {
      await result.current.copy(shortUrl)
    })

    expect(mockWriteText).toHaveBeenCalledTimes(1)
    expect(mockWriteText).toHaveBeenCalledWith(shortUrl)
  })

  it('should set copied state to true after successful copy', async () => {
    mockWriteText.mockResolvedValueOnce(undefined)

    const { result } = renderHook(() => useClipboard())

    expect(result.current.copied).toBe(false)

    await act(async () => {
      await result.current.copy('test-url')
    })

    expect(result.current.copied).toBe(true)
    expect(result.current.error).toBe(null)
  })

  it('should revert copied state to false after 2 seconds', async () => {
    mockWriteText.mockResolvedValueOnce(undefined)

    const { result } = renderHook(() => useClipboard())

    await act(async () => {
      await result.current.copy('test-url')
    })

    expect(result.current.copied).toBe(true)

    // Advance timers by 2 seconds
    act(() => {
      vi.advanceTimersByTime(2000)
    })

    expect(result.current.copied).toBe(false)
  })

  it('should set error state when clipboard API fails', async () => {
    const errorMessage = 'Permission denied'
    mockWriteText.mockRejectedValueOnce(new Error(errorMessage))

    const { result } = renderHook(() => useClipboard())

    await act(async () => {
      const success = await result.current.copy('test-url')
      expect(success).toBe(false)
    })

    expect(result.current.copied).toBe(false)
    expect(result.current.error).toBeInstanceOf(Error)
    expect(result.current.error?.message).toBe(errorMessage)
  })

  it('should handle clipboard API not available', async () => {
    // Remove clipboard API
    Object.defineProperty(navigator, 'clipboard', {
      value: undefined,
      writable: true,
      configurable: true,
    })

    const { result } = renderHook(() => useClipboard())

    await act(async () => {
      const success = await result.current.copy('test-url')
      expect(success).toBe(false)
    })

    expect(result.current.copied).toBe(false)
    expect(result.current.error).toBeInstanceOf(Error)
    expect(result.current.error?.message).toBe('Clipboard API not available')
  })

  it('should return true on successful copy', async () => {
    mockWriteText.mockResolvedValueOnce(undefined)

    const { result } = renderHook(() => useClipboard())

    let success: boolean = false
    await act(async () => {
      success = await result.current.copy('test-url')
    })

    expect(success).toBe(true)
  })

  it('should clear previous error on new copy attempt', async () => {
    // First copy fails
    mockWriteText.mockRejectedValueOnce(new Error('First error'))

    const { result } = renderHook(() => useClipboard())

    await act(async () => {
      await result.current.copy('test-url')
    })

    expect(result.current.error).not.toBe(null)

    // Second copy succeeds
    mockWriteText.mockResolvedValueOnce(undefined)

    await act(async () => {
      await result.current.copy('test-url')
    })

    expect(result.current.error).toBe(null)
    expect(result.current.copied).toBe(true)
  })

  it('should clear previous timeout on rapid successive copies', async () => {
    mockWriteText.mockResolvedValue(undefined)

    const { result } = renderHook(() => useClipboard())

    // First copy
    await act(async () => {
      await result.current.copy('test-url-1')
    })

    expect(result.current.copied).toBe(true)

    // Advance 1 second
    act(() => {
      vi.advanceTimersByTime(1000)
    })

    // Second copy before timeout
    await act(async () => {
      await result.current.copy('test-url-2')
    })

    expect(result.current.copied).toBe(true)

    // Advance another 1.5 seconds (2.5s total from first copy, 1.5s from second)
    act(() => {
      vi.advanceTimersByTime(1500)
    })

    // Should still be copied because second copy restarted the timer
    expect(result.current.copied).toBe(true)

    // Advance another 0.5 seconds (2s from second copy)
    act(() => {
      vi.advanceTimersByTime(500)
    })

    // Now it should be false
    expect(result.current.copied).toBe(false)
  })
})
