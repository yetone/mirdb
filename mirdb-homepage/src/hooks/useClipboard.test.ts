/**
 * useClipboard Hook Tests.
 * Owner: Scenario 13 - Code Examples Copy Functionality
 *
 * Tests:
 * - Hook initializes with correct default state
 * - copy function uses navigator.clipboard API when available
 * - copy function falls back to document.execCommand when clipboard API unavailable
 * - copied state becomes true after successful copy
 * - copied state resets after timeout
 * - error state is set when copy fails
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { renderHook, act, waitFor } from '@testing-library/react'
import { useClipboard } from './useClipboard'

describe('useClipboard Hook', () => {
  const originalClipboard = navigator.clipboard
  const originalExecCommand = document.execCommand

  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
    // Restore original clipboard
    Object.defineProperty(navigator, 'clipboard', {
      value: originalClipboard,
      writable: true,
      configurable: true,
    })
    document.execCommand = originalExecCommand
  })

  it('initializes with correct default state', () => {
    const { result } = renderHook(() => useClipboard())

    expect(result.current.copied).toBe(false)
    expect(result.current.error).toBe(null)
    expect(typeof result.current.copy).toBe('function')
  })

  it('copies text using navigator.clipboard API when available', async () => {
    const mockWriteText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', {
      value: { writeText: mockWriteText },
      writable: true,
      configurable: true,
    })

    const { result } = renderHook(() => useClipboard())

    await act(async () => {
      await result.current.copy('test text')
    })

    expect(mockWriteText).toHaveBeenCalledWith('test text')
    expect(result.current.copied).toBe(true)
    expect(result.current.error).toBe(null)
  })

  it('sets copied to true after successful copy', async () => {
    const mockWriteText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', {
      value: { writeText: mockWriteText },
      writable: true,
      configurable: true,
    })

    const { result } = renderHook(() => useClipboard())

    expect(result.current.copied).toBe(false)

    await act(async () => {
      await result.current.copy('hello world')
    })

    expect(result.current.copied).toBe(true)
  })

  it('resets copied state after timeout', async () => {
    const mockWriteText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', {
      value: { writeText: mockWriteText },
      writable: true,
      configurable: true,
    })

    const { result } = renderHook(() => useClipboard(1000))

    await act(async () => {
      await result.current.copy('test')
    })

    expect(result.current.copied).toBe(true)

    // Fast-forward time by 1000ms
    act(() => {
      vi.advanceTimersByTime(1000)
    })

    expect(result.current.copied).toBe(false)
  })

  it('uses default timeout of 2000ms', async () => {
    const mockWriteText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', {
      value: { writeText: mockWriteText },
      writable: true,
      configurable: true,
    })

    const { result } = renderHook(() => useClipboard())

    await act(async () => {
      await result.current.copy('test')
    })

    expect(result.current.copied).toBe(true)

    // After 1500ms, should still be true
    act(() => {
      vi.advanceTimersByTime(1500)
    })
    expect(result.current.copied).toBe(true)

    // After 2000ms total, should be false
    act(() => {
      vi.advanceTimersByTime(500)
    })
    expect(result.current.copied).toBe(false)
  })

  it('falls back to execCommand when clipboard API unavailable', async () => {
    // Remove clipboard API
    Object.defineProperty(navigator, 'clipboard', {
      value: undefined,
      writable: true,
      configurable: true,
    })

    const mockExecCommand = vi.fn().mockReturnValue(true)
    document.execCommand = mockExecCommand

    const { result } = renderHook(() => useClipboard())

    await act(async () => {
      await result.current.copy('fallback test')
    })

    expect(mockExecCommand).toHaveBeenCalledWith('copy')
    expect(result.current.copied).toBe(true)
  })

  it('sets error state when copy fails', async () => {
    const mockError = new Error('Copy failed')
    const mockWriteText = vi.fn().mockRejectedValue(mockError)
    Object.defineProperty(navigator, 'clipboard', {
      value: { writeText: mockWriteText },
      writable: true,
      configurable: true,
    })

    const { result } = renderHook(() => useClipboard())

    await act(async () => {
      await result.current.copy('test')
    })

    expect(result.current.error).toEqual(mockError)
    expect(result.current.copied).toBe(false)
  })

  it('clears previous error on successful copy', async () => {
    const mockError = new Error('Copy failed')
    const mockWriteText = vi.fn()
      .mockRejectedValueOnce(mockError)
      .mockResolvedValueOnce(undefined)

    Object.defineProperty(navigator, 'clipboard', {
      value: { writeText: mockWriteText },
      writable: true,
      configurable: true,
    })

    const { result } = renderHook(() => useClipboard())

    // First copy fails
    await act(async () => {
      await result.current.copy('test1')
    })
    expect(result.current.error).toEqual(mockError)

    // Second copy succeeds
    await act(async () => {
      await result.current.copy('test2')
    })
    expect(result.current.error).toBe(null)
    expect(result.current.copied).toBe(true)
  })

  it('returns consistent function reference for copy', () => {
    const { result, rerender } = renderHook(() => useClipboard())

    const firstCopy = result.current.copy
    rerender()
    const secondCopy = result.current.copy

    expect(firstCopy).toBe(secondCopy)
  })
})
