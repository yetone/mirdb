/**
 * Unit tests for useCopyToClipboard hook.
 * Owner: Scenario 5 - Quick Start Section
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { renderHook, act, waitFor } from '@testing-library/react'
import { useCopyToClipboard } from '../../../src/hooks/useCopyToClipboard'

// Mock clipboard API
const mockClipboard = {
  writeText: vi.fn(),
}

const originalNavigator = global.navigator

beforeEach(() => {
  vi.clearAllMocks()
  vi.useFakeTimers()
  Object.defineProperty(global, 'navigator', {
    value: { clipboard: mockClipboard },
    writable: true,
  })
})

afterEach(() => {
  vi.useRealTimers()
  Object.defineProperty(global, 'navigator', {
    value: originalNavigator,
    writable: true,
  })
})

describe('useCopyToClipboard Hook', () => {
  it('should initialize with copied as false', () => {
    const { result } = renderHook(() => useCopyToClipboard())

    expect(result.current.copied).toBe(false)
  })

  it('should provide copy function', () => {
    const { result } = renderHook(() => useCopyToClipboard())

    expect(typeof result.current.copy).toBe('function')
  })

  it('should copy text to clipboard successfully', async () => {
    mockClipboard.writeText.mockResolvedValue(undefined)
    const { result } = renderHook(() => useCopyToClipboard())

    let success: boolean | undefined
    await act(async () => {
      success = await result.current.copy('test text')
    })

    expect(success).toBe(true)
    expect(mockClipboard.writeText).toHaveBeenCalledWith('test text')
  })

  it('should set copied to true after successful copy', async () => {
    mockClipboard.writeText.mockResolvedValue(undefined)
    const { result } = renderHook(() => useCopyToClipboard())

    await act(async () => {
      await result.current.copy('test text')
    })

    expect(result.current.copied).toBe(true)
  })

  it('should reset copied to false after default delay (2000ms)', async () => {
    mockClipboard.writeText.mockResolvedValue(undefined)
    const { result } = renderHook(() => useCopyToClipboard())

    await act(async () => {
      await result.current.copy('test text')
    })

    expect(result.current.copied).toBe(true)

    act(() => {
      vi.advanceTimersByTime(2000)
    })

    expect(result.current.copied).toBe(false)
  })

  it('should reset copied to false after custom delay', async () => {
    mockClipboard.writeText.mockResolvedValue(undefined)
    const { result } = renderHook(() => useCopyToClipboard(1000))

    await act(async () => {
      await result.current.copy('test text')
    })

    expect(result.current.copied).toBe(true)

    act(() => {
      vi.advanceTimersByTime(999)
    })

    expect(result.current.copied).toBe(true)

    act(() => {
      vi.advanceTimersByTime(1)
    })

    expect(result.current.copied).toBe(false)
  })

  it('should return false when clipboard API is not available', async () => {
    Object.defineProperty(global, 'navigator', {
      value: {},
      writable: true,
    })

    const { result } = renderHook(() => useCopyToClipboard())

    let success: boolean | undefined
    await act(async () => {
      success = await result.current.copy('test text')
    })

    expect(success).toBe(false)
    expect(result.current.copied).toBe(false)
  })

  it('should return false and log error when copy fails', async () => {
    const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    mockClipboard.writeText.mockRejectedValue(new Error('Copy failed'))

    const { result } = renderHook(() => useCopyToClipboard())

    let success: boolean | undefined
    await act(async () => {
      success = await result.current.copy('test text')
    })

    expect(success).toBe(false)
    expect(result.current.copied).toBe(false)
    expect(consoleSpy).toHaveBeenCalled()

    consoleSpy.mockRestore()
  })

  it('should handle multiple consecutive copies', async () => {
    mockClipboard.writeText.mockResolvedValue(undefined)
    const { result } = renderHook(() => useCopyToClipboard())

    await act(async () => {
      await result.current.copy('first text')
    })

    expect(mockClipboard.writeText).toHaveBeenCalledWith('first text')

    await act(async () => {
      await result.current.copy('second text')
    })

    expect(mockClipboard.writeText).toHaveBeenCalledWith('second text')
    expect(result.current.copied).toBe(true)
  })
})
