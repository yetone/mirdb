/**
 * useCopyToClipboard Hook Tests
 * Owner: Scenario 4 - Quick Start Section
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { renderHook, act, waitFor } from '@testing-library/react'
import { useCopyToClipboard } from './useCopyToClipboard'

describe('useCopyToClipboard', () => {
  const mockWriteText = vi.fn()
  const originalClipboard = navigator.clipboard

  beforeEach(() => {
    vi.useFakeTimers()
    Object.defineProperty(navigator, 'clipboard', {
      value: {
        writeText: mockWriteText,
      },
      writable: true,
      configurable: true,
    })
    mockWriteText.mockResolvedValue(undefined)
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.clearAllMocks()
    Object.defineProperty(navigator, 'clipboard', {
      value: originalClipboard,
      writable: true,
      configurable: true,
    })
  })

  describe('Initial state', () => {
    it('returns copy function and copied state', () => {
      const { result } = renderHook(() => useCopyToClipboard())

      expect(result.current.copy).toBeDefined()
      expect(typeof result.current.copy).toBe('function')
      expect(result.current.copied).toBe(false)
      expect(result.current.error).toBeNull()
    })
  })

  describe('Copy functionality', () => {
    it('copies text to clipboard successfully', async () => {
      const { result } = renderHook(() => useCopyToClipboard())

      await act(async () => {
        const success = await result.current.copy('test text')
        expect(success).toBe(true)
      })

      expect(mockWriteText).toHaveBeenCalledWith('test text')
      expect(result.current.copied).toBe(true)
      expect(result.current.error).toBeNull()
    })

    it('sets copied to false after delay', async () => {
      const { result } = renderHook(() => useCopyToClipboard(1000))

      await act(async () => {
        await result.current.copy('test')
      })

      expect(result.current.copied).toBe(true)

      act(() => {
        vi.advanceTimersByTime(1000)
      })

      expect(result.current.copied).toBe(false)
    })

    it('uses default 2000ms reset delay', async () => {
      const { result } = renderHook(() => useCopyToClipboard())

      await act(async () => {
        await result.current.copy('test')
      })

      expect(result.current.copied).toBe(true)

      act(() => {
        vi.advanceTimersByTime(1999)
      })

      expect(result.current.copied).toBe(true)

      act(() => {
        vi.advanceTimersByTime(1)
      })

      expect(result.current.copied).toBe(false)
    })
  })

  describe('Error handling', () => {
    it('handles clipboard write errors', async () => {
      const clipboardError = new Error('Clipboard write failed')
      mockWriteText.mockRejectedValueOnce(clipboardError)

      const { result } = renderHook(() => useCopyToClipboard())

      await act(async () => {
        const success = await result.current.copy('test')
        expect(success).toBe(false)
      })

      expect(result.current.copied).toBe(false)
      expect(result.current.error).toEqual(clipboardError)
    })

    it('handles missing clipboard API', async () => {
      Object.defineProperty(navigator, 'clipboard', {
        value: undefined,
        writable: true,
        configurable: true,
      })

      const { result } = renderHook(() => useCopyToClipboard())

      await act(async () => {
        const success = await result.current.copy('test')
        expect(success).toBe(false)
      })

      expect(result.current.copied).toBe(false)
      expect(result.current.error?.message).toBe('Clipboard API not available')
    })

    it('handles non-Error exceptions', async () => {
      mockWriteText.mockRejectedValueOnce('string error')

      const { result } = renderHook(() => useCopyToClipboard())

      await act(async () => {
        const success = await result.current.copy('test')
        expect(success).toBe(false)
      })

      expect(result.current.error?.message).toBe('Failed to copy')
    })
  })

  describe('Multiple copies', () => {
    it('resets error on successful copy', async () => {
      mockWriteText.mockRejectedValueOnce(new Error('First error'))

      const { result } = renderHook(() => useCopyToClipboard())

      await act(async () => {
        await result.current.copy('test1')
      })

      expect(result.current.error).not.toBeNull()

      mockWriteText.mockResolvedValueOnce(undefined)

      await act(async () => {
        await result.current.copy('test2')
      })

      expect(result.current.error).toBeNull()
      expect(result.current.copied).toBe(true)
    })
  })
})
