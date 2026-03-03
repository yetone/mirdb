/**
 * Clipboard Utility Tests
 * Owner: Scenario 7 - Copy Short URL Functionality
 *
 * Test cases:
 * 1. URL is copied to clipboard via Clipboard API
 * 2. Toast notification confirms successful copy (integration - tested in GuestShortener)
 * 3. Copy button changes state temporarily to indicate success
 * 4. Fallback copy mechanism when Clipboard API is not available
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { renderHook, act, waitFor } from '@testing-library/react'
import {
  copyToClipboard,
  useCopyToClipboard,
  isClipboardApiAvailable,
  type CopyResult
} from '../../src/utils/clipboard'

describe('Clipboard Utilities', () => {
  // Store original navigator.clipboard
  const originalClipboard = navigator.clipboard

  beforeEach(() => {
    vi.clearAllMocks()
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
    // Restore original clipboard
    Object.defineProperty(navigator, 'clipboard', {
      value: originalClipboard,
      writable: true,
      configurable: true
    })
  })

  /**
   * Test Case 1: URL is copied to clipboard via Clipboard API
   * Type: Unit
   */
  describe('Test Case 1: Copy to clipboard via Clipboard API', () => {
    it('should copy text to clipboard using Clipboard API', async () => {
      const mockWriteText = vi.fn().mockResolvedValue(undefined)
      Object.defineProperty(navigator, 'clipboard', {
        value: { writeText: mockWriteText },
        writable: true,
        configurable: true
      })

      const testUrl = 'https://example.com/r/abc123'
      const result = await copyToClipboard(testUrl)

      expect(mockWriteText).toHaveBeenCalledWith(testUrl)
      expect(result.success).toBe(true)
      expect(result.error).toBeUndefined()
    })

    it('should return success true when clipboard write succeeds', async () => {
      const mockWriteText = vi.fn().mockResolvedValue(undefined)
      Object.defineProperty(navigator, 'clipboard', {
        value: { writeText: mockWriteText },
        writable: true,
        configurable: true
      })

      const result = await copyToClipboard('http://localhost:5173/r/test123')

      expect(result).toEqual({ success: true })
    })

    it('should detect Clipboard API availability correctly', () => {
      // When clipboard is available
      const mockWriteText = vi.fn()
      Object.defineProperty(navigator, 'clipboard', {
        value: { writeText: mockWriteText },
        writable: true,
        configurable: true
      })

      expect(isClipboardApiAvailable()).toBe(true)
    })

    it('should handle short URLs with special characters', async () => {
      const mockWriteText = vi.fn().mockResolvedValue(undefined)
      Object.defineProperty(navigator, 'clipboard', {
        value: { writeText: mockWriteText },
        writable: true,
        configurable: true
      })

      const urlWithSpecialChars = 'https://example.com/r/test-123_abc'
      const result = await copyToClipboard(urlWithSpecialChars)

      expect(mockWriteText).toHaveBeenCalledWith(urlWithSpecialChars)
      expect(result.success).toBe(true)
    })
  })

  /**
   * Test Case 3: Copy button changes state temporarily to indicate success
   * Type: Unit
   */
  describe('Test Case 3: Copied state changes temporarily', () => {
    it('should set copied state to true after successful copy', async () => {
      const mockWriteText = vi.fn().mockResolvedValue(undefined)
      Object.defineProperty(navigator, 'clipboard', {
        value: { writeText: mockWriteText },
        writable: true,
        configurable: true
      })

      const { result } = renderHook(() => useCopyToClipboard())

      expect(result.current.copied).toBe(false)

      await act(async () => {
        await result.current.copy('https://example.com/r/abc123')
      })

      expect(result.current.copied).toBe(true)
    })

    it('should reset copied state after default delay (2000ms)', async () => {
      const mockWriteText = vi.fn().mockResolvedValue(undefined)
      Object.defineProperty(navigator, 'clipboard', {
        value: { writeText: mockWriteText },
        writable: true,
        configurable: true
      })

      const { result } = renderHook(() => useCopyToClipboard())

      await act(async () => {
        await result.current.copy('https://example.com/r/abc123')
      })

      expect(result.current.copied).toBe(true)

      // Fast-forward time by 2000ms
      await act(async () => {
        vi.advanceTimersByTime(2000)
      })

      expect(result.current.copied).toBe(false)
    })

    it('should reset copied state after custom delay', async () => {
      const mockWriteText = vi.fn().mockResolvedValue(undefined)
      Object.defineProperty(navigator, 'clipboard', {
        value: { writeText: mockWriteText },
        writable: true,
        configurable: true
      })

      const { result } = renderHook(() => useCopyToClipboard({ resetDelay: 1000 }))

      await act(async () => {
        await result.current.copy('https://example.com/r/abc123')
      })

      expect(result.current.copied).toBe(true)

      // Advance 500ms - should still be copied
      await act(async () => {
        vi.advanceTimersByTime(500)
      })
      expect(result.current.copied).toBe(true)

      // Advance another 500ms (total 1000ms) - should reset
      await act(async () => {
        vi.advanceTimersByTime(500)
      })
      expect(result.current.copied).toBe(false)
    })

    it('should call onSuccess callback after successful copy', async () => {
      const mockWriteText = vi.fn().mockResolvedValue(undefined)
      Object.defineProperty(navigator, 'clipboard', {
        value: { writeText: mockWriteText },
        writable: true,
        configurable: true
      })

      const onSuccess = vi.fn()
      const { result } = renderHook(() => useCopyToClipboard({ onSuccess }))

      const testUrl = 'https://example.com/r/abc123'
      await act(async () => {
        await result.current.copy(testUrl)
      })

      expect(onSuccess).toHaveBeenCalledWith(testUrl)
    })

    it('should return true from copy function on success', async () => {
      const mockWriteText = vi.fn().mockResolvedValue(undefined)
      Object.defineProperty(navigator, 'clipboard', {
        value: { writeText: mockWriteText },
        writable: true,
        configurable: true
      })

      const { result } = renderHook(() => useCopyToClipboard())

      let copyResult: boolean = false
      await act(async () => {
        copyResult = await result.current.copy('https://example.com/r/abc123')
      })

      expect(copyResult).toBe(true)
    })

    it('should allow manual reset of copied state', async () => {
      const mockWriteText = vi.fn().mockResolvedValue(undefined)
      Object.defineProperty(navigator, 'clipboard', {
        value: { writeText: mockWriteText },
        writable: true,
        configurable: true
      })

      const { result } = renderHook(() => useCopyToClipboard())

      await act(async () => {
        await result.current.copy('https://example.com/r/abc123')
      })

      expect(result.current.copied).toBe(true)

      act(() => {
        result.current.reset()
      })

      expect(result.current.copied).toBe(false)
    })
  })

  /**
   * Test Case 4: Fallback copy mechanism when Clipboard API is not available
   * Type: Unit
   */
  describe('Test Case 4: Fallback when Clipboard API unavailable', () => {
    it('should detect when Clipboard API is not available', () => {
      Object.defineProperty(navigator, 'clipboard', {
        value: undefined,
        writable: true,
        configurable: true
      })

      expect(isClipboardApiAvailable()).toBe(false)
    })

    it('should use fallback when Clipboard API is not available', async () => {
      // Remove clipboard API
      Object.defineProperty(navigator, 'clipboard', {
        value: undefined,
        writable: true,
        configurable: true
      })

      // Mock document methods for fallback
      const mockExecCommand = vi.fn().mockReturnValue(true)
      document.execCommand = mockExecCommand

      // Track createElement to verify textarea is created
      const originalCreateElement = document.createElement.bind(document)
      const createdElements: HTMLElement[] = []
      vi.spyOn(document, 'createElement').mockImplementation((tagName: string) => {
        const element = originalCreateElement(tagName)
        createdElements.push(element)
        return element
      })

      const result = await copyToClipboard('https://example.com/r/fallback123')

      expect(mockExecCommand).toHaveBeenCalledWith('copy')
      expect(result.success).toBe(true)

      // Verify a textarea was created for the fallback
      const textarea = createdElements.find(el => el.tagName === 'TEXTAREA')
      expect(textarea).toBeDefined()
    })

    it('should fallback when Clipboard API throws an error', async () => {
      const mockWriteText = vi.fn().mockRejectedValue(new Error('Permission denied'))
      Object.defineProperty(navigator, 'clipboard', {
        value: { writeText: mockWriteText },
        writable: true,
        configurable: true
      })

      // Mock fallback
      const mockExecCommand = vi.fn().mockReturnValue(true)
      document.execCommand = mockExecCommand

      const result = await copyToClipboard('https://example.com/r/abc123')

      expect(mockWriteText).toHaveBeenCalled()
      expect(mockExecCommand).toHaveBeenCalledWith('copy')
      expect(result.success).toBe(true)
    })

    it('should set error state when both methods fail', async () => {
      // Remove clipboard API
      Object.defineProperty(navigator, 'clipboard', {
        value: undefined,
        writable: true,
        configurable: true
      })

      // Make fallback fail
      const mockExecCommand = vi.fn().mockReturnValue(false)
      document.execCommand = mockExecCommand

      const { result } = renderHook(() => useCopyToClipboard())

      await act(async () => {
        await result.current.copy('https://example.com/r/abc123')
      })

      expect(result.current.copied).toBe(false)
      expect(result.current.error).toBeTruthy()
    })

    it('should call onError callback when copy fails', async () => {
      // Remove clipboard API
      Object.defineProperty(navigator, 'clipboard', {
        value: undefined,
        writable: true,
        configurable: true
      })

      // Make fallback fail
      document.execCommand = vi.fn().mockReturnValue(false)

      const onError = vi.fn()
      const { result } = renderHook(() => useCopyToClipboard({ onError }))

      await act(async () => {
        await result.current.copy('https://example.com/r/abc123')
      })

      expect(onError).toHaveBeenCalled()
    })

    it('should return false from copy function on failure', async () => {
      // Remove clipboard API
      Object.defineProperty(navigator, 'clipboard', {
        value: undefined,
        writable: true,
        configurable: true
      })

      // Make fallback fail
      document.execCommand = vi.fn().mockReturnValue(false)

      const { result } = renderHook(() => useCopyToClipboard())

      let copyResult: boolean = true
      await act(async () => {
        copyResult = await result.current.copy('https://example.com/r/abc123')
      })

      expect(copyResult).toBe(false)
    })

    it('should return error message when fallback throws exception', async () => {
      // Remove clipboard API
      Object.defineProperty(navigator, 'clipboard', {
        value: undefined,
        writable: true,
        configurable: true
      })

      // Make fallback throw
      document.execCommand = vi.fn().mockImplementation(() => {
        throw new Error('execCommand not supported')
      })

      const result = await copyToClipboard('https://example.com/r/abc123')

      expect(result.success).toBe(false)
      expect(result.error).toContain('execCommand not supported')
    })

    it('should clear error state after reset delay', async () => {
      // Remove clipboard API
      Object.defineProperty(navigator, 'clipboard', {
        value: undefined,
        writable: true,
        configurable: true
      })

      // Make fallback fail
      document.execCommand = vi.fn().mockReturnValue(false)

      const { result } = renderHook(() => useCopyToClipboard({ resetDelay: 1000 }))

      await act(async () => {
        await result.current.copy('https://example.com/r/abc123')
      })

      expect(result.current.error).toBeTruthy()

      // Advance time
      await act(async () => {
        vi.advanceTimersByTime(1000)
      })

      expect(result.current.error).toBeNull()
    })
  })

  /**
   * Additional edge case tests
   */
  describe('Edge Cases', () => {
    it('should handle empty string', async () => {
      const mockWriteText = vi.fn().mockResolvedValue(undefined)
      Object.defineProperty(navigator, 'clipboard', {
        value: { writeText: mockWriteText },
        writable: true,
        configurable: true
      })

      const result = await copyToClipboard('')

      expect(mockWriteText).toHaveBeenCalledWith('')
      expect(result.success).toBe(true)
    })

    it('should handle rapid consecutive copies', async () => {
      const mockWriteText = vi.fn().mockResolvedValue(undefined)
      Object.defineProperty(navigator, 'clipboard', {
        value: { writeText: mockWriteText },
        writable: true,
        configurable: true
      })

      const { result } = renderHook(() => useCopyToClipboard({ resetDelay: 2000 }))

      // First copy
      await act(async () => {
        await result.current.copy('https://example.com/r/first')
      })
      expect(result.current.copied).toBe(true)

      // Advance 1000ms (half the delay)
      await act(async () => {
        vi.advanceTimersByTime(1000)
      })

      // Second copy - should reset the timer
      await act(async () => {
        await result.current.copy('https://example.com/r/second')
      })
      expect(result.current.copied).toBe(true)

      // Advance another 1000ms (should still be copied due to timer reset)
      await act(async () => {
        vi.advanceTimersByTime(1000)
      })
      expect(result.current.copied).toBe(true)

      // Advance final 1000ms (total 2000ms from second copy)
      await act(async () => {
        vi.advanceTimersByTime(1000)
      })
      expect(result.current.copied).toBe(false)
    })

    it('should handle long URLs', async () => {
      const mockWriteText = vi.fn().mockResolvedValue(undefined)
      Object.defineProperty(navigator, 'clipboard', {
        value: { writeText: mockWriteText },
        writable: true,
        configurable: true
      })

      const longUrl = 'https://example.com/r/' + 'a'.repeat(1000)
      const result = await copyToClipboard(longUrl)

      expect(mockWriteText).toHaveBeenCalledWith(longUrl)
      expect(result.success).toBe(true)
    })
  })
})
