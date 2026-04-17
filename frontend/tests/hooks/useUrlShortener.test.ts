/**
 * useUrlShortener Hook Tests
 * Owner: Scenario 2 - Inline URL Shortening Demo
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { renderHook, act, waitFor } from '@testing-library/react'
import { useUrlShortener } from '../../src/hooks/useUrlShortener'
import * as api from '../../src/api'

// Mock the API module
vi.mock('../../src/api', () => ({
  shortenUrl: vi.fn(),
}))

// Mock clipboard API
const mockClipboard = {
  writeText: vi.fn().mockResolvedValue(undefined),
}
Object.defineProperty(navigator, 'clipboard', {
  value: mockClipboard,
  writable: true,
})

describe('useUrlShortener', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  describe('initial state', () => {
    it('should return initial state', () => {
      const { result } = renderHook(() => useUrlShortener())

      expect(result.current.isLoading).toBe(false)
      expect(result.current.error).toBeNull()
      expect(result.current.shortenedUrl).toBeNull()
      expect(result.current.originalUrl).toBeNull()
      expect(result.current.shortCode).toBeNull()
      expect(result.current.isCopied).toBe(false)
    })
  })

  describe('shortenUrl', () => {
    it('should successfully shorten a valid URL', async () => {
      const mockResponse = {
        shortCode: 'abc123',
        shortUrl: 'https://short.url/abc123',
        originalUrl: 'https://example.com/very/long/url/path',
        createdAt: new Date().toISOString(),
      }

      vi.mocked(api.shortenUrl).mockResolvedValueOnce(mockResponse)

      const { result } = renderHook(() => useUrlShortener())

      let response: ReturnType<typeof result.current.shortenUrl>
      await act(async () => {
        response = await result.current.shortenUrl('https://example.com/very/long/url/path')
      })

      expect(result.current.isLoading).toBe(false)
      expect(result.current.error).toBeNull()
      expect(result.current.shortenedUrl).toBe('https://short.url/abc123')
      expect(result.current.originalUrl).toBe('https://example.com/very/long/url/path')
      expect(result.current.shortCode).toBe('abc123')
    })

    it('should show loading state during API call', async () => {
      let resolvePromise: (value: any) => void
      const pendingPromise = new Promise((resolve) => {
        resolvePromise = resolve
      })

      vi.mocked(api.shortenUrl).mockReturnValueOnce(pendingPromise as any)

      const { result } = renderHook(() => useUrlShortener())

      act(() => {
        result.current.shortenUrl('https://example.com')
      })

      expect(result.current.isLoading).toBe(true)

      // Resolve the promise
      await act(async () => {
        resolvePromise!({
          shortCode: 'abc123',
          shortUrl: 'https://short.url/abc123',
          originalUrl: 'https://example.com',
          createdAt: new Date().toISOString(),
        })
      })

      expect(result.current.isLoading).toBe(false)
    })

    it('should return error for invalid URL', async () => {
      const { result } = renderHook(() => useUrlShortener())

      // Use ftp:// which is not http/https and will fail isValidUrl check
      await act(async () => {
        await result.current.shortenUrl('ftp://example.com')
      })

      expect(result.current.isLoading).toBe(false)
      expect(result.current.error).toBe('Please enter a valid URL')
      expect(result.current.shortenedUrl).toBeNull()
    })

    it('should return error for empty URL', async () => {
      const { result } = renderHook(() => useUrlShortener())

      await act(async () => {
        await result.current.shortenUrl('')
      })

      expect(result.current.error).toBe('Please enter a URL')
    })

    it('should return error for whitespace-only URL', async () => {
      const { result } = renderHook(() => useUrlShortener())

      await act(async () => {
        await result.current.shortenUrl('   ')
      })

      expect(result.current.error).toBe('Please enter a URL')
    })

    it('should handle API errors', async () => {
      vi.mocked(api.shortenUrl).mockRejectedValueOnce(new Error('Network error'))

      const { result } = renderHook(() => useUrlShortener())

      await act(async () => {
        await result.current.shortenUrl('https://example.com')
      })

      expect(result.current.isLoading).toBe(false)
      // User-friendly error message for network errors
      expect(result.current.error).toBe('Unable to connect. Please check your internet connection and try again.')
      expect(result.current.shortenedUrl).toBeNull()
    })

    it('should handle non-Error rejection', async () => {
      vi.mocked(api.shortenUrl).mockRejectedValueOnce('Some error')

      const { result } = renderHook(() => useUrlShortener())

      await act(async () => {
        await result.current.shortenUrl('https://example.com')
      })

      expect(result.current.error).toBe('Failed to shorten URL. Please try again.')
    })

    it('should auto-add https:// to URLs without protocol', async () => {
      const mockResponse = {
        shortCode: 'abc123',
        shortUrl: 'https://short.url/abc123',
        originalUrl: 'https://example.com',
        createdAt: new Date().toISOString(),
      }

      vi.mocked(api.shortenUrl).mockResolvedValueOnce(mockResponse)

      const { result } = renderHook(() => useUrlShortener())

      await act(async () => {
        await result.current.shortenUrl('example.com')
      })

      expect(api.shortenUrl).toHaveBeenCalledWith({ url: 'https://example.com' })
    })
  })

  describe('copyToClipboard', () => {
    it('should copy shortened URL to clipboard', async () => {
      const mockResponse = {
        shortCode: 'abc123',
        shortUrl: 'https://short.url/abc123',
        originalUrl: 'https://example.com',
        createdAt: new Date().toISOString(),
      }

      vi.mocked(api.shortenUrl).mockResolvedValueOnce(mockResponse)

      const { result } = renderHook(() => useUrlShortener())

      await act(async () => {
        await result.current.shortenUrl('https://example.com')
      })

      await act(async () => {
        await result.current.copyToClipboard()
      })

      expect(mockClipboard.writeText).toHaveBeenCalledWith('https://short.url/abc123')
      expect(result.current.isCopied).toBe(true)
    })

    it('should reset isCopied state after 2 seconds', async () => {
      const mockResponse = {
        shortCode: 'abc123',
        shortUrl: 'https://short.url/abc123',
        originalUrl: 'https://example.com',
        createdAt: new Date().toISOString(),
      }

      vi.mocked(api.shortenUrl).mockResolvedValueOnce(mockResponse)

      const { result } = renderHook(() => useUrlShortener())

      await act(async () => {
        await result.current.shortenUrl('https://example.com')
      })

      await act(async () => {
        await result.current.copyToClipboard()
      })

      expect(result.current.isCopied).toBe(true)

      // Fast-forward 2 seconds
      act(() => {
        vi.advanceTimersByTime(2000)
      })

      expect(result.current.isCopied).toBe(false)
    })

    it('should not copy if no shortened URL exists', async () => {
      const { result } = renderHook(() => useUrlShortener())

      await act(async () => {
        await result.current.copyToClipboard()
      })

      expect(mockClipboard.writeText).not.toHaveBeenCalled()
    })

    it('should handle clipboard API error', async () => {
      mockClipboard.writeText.mockRejectedValueOnce(new Error('Clipboard error'))

      const mockResponse = {
        shortCode: 'abc123',
        shortUrl: 'https://short.url/abc123',
        originalUrl: 'https://example.com',
        createdAt: new Date().toISOString(),
      }

      vi.mocked(api.shortenUrl).mockResolvedValueOnce(mockResponse)

      const { result } = renderHook(() => useUrlShortener())

      await act(async () => {
        await result.current.shortenUrl('https://example.com')
      })

      await act(async () => {
        await result.current.copyToClipboard()
      })

      expect(result.current.error).toBe('Failed to copy to clipboard')
    })
  })

  describe('reset', () => {
    it('should reset all state to initial values', async () => {
      const mockResponse = {
        shortCode: 'abc123',
        shortUrl: 'https://short.url/abc123',
        originalUrl: 'https://example.com',
        createdAt: new Date().toISOString(),
      }

      vi.mocked(api.shortenUrl).mockResolvedValueOnce(mockResponse)

      const { result } = renderHook(() => useUrlShortener())

      // First, get a result
      await act(async () => {
        await result.current.shortenUrl('https://example.com')
      })

      expect(result.current.shortenedUrl).not.toBeNull()

      // Then reset
      act(() => {
        result.current.reset()
      })

      expect(result.current.isLoading).toBe(false)
      expect(result.current.error).toBeNull()
      expect(result.current.shortenedUrl).toBeNull()
      expect(result.current.originalUrl).toBeNull()
      expect(result.current.shortCode).toBeNull()
      expect(result.current.isCopied).toBe(false)
    })
  })
})
