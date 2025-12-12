import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { renderHook, waitFor } from '@testing-library/react'
import { useContentFetch } from './useContentFetch'

/**
 * Integration tests for useContentFetch hook
 * Test Case 3: Test content API failure - Page displays fallback content or error state gracefully
 */
describe('useContentFetch Hook', () => {
  const mockFetch = vi.fn()

  beforeEach(() => {
    vi.stubGlobal('fetch', mockFetch)
    mockFetch.mockReset()
  })

  afterEach(() => {
    vi.unstubAllGlobals()
  })

  describe('Successful fetch', () => {
    it('returns data on successful API call', async () => {
      const mockData = {
        items: [
          { id: '1', title: 'Test Item', description: 'Test Description' }
        ]
      }

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve(mockData),
      })

      const { result } = renderHook(() => useContentFetch('/api/content'))

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })

      expect(result.current.data).toEqual(mockData)
      expect(result.current.error).toBeNull()
    })

    it('shows loading state during fetch', () => {
      mockFetch.mockImplementation(() => new Promise(() => {})) // Never resolves

      const { result } = renderHook(() => useContentFetch('/api/content'))

      expect(result.current.isLoading).toBe(true)
      expect(result.current.data).toBeNull()
      expect(result.current.error).toBeNull()
    })
  })

  describe('API failure handling', () => {
    it('handles 500 Internal Server Error gracefully', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 500,
        statusText: 'Internal Server Error',
        json: () => Promise.resolve({ error: 'Internal Server Error' }),
      })

      const { result } = renderHook(() => useContentFetch('/api/content'))

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })

      expect(result.current.error).not.toBeNull()
      expect(result.current.error?.message).toContain('500')
      expect(result.current.data).toBeNull()
    })

    it('handles 503 Service Unavailable gracefully', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 503,
        statusText: 'Service Unavailable',
        json: () => Promise.resolve({ error: 'Service Unavailable' }),
      })

      const { result } = renderHook(() => useContentFetch('/api/content'))

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })

      expect(result.current.error).not.toBeNull()
      expect(result.current.error?.message).toContain('503')
    })

    it('handles network error gracefully', async () => {
      mockFetch.mockRejectedValueOnce(new TypeError('Failed to fetch'))

      const { result } = renderHook(() => useContentFetch('/api/content'))

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })

      expect(result.current.error).not.toBeNull()
      expect(result.current.error?.message).toContain('Failed to fetch')
      expect(result.current.data).toBeNull()
    })

    it('handles timeout gracefully', async () => {
      mockFetch.mockImplementation(() =>
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error('Request timeout')), 100)
        )
      )

      const { result } = renderHook(() => useContentFetch('/api/content', { timeout: 50 }))

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      }, { timeout: 200 })

      expect(result.current.error).not.toBeNull()
    })
  })

  describe('Retry mechanism', () => {
    it('provides retry function after error', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 500,
        statusText: 'Internal Server Error',
      })

      const { result } = renderHook(() => useContentFetch('/api/content'))

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })

      expect(result.current.error).not.toBeNull()
      expect(typeof result.current.retry).toBe('function')
    })

    it('retries fetch when retry function is called', async () => {
      const mockData = {
        items: [{ id: '1', title: 'Success' }]
      }

      // First call fails
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 500,
        statusText: 'Internal Server Error',
      })

      // Second call succeeds
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve(mockData),
      })

      const { result } = renderHook(() => useContentFetch('/api/content'))

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })

      expect(result.current.error).not.toBeNull()

      // Trigger retry
      result.current.retry()

      await waitFor(() => {
        expect(result.current.data).toEqual(mockData)
      })

      expect(result.current.error).toBeNull()
    })
  })

  describe('Fallback data', () => {
    it('uses fallback data when API fails and fallback is provided', async () => {
      const fallbackData = {
        items: [{ id: 'fallback', title: 'Fallback Content' }]
      }

      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 500,
        statusText: 'Internal Server Error',
      })

      const { result } = renderHook(() =>
        useContentFetch('/api/content', { fallbackData })
      )

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })

      expect(result.current.data).toEqual(fallbackData)
      expect(result.current.error).not.toBeNull() // Error still tracked
      expect(result.current.isUsingFallback).toBe(true)
    })
  })
})
