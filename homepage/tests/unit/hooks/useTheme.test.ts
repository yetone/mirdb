/**
 * Unit tests for useTheme hook.
 * Owner: Scenario 10 - Dark Mode Theme
 *
 * Test cases covered:
 * - Theme toggle functionality
 * - localStorage persistence
 * - System preference detection
 * - Initial theme state
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { renderHook, act } from '@testing-library/react'
import { useTheme } from '../../../src/hooks/useTheme'

describe('useTheme', () => {
  const originalMatchMedia = window.matchMedia

  beforeEach(() => {
    localStorage.clear()
    document.documentElement.removeAttribute('data-theme')

    // Mock matchMedia to return light mode by default
    window.matchMedia = vi.fn().mockImplementation((query) => ({
      matches: false,
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    }))
  })

  afterEach(() => {
    window.matchMedia = originalMatchMedia
  })

  describe('initial state', () => {
    it('should return light theme by default when no preference is saved', () => {
      const { result } = renderHook(() => useTheme())

      expect(result.current.theme).toBe('light')
    })

    it('should load saved theme from localStorage', () => {
      localStorage.setItem('mirdb-theme', 'dark')

      const { result } = renderHook(() => useTheme())

      expect(result.current.theme).toBe('dark')
    })

    it('should respect system preference when no saved preference exists', () => {
      window.matchMedia = vi.fn().mockImplementation((query) => ({
        matches: query === '(prefers-color-scheme: dark)',
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      }))

      const { result } = renderHook(() => useTheme())

      expect(result.current.theme).toBe('dark')
    })

    it('should prioritize localStorage over system preference', () => {
      localStorage.setItem('mirdb-theme', 'light')

      window.matchMedia = vi.fn().mockImplementation((query) => ({
        matches: query === '(prefers-color-scheme: dark)',
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      }))

      const { result } = renderHook(() => useTheme())

      expect(result.current.theme).toBe('light')
    })
  })

  describe('toggleTheme', () => {
    it('should toggle from light to dark', () => {
      const { result } = renderHook(() => useTheme())

      expect(result.current.theme).toBe('light')

      act(() => {
        result.current.toggleTheme()
      })

      expect(result.current.theme).toBe('dark')
    })

    it('should toggle from dark to light', () => {
      localStorage.setItem('mirdb-theme', 'dark')

      const { result } = renderHook(() => useTheme())

      expect(result.current.theme).toBe('dark')

      act(() => {
        result.current.toggleTheme()
      })

      expect(result.current.theme).toBe('light')
    })

    it('should update localStorage when toggling', () => {
      const { result } = renderHook(() => useTheme())

      act(() => {
        result.current.toggleTheme()
      })

      expect(localStorage.getItem('mirdb-theme')).toBe('dark')

      act(() => {
        result.current.toggleTheme()
      })

      expect(localStorage.getItem('mirdb-theme')).toBe('light')
    })

    it('should update data-theme attribute on document when toggling', () => {
      const { result } = renderHook(() => useTheme())

      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      act(() => {
        result.current.toggleTheme()
      })

      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })
  })

  describe('setTheme', () => {
    it('should set theme to dark', () => {
      const { result } = renderHook(() => useTheme())

      act(() => {
        result.current.setTheme('dark')
      })

      expect(result.current.theme).toBe('dark')
      expect(localStorage.getItem('mirdb-theme')).toBe('dark')
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('should set theme to light', () => {
      localStorage.setItem('mirdb-theme', 'dark')

      const { result } = renderHook(() => useTheme())

      act(() => {
        result.current.setTheme('light')
      })

      expect(result.current.theme).toBe('light')
      expect(localStorage.getItem('mirdb-theme')).toBe('light')
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
    })
  })

  describe('persistence', () => {
    it('should persist dark theme across hook re-renders', () => {
      const { result, rerender } = renderHook(() => useTheme())

      act(() => {
        result.current.setTheme('dark')
      })

      rerender()

      expect(result.current.theme).toBe('dark')
    })

    it('should persist theme preference in localStorage for page reloads', () => {
      const { result } = renderHook(() => useTheme())

      act(() => {
        result.current.setTheme('dark')
      })

      // Simulate page reload by creating a new hook instance
      const { result: newResult } = renderHook(() => useTheme())

      expect(newResult.current.theme).toBe('dark')
      expect(localStorage.getItem('mirdb-theme')).toBe('dark')
    })
  })
})
