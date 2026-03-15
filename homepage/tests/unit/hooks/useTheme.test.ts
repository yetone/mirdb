/**
 * Unit tests for useTheme hook.
 * Owner: Scenario 7 - Dark Mode and Light Mode Toggle
 *
 * Test Cases:
 * - Theme initialization from localStorage
 * - Theme initialization from system preference
 * - Theme toggle functionality
 * - localStorage persistence
 * - CSS class application
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { renderHook, act } from '@testing-library/react'
import { useTheme, THEME_STORAGE_KEY } from '@/hooks/useTheme'

// Mock localStorage
const localStorageMock = (() => {
  let store: Record<string, string> = {}
  return {
    getItem: vi.fn((key: string) => store[key] || null),
    setItem: vi.fn((key: string, value: string) => {
      store[key] = value
    }),
    removeItem: vi.fn((key: string) => {
      delete store[key]
    }),
    clear: vi.fn(() => {
      store = {}
    }),
  }
})()

Object.defineProperty(window, 'localStorage', { value: localStorageMock })

describe('useTheme Hook', () => {
  beforeEach(() => {
    localStorageMock.clear()
    vi.clearAllMocks()
    document.documentElement.classList.remove('dark')
  })

  afterEach(() => {
    document.documentElement.classList.remove('dark')
  })

  describe('Initialization', () => {
    it('initializes with default light theme when no preference exists', () => {
      const { result } = renderHook(() => useTheme())

      expect(result.current.theme).toBe('light')
    })

    it('initializes with provided default theme', () => {
      const { result } = renderHook(() => useTheme('dark'))

      expect(result.current.theme).toBe('dark')
    })

    it('loads theme from localStorage if available', () => {
      localStorageMock.setItem(THEME_STORAGE_KEY, 'dark')

      const { result } = renderHook(() => useTheme())

      expect(result.current.theme).toBe('dark')
    })

    it('localStorage preference overrides default theme', () => {
      localStorageMock.setItem(THEME_STORAGE_KEY, 'dark')

      const { result } = renderHook(() => useTheme('light'))

      expect(result.current.theme).toBe('dark')
    })

    it('applies dark class to document when dark theme', () => {
      localStorageMock.setItem(THEME_STORAGE_KEY, 'dark')

      renderHook(() => useTheme())

      expect(document.documentElement.classList.contains('dark')).toBe(true)
    })

    it('does not apply dark class when light theme', () => {
      const { result } = renderHook(() => useTheme('light'))

      expect(result.current.theme).toBe('light')
      expect(document.documentElement.classList.contains('dark')).toBe(false)
    })
  })

  describe('toggleTheme', () => {
    it('toggles from light to dark', () => {
      const { result } = renderHook(() => useTheme('light'))

      act(() => {
        result.current.toggleTheme()
      })

      expect(result.current.theme).toBe('dark')
    })

    it('toggles from dark to light', () => {
      const { result } = renderHook(() => useTheme('dark'))

      act(() => {
        result.current.toggleTheme()
      })

      expect(result.current.theme).toBe('light')
    })

    it('applies dark class after toggling to dark', () => {
      const { result } = renderHook(() => useTheme('light'))

      act(() => {
        result.current.toggleTheme()
      })

      expect(document.documentElement.classList.contains('dark')).toBe(true)
    })

    it('removes dark class after toggling to light', () => {
      const { result } = renderHook(() => useTheme('dark'))

      act(() => {
        result.current.toggleTheme()
      })

      expect(document.documentElement.classList.contains('dark')).toBe(false)
    })

    it('persists theme to localStorage after toggle', () => {
      const { result } = renderHook(() => useTheme('light'))

      act(() => {
        result.current.toggleTheme()
      })

      expect(localStorageMock.setItem).toHaveBeenCalledWith(THEME_STORAGE_KEY, 'dark')
    })
  })

  describe('setTheme', () => {
    it('sets theme to dark', () => {
      const { result } = renderHook(() => useTheme('light'))

      act(() => {
        result.current.setTheme('dark')
      })

      expect(result.current.theme).toBe('dark')
    })

    it('sets theme to light', () => {
      const { result } = renderHook(() => useTheme('dark'))

      act(() => {
        result.current.setTheme('light')
      })

      expect(result.current.theme).toBe('light')
    })

    it('persists theme to localStorage', () => {
      const { result } = renderHook(() => useTheme('light'))

      act(() => {
        result.current.setTheme('dark')
      })

      expect(localStorageMock.setItem).toHaveBeenCalledWith(THEME_STORAGE_KEY, 'dark')
    })

    it('updates document class when setting theme', () => {
      const { result } = renderHook(() => useTheme('light'))

      act(() => {
        result.current.setTheme('dark')
      })

      expect(document.documentElement.classList.contains('dark')).toBe(true)

      act(() => {
        result.current.setTheme('light')
      })

      expect(document.documentElement.classList.contains('dark')).toBe(false)
    })
  })

  describe('Return values', () => {
    it('returns theme, toggleTheme, and setTheme', () => {
      const { result } = renderHook(() => useTheme())

      expect(result.current).toHaveProperty('theme')
      expect(result.current).toHaveProperty('toggleTheme')
      expect(result.current).toHaveProperty('setTheme')
      expect(typeof result.current.theme).toBe('string')
      expect(typeof result.current.toggleTheme).toBe('function')
      expect(typeof result.current.setTheme).toBe('function')
    })
  })
})
