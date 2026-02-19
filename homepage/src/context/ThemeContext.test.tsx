/**
 * ThemeContext Tests
 * Owner: Scenario 6 - Dark Mode & Theme System
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, act, renderHook } from '@testing-library/react'
import { ThemeProvider, useTheme } from './ThemeContext'
import type { Theme } from '@/types'

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

// Mock matchMedia
let mockMatchMediaMatches = false
const mockAddEventListener = vi.fn()
const mockRemoveEventListener = vi.fn()

const matchMediaMock = vi.fn().mockImplementation((query: string) => ({
  matches: mockMatchMediaMatches,
  media: query,
  onchange: null,
  addListener: vi.fn(),
  removeListener: vi.fn(),
  addEventListener: mockAddEventListener,
  removeEventListener: mockRemoveEventListener,
  dispatchEvent: vi.fn(),
}))

Object.defineProperty(window, 'matchMedia', { value: matchMediaMock })

function TestComponent() {
  const { theme, toggleTheme, setTheme } = useTheme()
  return (
    <div>
      <span data-testid="current-theme">{theme}</span>
      <button data-testid="toggle-btn" onClick={toggleTheme}>
        Toggle
      </button>
      <button data-testid="set-light-btn" onClick={() => setTheme('light')}>
        Set Light
      </button>
      <button data-testid="set-dark-btn" onClick={() => setTheme('dark')}>
        Set Dark
      </button>
    </div>
  )
}

describe('ThemeContext', () => {
  beforeEach(() => {
    localStorageMock.clear()
    vi.clearAllMocks()
    mockMatchMediaMatches = false
    document.documentElement.removeAttribute('data-theme')
  })

  afterEach(() => {
    document.documentElement.removeAttribute('data-theme')
  })

  describe('useTheme hook', () => {
    it('returns current theme and toggle function', () => {
      render(
        <ThemeProvider defaultTheme="light">
          <TestComponent />
        </ThemeProvider>
      )

      expect(screen.getByTestId('current-theme')).toHaveTextContent('light')
    })

    it('returns light theme by default when no system preference', () => {
      mockMatchMediaMatches = false

      render(
        <ThemeProvider>
          <TestComponent />
        </ThemeProvider>
      )

      expect(screen.getByTestId('current-theme')).toHaveTextContent('light')
    })

    it('respects system dark mode preference on initial load', () => {
      mockMatchMediaMatches = true

      render(
        <ThemeProvider>
          <TestComponent />
        </ThemeProvider>
      )

      expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
    })

    it('toggles theme from light to dark', () => {
      render(
        <ThemeProvider defaultTheme="light">
          <TestComponent />
        </ThemeProvider>
      )

      expect(screen.getByTestId('current-theme')).toHaveTextContent('light')

      fireEvent.click(screen.getByTestId('toggle-btn'))

      expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
    })

    it('toggles theme from dark to light', () => {
      render(
        <ThemeProvider defaultTheme="dark">
          <TestComponent />
        </ThemeProvider>
      )

      expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')

      fireEvent.click(screen.getByTestId('toggle-btn'))

      expect(screen.getByTestId('current-theme')).toHaveTextContent('light')
    })

    it('setTheme function works correctly', () => {
      render(
        <ThemeProvider defaultTheme="light">
          <TestComponent />
        </ThemeProvider>
      )

      fireEvent.click(screen.getByTestId('set-dark-btn'))
      expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')

      fireEvent.click(screen.getByTestId('set-light-btn'))
      expect(screen.getByTestId('current-theme')).toHaveTextContent('light')
    })

    it('throws error when used outside ThemeProvider', () => {
      // Suppress console.error for this test
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})

      expect(() => {
        render(<TestComponent />)
      }).toThrow('useTheme must be used within a ThemeProvider')

      consoleSpy.mockRestore()
    })
  })

  describe('localStorage persistence', () => {
    it('persists theme to localStorage when changed', () => {
      render(
        <ThemeProvider defaultTheme="light">
          <TestComponent />
        </ThemeProvider>
      )

      fireEvent.click(screen.getByTestId('toggle-btn'))

      expect(localStorageMock.setItem).toHaveBeenCalledWith('theme', 'dark')
    })

    it('loads theme from localStorage if present', () => {
      localStorageMock.getItem.mockReturnValueOnce('dark')

      render(
        <ThemeProvider>
          <TestComponent />
        </ThemeProvider>
      )

      expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
    })
  })

  describe('data-theme attribute', () => {
    it('sets data-theme attribute on document element', () => {
      render(
        <ThemeProvider defaultTheme="dark">
          <TestComponent />
        </ThemeProvider>
      )

      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('updates data-theme attribute when theme changes', () => {
      render(
        <ThemeProvider defaultTheme="light">
          <TestComponent />
        </ThemeProvider>
      )

      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      fireEvent.click(screen.getByTestId('toggle-btn'))

      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })
  })
})
