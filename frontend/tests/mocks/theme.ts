/**
 * Theme Test Mocks
 * Owner: Scenario 5 - Theme Switching
 *
 * Provides mocks and utilities for testing theme functionality:
 * - localStorage mock with theme persistence
 * - Mock ThemeProvider for controlled testing
 * - Helper functions for theme testing
 */

import { vi } from 'vitest'
import { Theme, AVAILABLE_THEMES } from '../../src/contexts/ThemeContext'

const THEME_STORAGE_KEY = 'theme-preference'

/**
 * Create a mock localStorage with optional initial theme
 */
export function createMockLocalStorage(initialTheme?: Theme) {
  const store: Record<string, string> = {}

  if (initialTheme) {
    store[THEME_STORAGE_KEY] = initialTheme
  }

  return {
    getItem: vi.fn((key: string) => store[key] ?? null),
    setItem: vi.fn((key: string, value: string) => {
      store[key] = value
    }),
    removeItem: vi.fn((key: string) => {
      delete store[key]
    }),
    clear: vi.fn(() => {
      Object.keys(store).forEach((key) => delete store[key])
    }),
    key: vi.fn((index: number) => Object.keys(store)[index] ?? null),
    get length() {
      return Object.keys(store).length
    },
    // Access to internal store for testing
    _store: store,
  }
}

/**
 * Setup localStorage mock for a test
 */
export function setupLocalStorageMock(initialTheme?: Theme) {
  const mockStorage = createMockLocalStorage(initialTheme)
  Object.defineProperty(window, 'localStorage', {
    value: mockStorage,
    writable: true,
    configurable: true,
  })
  return mockStorage
}

/**
 * Create a mock matchMedia for testing system preference detection
 */
export function createMockMatchMedia(prefersDark: boolean = false) {
  const listeners: Array<(e: MediaQueryListEvent) => void> = []

  const mockMatchMedia = vi.fn().mockImplementation((query: string) => {
    const isDarkQuery = query === '(prefers-color-scheme: dark)'

    return {
      matches: isDarkQuery ? prefersDark : false,
      media: query,
      onchange: null,
      addListener: vi.fn((cb) => listeners.push(cb)),
      removeListener: vi.fn((cb) => {
        const idx = listeners.indexOf(cb)
        if (idx >= 0) listeners.splice(idx, 1)
      }),
      addEventListener: vi.fn((_, cb) => listeners.push(cb)),
      removeEventListener: vi.fn((_, cb) => {
        const idx = listeners.indexOf(cb)
        if (idx >= 0) listeners.splice(idx, 1)
      }),
      dispatchEvent: vi.fn(),
    }
  })

  // Function to simulate system preference change
  const simulatePreferenceChange = (prefersDark: boolean) => {
    listeners.forEach((listener) => {
      listener({ matches: prefersDark } as MediaQueryListEvent)
    })
  }

  return { mockMatchMedia, simulatePreferenceChange }
}

/**
 * Setup matchMedia mock for a test
 */
export function setupMatchMediaMock(prefersDark: boolean = false) {
  const { mockMatchMedia, simulatePreferenceChange } = createMockMatchMedia(prefersDark)
  Object.defineProperty(window, 'matchMedia', {
    value: mockMatchMedia,
    writable: true,
    configurable: true,
  })
  return { mockMatchMedia, simulatePreferenceChange }
}

/**
 * Get the current theme from the document
 */
export function getCurrentDocumentTheme(): Theme | null {
  const theme = document.documentElement.getAttribute('data-theme')
  return theme && AVAILABLE_THEMES.includes(theme as Theme) ? (theme as Theme) : null
}

/**
 * Reset document theme attribute
 */
export function resetDocumentTheme() {
  document.documentElement.removeAttribute('data-theme')
}

/**
 * Verify theme is correctly applied to document
 */
export function verifyThemeApplied(expectedTheme: Theme): boolean {
  return document.documentElement.getAttribute('data-theme') === expectedTheme
}

export { THEME_STORAGE_KEY, AVAILABLE_THEMES }
