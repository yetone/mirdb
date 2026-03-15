/**
 * Theme management hook.
 * Owner: Scenario 7 - Dark Mode and Light Mode Toggle
 *
 * Returns:
 * - theme: 'light' | 'dark'
 * - toggleTheme: () => void
 * - setTheme: (theme: Theme) => void
 *
 * Features:
 * - localStorage persistence
 * - System preference detection on initial load
 * - CSS class application to document root
 */

import { useState, useEffect, useCallback } from 'react'
import type { Theme } from '@/types'

const THEME_STORAGE_KEY = 'mirdb-theme'

/**
 * Gets the initial theme from localStorage or system preference
 */
function getInitialTheme(defaultTheme?: Theme): Theme {
  // Check localStorage first
  if (typeof window !== 'undefined') {
    const storedTheme = localStorage.getItem(THEME_STORAGE_KEY) as Theme | null
    if (storedTheme === 'light' || storedTheme === 'dark') {
      return storedTheme
    }

    // Check system preference
    if (window.matchMedia('(prefers-color-scheme: dark)').matches) {
      return 'dark'
    }
  }

  return defaultTheme ?? 'light'
}

/**
 * Applies the theme class to the document root element
 */
function applyTheme(theme: Theme): void {
  if (typeof document !== 'undefined') {
    const root = document.documentElement
    if (theme === 'dark') {
      root.classList.add('dark')
    } else {
      root.classList.remove('dark')
    }
  }
}

/**
 * Persists the theme to localStorage
 */
function persistTheme(theme: Theme): void {
  if (typeof window !== 'undefined') {
    localStorage.setItem(THEME_STORAGE_KEY, theme)
  }
}

export interface UseThemeReturn {
  theme: Theme
  toggleTheme: () => void
  setTheme: (theme: Theme) => void
}

export function useTheme(defaultTheme?: Theme): UseThemeReturn {
  const [theme, setThemeState] = useState<Theme>(() => getInitialTheme(defaultTheme))

  // Apply theme class on mount and theme changes
  useEffect(() => {
    applyTheme(theme)
  }, [theme])

  // Set theme with persistence
  const setTheme = useCallback((newTheme: Theme) => {
    setThemeState(newTheme)
    persistTheme(newTheme)
  }, [])

  // Toggle between light and dark
  const toggleTheme = useCallback(() => {
    setTheme(theme === 'light' ? 'dark' : 'light')
  }, [theme, setTheme])

  // Listen for system preference changes
  useEffect(() => {
    if (typeof window === 'undefined') return

    const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)')

    const handleChange = (e: MediaQueryListEvent) => {
      // Only update if there's no stored preference
      const storedTheme = localStorage.getItem(THEME_STORAGE_KEY)
      if (!storedTheme) {
        const newTheme: Theme = e.matches ? 'dark' : 'light'
        setThemeState(newTheme)
        applyTheme(newTheme)
      }
    }

    mediaQuery.addEventListener('change', handleChange)
    return () => mediaQuery.removeEventListener('change', handleChange)
  }, [])

  return { theme, toggleTheme, setTheme }
}

export { THEME_STORAGE_KEY }
