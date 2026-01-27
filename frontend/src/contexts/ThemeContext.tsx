/**
 * Theme Context
 *
 * Provides theme state management for the application.
 * Supports multiple DaisyUI themes: light, dark, cyberpunk, synthwave.
 * Persists user preference to localStorage.
 *
 * Requirements: REQ-8, NFR-4, NFR-6, US-6
 */

import { createContext, useContext, useState, useEffect, useCallback, type ReactNode } from 'react'

export const AVAILABLE_THEMES = ['light', 'dark', 'cyberpunk', 'synthwave'] as const
export type ThemeName = typeof AVAILABLE_THEMES[number]

const THEME_STORAGE_KEY = 'theme-preference'
const DEFAULT_THEME: ThemeName = 'light'

interface ThemeContextValue {
  theme: ThemeName
  setTheme: (theme: ThemeName) => void
  toggleTheme: () => void
  availableThemes: readonly ThemeName[]
}

const ThemeContext = createContext<ThemeContextValue | undefined>(undefined)

function getInitialTheme(): ThemeName {
  // Check localStorage first
  if (typeof window !== 'undefined') {
    const stored = localStorage.getItem(THEME_STORAGE_KEY)
    if (stored && AVAILABLE_THEMES.includes(stored as ThemeName)) {
      return stored as ThemeName
    }
    // Check system preference
    if (window.matchMedia('(prefers-color-scheme: dark)').matches) {
      return 'dark'
    }
  }
  return DEFAULT_THEME
}

function applyTheme(theme: ThemeName): void {
  if (typeof document !== 'undefined') {
    document.documentElement.setAttribute('data-theme', theme)
  }
}

export interface ThemeProviderProps {
  children: ReactNode
  defaultTheme?: ThemeName
}

export function ThemeProvider({ children, defaultTheme }: ThemeProviderProps) {
  const [theme, setThemeState] = useState<ThemeName>(() => defaultTheme ?? getInitialTheme())

  // Apply theme to document on mount and when theme changes
  useEffect(() => {
    applyTheme(theme)
  }, [theme])

  const setTheme = useCallback((newTheme: ThemeName) => {
    if (AVAILABLE_THEMES.includes(newTheme)) {
      setThemeState(newTheme)
      localStorage.setItem(THEME_STORAGE_KEY, newTheme)
    }
  }, [])

  const toggleTheme = useCallback(() => {
    const currentIndex = AVAILABLE_THEMES.indexOf(theme)
    const nextIndex = (currentIndex + 1) % AVAILABLE_THEMES.length
    setTheme(AVAILABLE_THEMES[nextIndex])
  }, [theme, setTheme])

  const value: ThemeContextValue = {
    theme,
    setTheme,
    toggleTheme,
    availableThemes: AVAILABLE_THEMES,
  }

  return (
    <ThemeContext.Provider value={value}>
      {children}
    </ThemeContext.Provider>
  )
}

export function useTheme(): ThemeContextValue {
  const context = useContext(ThemeContext)
  if (context === undefined) {
    throw new Error('useTheme must be used within a ThemeProvider')
  }
  return context
}

export { ThemeContext }
