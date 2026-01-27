/**
 * Theme Context
 * Owner: Scenario 5 - Theme Switching
 *
 * Provides theme state management for the application:
 * - Manages current theme state
 * - Persists theme preference to localStorage
 * - Initializes from localStorage or system preference
 * - Applies theme to document via data-theme attribute
 *
 * Requirements: REQ-8 - Support theme switching (light/dark mode)
 * User Stories: US-6 - Toggle Theme
 */

import { createContext, useContext, useState, useEffect, useCallback, ReactNode } from 'react'

/** Available themes from DaisyUI configuration */
export const AVAILABLE_THEMES = ['light', 'dark', 'cyberpunk', 'synthwave'] as const
export type Theme = (typeof AVAILABLE_THEMES)[number]

const THEME_STORAGE_KEY = 'theme-preference'
const DEFAULT_THEME: Theme = 'light'

export interface ThemeContextType {
  /** Current active theme */
  theme: Theme
  /** Change the current theme */
  setTheme: (theme: Theme) => void
  /** Toggle between light and dark themes */
  toggleTheme: () => void
  /** List of available themes */
  availableThemes: readonly Theme[]
}

const ThemeContext = createContext<ThemeContextType | undefined>(undefined)

/**
 * Get initial theme from localStorage or system preference
 */
function getInitialTheme(): Theme {
  // Check localStorage first
  if (typeof window !== 'undefined') {
    const stored = localStorage.getItem(THEME_STORAGE_KEY)
    if (stored && AVAILABLE_THEMES.includes(stored as Theme)) {
      return stored as Theme
    }

    // Check system preference
    if (window.matchMedia?.('(prefers-color-scheme: dark)').matches) {
      return 'dark'
    }
  }

  return DEFAULT_THEME
}

/**
 * Apply theme to document element
 */
function applyTheme(theme: Theme): void {
  if (typeof document !== 'undefined') {
    document.documentElement.setAttribute('data-theme', theme)
  }
}

export interface ThemeProviderProps {
  children: ReactNode
  /** Initial theme (for testing) */
  initialTheme?: Theme
}

export function ThemeProvider({ children, initialTheme }: ThemeProviderProps) {
  const [theme, setThemeState] = useState<Theme>(() => initialTheme ?? getInitialTheme())

  // Apply theme on mount and when theme changes
  useEffect(() => {
    applyTheme(theme)
  }, [theme])

  // Listen to system preference changes
  useEffect(() => {
    if (typeof window === 'undefined') return

    const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)')

    const handleChange = (e: MediaQueryListEvent) => {
      // Only auto-change if no preference is stored
      if (!localStorage.getItem(THEME_STORAGE_KEY)) {
        setThemeState(e.matches ? 'dark' : 'light')
      }
    }

    mediaQuery.addEventListener('change', handleChange)
    return () => mediaQuery.removeEventListener('change', handleChange)
  }, [])

  const setTheme = useCallback((newTheme: Theme) => {
    setThemeState(newTheme)
    if (typeof window !== 'undefined') {
      localStorage.setItem(THEME_STORAGE_KEY, newTheme)
    }
  }, [])

  const toggleTheme = useCallback(() => {
    setTheme(theme === 'light' ? 'dark' : 'light')
  }, [theme, setTheme])

  const value: ThemeContextType = {
    theme,
    setTheme,
    toggleTheme,
    availableThemes: AVAILABLE_THEMES,
  }

  return <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>
}

/**
 * Hook to access theme context
 * @throws Error if used outside ThemeProvider
 */
export function useTheme(): ThemeContextType {
  const context = useContext(ThemeContext)
  if (context === undefined) {
    throw new Error('useTheme must be used within a ThemeProvider')
  }
  return context
}

export default ThemeContext
