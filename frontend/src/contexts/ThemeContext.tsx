/**
 * Theme Context
 *
 * Provides theme state management for the application.
 * Supports light, dark, cyberpunk, and synthwave themes from DaisyUI.
 *
 * Requirements:
 * - REQ-7: Theme adaptation for user preferences
 * - NFR-2: Use existing theme system (Tailwind CSS + DaisyUI)
 */
import { createContext, useContext, useState, useEffect, useCallback, ReactNode } from 'react'

export type Theme = 'light' | 'dark' | 'cyberpunk' | 'synthwave'

export interface ThemeContextValue {
  theme: Theme
  setTheme: (theme: Theme) => void
  toggleTheme: () => void
  availableThemes: Theme[]
}

const ThemeContext = createContext<ThemeContextValue | null>(null)

const AVAILABLE_THEMES: Theme[] = ['light', 'dark', 'cyberpunk', 'synthwave']
const STORAGE_KEY = 'url-shortener-theme'

export interface ThemeProviderProps {
  children: ReactNode
  defaultTheme?: Theme
}

export function ThemeProvider({ children, defaultTheme = 'dark' }: ThemeProviderProps) {
  const [theme, setThemeState] = useState<Theme>(() => {
    // Check localStorage for saved theme preference
    if (typeof window !== 'undefined') {
      const saved = localStorage.getItem(STORAGE_KEY)
      if (saved && AVAILABLE_THEMES.includes(saved as Theme)) {
        return saved as Theme
      }
    }
    return defaultTheme
  })

  // Apply theme to DOM
  useEffect(() => {
    const root = document.documentElement
    // Find the data-theme container (could be on html or a wrapper div)
    const themeContainer = document.querySelector('[data-theme]') || root
    themeContainer.setAttribute('data-theme', theme)
    localStorage.setItem(STORAGE_KEY, theme)
  }, [theme])

  const setTheme = useCallback((newTheme: Theme) => {
    if (AVAILABLE_THEMES.includes(newTheme)) {
      setThemeState(newTheme)
    }
  }, [])

  const toggleTheme = useCallback(() => {
    setThemeState((current) => {
      const currentIndex = AVAILABLE_THEMES.indexOf(current)
      const nextIndex = (currentIndex + 1) % AVAILABLE_THEMES.length
      return AVAILABLE_THEMES[nextIndex]
    })
  }, [])

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
  if (!context) {
    throw new Error('useTheme must be used within a ThemeProvider')
  }
  return context
}

export { ThemeContext }
