import { createContext, useContext, useEffect, useState, useCallback, type ReactNode } from 'react'

export type Theme = 'light' | 'dark' | 'cyberpunk' | 'synthwave'
export type ThemePreference = Theme | 'system'

interface ThemeContextType {
  theme: Theme
  themePreference: ThemePreference
  setThemePreference: (preference: ThemePreference) => void
  isDarkMode: boolean
}

const ThemeContext = createContext<ThemeContextType | undefined>(undefined)

const STORAGE_KEY = 'theme-preference'
const DARK_THEMES: Theme[] = ['dark', 'cyberpunk', 'synthwave']

function getSystemTheme(): Theme {
  if (typeof window !== 'undefined' && window.matchMedia) {
    return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'
  }
  return 'light'
}

function getStoredPreference(): ThemePreference {
  if (typeof window === 'undefined') return 'system'
  const stored = localStorage.getItem(STORAGE_KEY)
  if (stored && ['light', 'dark', 'cyberpunk', 'synthwave', 'system'].includes(stored)) {
    return stored as ThemePreference
  }
  return 'system'
}

function resolveTheme(preference: ThemePreference): Theme {
  if (preference === 'system') {
    return getSystemTheme()
  }
  return preference
}

function applyTheme(theme: Theme): void {
  if (typeof document !== 'undefined') {
    document.documentElement.setAttribute('data-theme', theme)
  }
}

interface ThemeProviderProps {
  children: ReactNode
  defaultTheme?: ThemePreference
}

export function ThemeProvider({ children, defaultTheme }: ThemeProviderProps) {
  const [themePreference, setThemePreferenceState] = useState<ThemePreference>(() => {
    return defaultTheme ?? getStoredPreference()
  })

  const [theme, setTheme] = useState<Theme>(() => resolveTheme(themePreference))

  const setThemePreference = useCallback((preference: ThemePreference) => {
    setThemePreferenceState(preference)
    if (typeof window !== 'undefined') {
      localStorage.setItem(STORAGE_KEY, preference)
    }
  }, [])

  // Update theme when preference changes
  useEffect(() => {
    const resolved = resolveTheme(themePreference)
    setTheme(resolved)
    applyTheme(resolved)
  }, [themePreference])

  // Listen for system theme changes when using 'system' preference
  useEffect(() => {
    if (themePreference !== 'system' || typeof window === 'undefined') return

    const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)')

    const handleChange = (e: MediaQueryListEvent) => {
      const newTheme = e.matches ? 'dark' : 'light'
      setTheme(newTheme)
      applyTheme(newTheme)
    }

    mediaQuery.addEventListener('change', handleChange)
    return () => mediaQuery.removeEventListener('change', handleChange)
  }, [themePreference])

  const isDarkMode = DARK_THEMES.includes(theme)

  return (
    <ThemeContext.Provider
      value={{
        theme,
        themePreference,
        setThemePreference,
        isDarkMode,
      }}
    >
      {children}
    </ThemeContext.Provider>
  )
}

export function useTheme(): ThemeContextType {
  const context = useContext(ThemeContext)
  if (context === undefined) {
    throw new Error('useTheme must be used within a ThemeProvider')
  }
  return context
}

export { ThemeContext }
