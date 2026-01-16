import { createContext, useContext, useEffect, useState, ReactNode } from 'react'

export type Theme = 'light' | 'dark' | 'cyberpunk' | 'synthwave'
export type ThemePreference = Theme | 'system'

interface ThemeContextType {
  theme: Theme
  themePreference: ThemePreference
  setThemePreference: (preference: ThemePreference) => void
}

const ThemeContext = createContext<ThemeContextType | undefined>(undefined)

const STORAGE_KEY = 'theme-preference'

function getSystemTheme(): Theme {
  if (typeof window !== 'undefined' && window.matchMedia) {
    return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'
  }
  return 'light'
}

function resolveTheme(preference: ThemePreference): Theme {
  if (preference === 'system') {
    return getSystemTheme()
  }
  return preference
}

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [themePreference, setThemePreferenceState] = useState<ThemePreference>(() => {
    if (typeof window !== 'undefined') {
      const stored = localStorage.getItem(STORAGE_KEY)
      if (stored && ['light', 'dark', 'cyberpunk', 'synthwave', 'system'].includes(stored)) {
        return stored as ThemePreference
      }
    }
    return 'system'
  })

  const theme = resolveTheme(themePreference)

  const setThemePreference = (preference: ThemePreference) => {
    setThemePreferenceState(preference)
    localStorage.setItem(STORAGE_KEY, preference)
  }

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme)
  }, [theme])

  useEffect(() => {
    if (themePreference === 'system') {
      const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)')
      const handler = () => {
        document.documentElement.setAttribute('data-theme', getSystemTheme())
      }
      mediaQuery.addEventListener('change', handler)
      return () => mediaQuery.removeEventListener('change', handler)
    }
  }, [themePreference])

  return (
    <ThemeContext.Provider value={{ theme, themePreference, setThemePreference }}>
      {children}
    </ThemeContext.Provider>
  )
}

export function useTheme() {
  const context = useContext(ThemeContext)
  if (context === undefined) {
    // Return fallback for components rendered outside provider (e.g., in tests)
    return {
      theme: 'light' as Theme,
      themePreference: 'system' as ThemePreference,
      setThemePreference: () => {},
    }
  }
  return context
}
