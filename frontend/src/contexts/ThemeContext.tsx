/**
 * Theme Context
 *
 * Provides theme state and switching logic throughout the app.
 */

import React, { createContext, useContext, useState, useEffect, ReactNode } from 'react'

interface ThemeContextType {
  theme: string
  setTheme: (theme: string) => void
  themes: string[]
}

const ThemeContext = createContext<ThemeContextType | undefined>(undefined)

const AVAILABLE_THEMES = ['light', 'dark', 'cyberpunk', 'synthwave', 'forest', 'aqua']

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [theme, setThemeState] = useState(() => {
    const saved = localStorage.getItem('theme')
    return saved || 'dark'
  })

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme)
    localStorage.setItem('theme', theme)
  }, [theme])

  const setTheme = (newTheme: string) => {
    if (AVAILABLE_THEMES.includes(newTheme)) {
      setThemeState(newTheme)
    }
  }

  return (
    <ThemeContext.Provider
      value={{
        theme,
        setTheme,
        themes: AVAILABLE_THEMES,
      }}
    >
      {children}
    </ThemeContext.Provider>
  )
}

export function useTheme() {
  const context = useContext(ThemeContext)
  if (context === undefined) {
    throw new Error('useTheme must be used within a ThemeProvider')
  }
  return context
}
