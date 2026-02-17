/**
 * Theme context provider and hook.
 * Owner: First builder (shared)
 *
 * Manages application theme state:
 * - Current theme selection
 * - Theme persistence (localStorage)
 * - Available themes list
 */

import React, { createContext, useContext, useState, useEffect, ReactNode } from 'react'
import { Theme } from '../types'

interface ThemeContextType {
  theme: Theme
  setTheme: (theme: Theme) => void
  availableThemes: Theme[]
}

const availableThemes: Theme[] = ['light', 'dark', 'cyberpunk', 'synthwave', 'retro', 'valentine']

const ThemeContext = createContext<ThemeContextType | undefined>(undefined)

interface ThemeProviderProps {
  children: ReactNode
}

export function ThemeProvider({ children }: ThemeProviderProps) {
  const [theme, setThemeState] = useState<Theme>(() => {
    if (typeof window !== 'undefined') {
      const stored = localStorage.getItem('theme') as Theme
      if (stored && availableThemes.includes(stored)) {
        return stored
      }
    }
    return 'dark'
  })

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme)
    localStorage.setItem('theme', theme)
  }, [theme])

  const setTheme = (newTheme: Theme) => {
    setThemeState(newTheme)
  }

  return (
    <ThemeContext.Provider value={{ theme, setTheme, availableThemes }}>
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
