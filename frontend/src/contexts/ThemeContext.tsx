/**
 * Theme context provider and hook.
 *
 * Manages application theme state with persistence.
 */

import React, { createContext, useContext, useState, useEffect, type ReactNode } from 'react'
import type { Theme } from '../types'

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
    const stored = localStorage.getItem('theme') as Theme | null
    return stored && availableThemes.includes(stored) ? stored : 'dark'
  })

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme)
    localStorage.setItem('theme', theme)
  }, [theme])

  const setTheme = (newTheme: Theme) => {
    if (availableThemes.includes(newTheme)) {
      setThemeState(newTheme)
    }
  }

  return (
    <ThemeContext.Provider value={{ theme, setTheme, availableThemes }}>
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
