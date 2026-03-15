/**
 * Theme Context for application-wide theme state.
 * Owner: Scenario 7 - Dark Mode and Light Mode Toggle
 *
 * Exports:
 * - ThemeProvider: React context provider component
 * - useThemeContext: Hook to access theme state and toggle function
 *
 * Features:
 * - Theme persistence via localStorage
 * - System preference detection
 * - WCAG contrast compliance in both modes
 */

import { createContext, useContext, ReactNode } from 'react'
import { useTheme } from '@/hooks/useTheme'
import type { Theme } from '@/types'

export interface ThemeContextValue {
  theme: Theme
  toggleTheme: () => void
  setTheme: (theme: Theme) => void
}

const ThemeContext = createContext<ThemeContextValue | undefined>(undefined)

export interface ThemeProviderProps {
  children: ReactNode
  defaultTheme?: Theme
}

export function ThemeProvider({ children, defaultTheme }: ThemeProviderProps) {
  const themeState = useTheme(defaultTheme)

  return (
    <ThemeContext.Provider value={themeState}>
      {children}
    </ThemeContext.Provider>
  )
}

export function useThemeContext(): ThemeContextValue {
  const context = useContext(ThemeContext)
  if (context === undefined) {
    throw new Error('useThemeContext must be used within a ThemeProvider')
  }
  return context
}

export { ThemeContext }
