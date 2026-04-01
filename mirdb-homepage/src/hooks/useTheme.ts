/**
 * Theme Hook.
 * Owner: Scenario 10 - Dark Mode Theme Toggle
 *
 * Custom hook for theme management:
 * - Returns current theme
 * - Provides toggleTheme function
 * - Handles localStorage persistence
 * - Respects system preference
 */
import { useThemeContext } from '../context/ThemeContext'
import { Theme } from '../types'

interface UseThemeReturn {
  theme: Theme
  toggleTheme: () => void
  setTheme: (theme: Theme) => void
  isDark: boolean
}

export function useTheme(): UseThemeReturn {
  const { theme, toggleTheme, setTheme } = useThemeContext()

  return {
    theme,
    toggleTheme,
    setTheme,
    isDark: theme === 'dark'
  }
}
