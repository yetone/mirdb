/**
 * Theme Hook
 * Owner: Scenario 8 - Dark Mode Toggle
 *
 * Provides access to theme state and controls.
 * This hook wraps the ThemeContext for convenient usage.
 *
 * Returns:
 * - theme: 'light' | 'dark'
 * - toggleTheme: () => void
 * - isDark: boolean (convenience helper)
 */

import { useThemeContext } from '../context/ThemeContext';
import type { Theme } from '../types';

interface UseThemeReturn {
  theme: Theme;
  toggleTheme: () => void;
  isDark: boolean;
}

/**
 * Hook for accessing and controlling the current theme
 * @throws Error if used outside of ThemeProvider
 */
export function useTheme(): UseThemeReturn {
  const { theme, toggleTheme } = useThemeContext();

  return {
    theme,
    toggleTheme,
    isDark: theme === 'dark',
  };
}
