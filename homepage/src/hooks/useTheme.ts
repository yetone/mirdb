/**
 * Theme Hook
 * Owner: Scenario 6 - Dark/Light Theme Toggle
 *
 * Custom hook for theme management:
 * - Returns current theme
 * - Toggle function
 * - Set specific theme function
 */

import { useThemeContext } from '../context/ThemeContext';
import type { Theme } from '../types';

interface UseThemeReturn {
  theme: Theme;
  toggleTheme: () => void;
  setTheme: (theme: Theme) => void;
  isDark: boolean;
  isLight: boolean;
}

/**
 * Custom hook for managing theme state
 * Must be used within a ThemeProvider
 *
 * @returns Object containing theme state and control functions
 * @throws Error if used outside of ThemeProvider
 *
 * @example
 * const { theme, toggleTheme, isDark } = useTheme();
 * // theme: 'light' | 'dark'
 * // toggleTheme: () => void
 * // isDark: boolean
 */
export const useTheme = (): UseThemeReturn => {
  const { theme, toggleTheme, setTheme } = useThemeContext();

  return {
    theme,
    toggleTheme,
    setTheme,
    isDark: theme === 'dark',
    isLight: theme === 'light',
  };
};

export default useTheme;
