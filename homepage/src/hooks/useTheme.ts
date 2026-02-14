import { useThemeContext } from '../context/ThemeContext';

/**
 * Custom hook for theme management.
 *
 * Returns:
 * - theme: Current theme ('light' or 'dark')
 * - toggleTheme: Function to toggle between themes
 * - isDark: Boolean indicating if current theme is dark
 */
export const useTheme = () => {
  const { theme, toggleTheme } = useThemeContext();

  return {
    theme,
    toggleTheme,
    isDark: theme === 'dark',
  };
};
