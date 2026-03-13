/**
 * Theme Context Provider
 * Owner: Scenario 8 - Dark Mode Toggle
 *
 * Provides theme state management with:
 * - localStorage persistence
 * - System preference detection on initial load
 * - Toggle between light and dark themes
 * - Applies 'dark' class to document element for Tailwind CSS
 */

import { createContext, useContext, useEffect, useState, type ReactNode } from 'react';
import type { Theme, ThemeContextValue } from '../types';

const STORAGE_KEY = 'mirdb-theme';

const ThemeContext = createContext<ThemeContextValue | undefined>(undefined);

/**
 * Detects the user's preferred color scheme from system settings
 */
function getSystemTheme(): Theme {
  if (typeof window !== 'undefined' && window.matchMedia) {
    return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
  }
  return 'light';
}

/**
 * Retrieves stored theme from localStorage or falls back to system preference
 */
function getInitialTheme(): Theme {
  if (typeof window !== 'undefined') {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (stored === 'dark' || stored === 'light') {
      return stored;
    }
  }
  return getSystemTheme();
}

/**
 * Applies the theme class to the document element
 */
function applyTheme(theme: Theme): void {
  if (typeof document !== 'undefined') {
    const root = document.documentElement;
    if (theme === 'dark') {
      root.classList.add('dark');
    } else {
      root.classList.remove('dark');
    }
  }
}

interface ThemeProviderProps {
  children: ReactNode;
  defaultTheme?: Theme;
}

/**
 * ThemeProvider component that wraps the app and provides theme context
 */
export function ThemeProvider({ children, defaultTheme }: ThemeProviderProps) {
  const [theme, setTheme] = useState<Theme>(() => {
    // Use defaultTheme if provided (useful for testing), otherwise detect
    return defaultTheme ?? getInitialTheme();
  });

  // Apply theme class on mount and whenever theme changes
  useEffect(() => {
    applyTheme(theme);
  }, [theme]);

  // Persist theme to localStorage whenever it changes
  useEffect(() => {
    if (typeof window !== 'undefined') {
      localStorage.setItem(STORAGE_KEY, theme);
    }
  }, [theme]);

  const toggleTheme = () => {
    setTheme((prev) => (prev === 'light' ? 'dark' : 'light'));
  };

  const value: ThemeContextValue = {
    theme,
    toggleTheme,
  };

  return <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>;
}

/**
 * Hook to access the theme context
 * @throws Error if used outside of ThemeProvider
 */
export function useThemeContext(): ThemeContextValue {
  const context = useContext(ThemeContext);
  if (context === undefined) {
    throw new Error('useThemeContext must be used within a ThemeProvider');
  }
  return context;
}

export { ThemeContext };
