/**
 * Theme Context Provider
 * Owner: Scenario 6 - Dark/Light Theme Toggle
 *
 * Provides theme state management:
 * - Current theme state
 * - Toggle function
 * - localStorage persistence
 * - System preference detection
 */

import React, { createContext, useContext, useState, useEffect, useCallback } from 'react';
import type { Theme } from '../types';

interface ThemeContextType {
  theme: Theme;
  toggleTheme: () => void;
  setTheme: (theme: Theme) => void;
}

const ThemeContext = createContext<ThemeContextType | undefined>(undefined);

const THEME_STORAGE_KEY = 'theme';

/**
 * Gets the initial theme from localStorage or system preference
 */
const getInitialTheme = (): Theme => {
  // Check localStorage first
  if (typeof window !== 'undefined') {
    const storedTheme = localStorage.getItem(THEME_STORAGE_KEY);
    if (storedTheme === 'dark' || storedTheme === 'light') {
      return storedTheme;
    }

    // Check system preference
    if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
      return 'dark';
    }
  }

  return 'light';
};

/**
 * Applies theme to the document root element
 */
const applyTheme = (theme: Theme): void => {
  if (typeof document !== 'undefined') {
    document.documentElement.setAttribute('data-theme', theme);
  }
};

interface ThemeProviderProps {
  children: React.ReactNode;
  defaultTheme?: Theme;
}

export const ThemeProvider: React.FC<ThemeProviderProps> = ({
  children,
  defaultTheme
}) => {
  const [theme, setThemeState] = useState<Theme>(() => {
    return defaultTheme ?? getInitialTheme();
  });

  // Apply theme to document on initial render and when theme changes
  useEffect(() => {
    applyTheme(theme);
  }, [theme]);

  // Persist theme to localStorage when it changes
  useEffect(() => {
    if (typeof window !== 'undefined') {
      localStorage.setItem(THEME_STORAGE_KEY, theme);
    }
  }, [theme]);

  const toggleTheme = useCallback(() => {
    setThemeState((prevTheme) => (prevTheme === 'light' ? 'dark' : 'light'));
  }, []);

  const setTheme = useCallback((newTheme: Theme) => {
    setThemeState(newTheme);
  }, []);

  const value: ThemeContextType = {
    theme,
    toggleTheme,
    setTheme,
  };

  return (
    <ThemeContext.Provider value={value}>
      {children}
    </ThemeContext.Provider>
  );
};

/**
 * Hook to access theme context
 * @throws Error if used outside of ThemeProvider
 */
export const useThemeContext = (): ThemeContextType => {
  const context = useContext(ThemeContext);
  if (context === undefined) {
    throw new Error('useThemeContext must be used within a ThemeProvider');
  }
  return context;
};

export { ThemeContext };
export type { ThemeContextType };
