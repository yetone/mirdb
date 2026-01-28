/**
 * Theme Context
 * Owner: Scenario 7 - Theme Support
 *
 * Requirements covered:
 * - REQ-6: Support multiple themes (light, dark, cyberpunk, synthwave) consistent with existing app
 * - US-7: Homepage should respect theme preference
 *
 * Expected exports:
 * - ThemeProvider: React.FC - Context provider component
 * - useTheme: Hook to access current theme and setTheme function
 * - Theme: Type for valid theme names
 *
 * Features:
 * - Provides theme context to all child components
 * - Persists theme preference to localStorage
 * - Applies data-theme attribute to root element
 * - Supports all DaisyUI themes: light, dark, cyberpunk, synthwave
 */

import { createContext, useContext, useState, useEffect, ReactNode } from 'react';

// Valid theme names matching DaisyUI configuration
export type Theme = 'light' | 'dark' | 'cyberpunk' | 'synthwave';

// Available themes for iteration
export const AVAILABLE_THEMES: Theme[] = ['light', 'dark', 'cyberpunk', 'synthwave'];

interface ThemeContextValue {
  theme: Theme;
  setTheme: (theme: Theme) => void;
}

const ThemeContext = createContext<ThemeContextValue | undefined>(undefined);

const THEME_STORAGE_KEY = 'url-shortener-theme';
const DEFAULT_THEME: Theme = 'dark';

interface ThemeProviderProps {
  children: ReactNode;
  initialTheme?: Theme;
}

export function ThemeProvider({ children, initialTheme }: ThemeProviderProps) {
  const [theme, setThemeState] = useState<Theme>(() => {
    // If initialTheme is provided (e.g., for testing), use it
    if (initialTheme) {
      return initialTheme;
    }
    // Try to get theme from localStorage
    if (typeof window !== 'undefined') {
      const storedTheme = localStorage.getItem(THEME_STORAGE_KEY);
      if (storedTheme && AVAILABLE_THEMES.includes(storedTheme as Theme)) {
        return storedTheme as Theme;
      }
    }
    return DEFAULT_THEME;
  });

  // Apply theme to document
  useEffect(() => {
    if (typeof document !== 'undefined') {
      // Apply data-theme attribute for DaisyUI
      document.documentElement.setAttribute('data-theme', theme);
    }
  }, [theme]);

  const setTheme = (newTheme: Theme) => {
    setThemeState(newTheme);
    // Persist to localStorage
    if (typeof window !== 'undefined') {
      localStorage.setItem(THEME_STORAGE_KEY, newTheme);
    }
  };

  return (
    <ThemeContext.Provider value={{ theme, setTheme }}>
      {children}
    </ThemeContext.Provider>
  );
}

export function useTheme(): ThemeContextValue {
  const context = useContext(ThemeContext);
  if (context === undefined) {
    throw new Error('useTheme must be used within a ThemeProvider');
  }
  return context;
}

export { ThemeContext };
