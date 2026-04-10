/**
 * useTheme Hook
 * Owner: Scenario 16 - Dark/Light Mode Toggle
 *
 * Custom hook for theme management:
 * - Returns current theme and toggle function
 * - Persists preference to localStorage
 * - Respects system preference on first visit
 * - Applies theme class to document
 */

import { useState, useEffect, useCallback } from 'react';
import { Theme } from '../types';
import { THEME_STORAGE_KEY } from '../utils/constants';

interface UseThemeReturn {
  theme: Theme;
  toggleTheme: () => void;
  setTheme: (theme: Theme) => void;
}

/**
 * Get the system color scheme preference
 */
function getSystemPreference(): Theme {
  if (typeof window === 'undefined') {
    return 'light';
  }
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
}

/**
 * Get the stored theme preference from localStorage
 */
function getStoredTheme(): Theme | null {
  if (typeof window === 'undefined') {
    return null;
  }
  try {
    const stored = localStorage.getItem(THEME_STORAGE_KEY);
    if (stored === 'dark' || stored === 'light') {
      return stored;
    }
  } catch {
    // localStorage might be blocked
  }
  return null;
}

/**
 * Apply theme to document
 */
function applyTheme(theme: Theme): void {
  if (typeof document !== 'undefined') {
    document.documentElement.setAttribute('data-theme', theme);
  }
}

/**
 * Custom hook for managing dark/light theme with persistence
 */
export function useTheme(): UseThemeReturn {
  const [theme, setThemeState] = useState<Theme>(() => {
    // First check for stored preference
    const storedTheme = getStoredTheme();
    if (storedTheme) {
      return storedTheme;
    }
    // Fall back to system preference
    return getSystemPreference();
  });

  // Apply theme to document on initial load and when theme changes
  useEffect(() => {
    applyTheme(theme);
  }, [theme]);

  // Listen for system preference changes
  useEffect(() => {
    const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');

    const handleChange = (e: MediaQueryListEvent) => {
      // Only update if user hasn't set a manual preference
      const storedTheme = getStoredTheme();
      if (!storedTheme) {
        setThemeState(e.matches ? 'dark' : 'light');
      }
    };

    mediaQuery.addEventListener('change', handleChange);
    return () => mediaQuery.removeEventListener('change', handleChange);
  }, []);

  // Set theme and persist to localStorage
  const setTheme = useCallback((newTheme: Theme) => {
    setThemeState(newTheme);
    try {
      localStorage.setItem(THEME_STORAGE_KEY, newTheme);
    } catch {
      // localStorage might be blocked
    }
  }, []);

  // Toggle between light and dark themes
  const toggleTheme = useCallback(() => {
    setTheme(theme === 'light' ? 'dark' : 'light');
  }, [theme, setTheme]);

  return {
    theme,
    toggleTheme,
    setTheme,
  };
}

export default useTheme;
