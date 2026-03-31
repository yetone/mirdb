/**
 * Theme Hook
 * Owner: Scenario 7 - Dark Mode and Theming
 *
 * Manages theme state and persistence.
 * Respects system preference by default.
 *
 * Requirements: NFR-4, Story 7 (Dark Mode)
 *
 * Features:
 * - System preference detection via prefers-color-scheme
 * - Manual toggle between light and dark themes
 * - Persistence via localStorage
 * - Smooth theme transitions
 */

import { useState, useEffect, useCallback } from 'react';
import type { Theme } from '../types';

const THEME_STORAGE_KEY = 'mirdb-theme';
const TRANSITION_CLASS = 'theme-transition';

/**
 * Gets the system's preferred color scheme
 */
function getSystemTheme(): Theme {
  if (typeof window === 'undefined') return 'light';
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
}

/**
 * Gets the stored theme from localStorage
 */
function getStoredTheme(): Theme | null {
  if (typeof window === 'undefined') return null;
  const stored = localStorage.getItem(THEME_STORAGE_KEY);
  if (stored === 'light' || stored === 'dark') {
    return stored;
  }
  return null;
}

/**
 * Applies the theme to the document
 */
function applyTheme(theme: Theme, animate: boolean = false): void {
  if (typeof document === 'undefined') return;

  const root = document.documentElement;

  if (animate) {
    root.classList.add(TRANSITION_CLASS);
    // Remove transition class after animation completes
    setTimeout(() => {
      root.classList.remove(TRANSITION_CLASS);
    }, 300);
  }

  if (theme === 'dark') {
    root.classList.add('dark');
  } else {
    root.classList.remove('dark');
  }
}

/**
 * Stores the theme preference in localStorage
 */
function storeTheme(theme: Theme): void {
  if (typeof window === 'undefined') return;
  localStorage.setItem(THEME_STORAGE_KEY, theme);
}

export interface UseThemeReturn {
  theme: Theme;
  toggleTheme: () => void;
  setTheme: (theme: Theme) => void;
  systemTheme: Theme;
}

/**
 * Hook for managing theme state with system preference detection and persistence
 *
 * @returns Object containing current theme, toggle function, and setter
 *
 * @example
 * ```tsx
 * const { theme, toggleTheme, setTheme } = useTheme();
 *
 * return (
 *   <button onClick={toggleTheme}>
 *     Current theme: {theme}
 *   </button>
 * );
 * ```
 */
export function useTheme(): UseThemeReturn {
  const [systemTheme, setSystemTheme] = useState<Theme>(() => getSystemTheme());

  // Initialize theme: stored preference > system preference
  const [theme, setThemeState] = useState<Theme>(() => {
    const stored = getStoredTheme();
    return stored ?? getSystemTheme();
  });

  // Apply theme on initial mount and when theme changes
  useEffect(() => {
    applyTheme(theme, false);
  }, []);

  // Listen for system theme changes
  useEffect(() => {
    const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');

    const handleChange = (e: MediaQueryListEvent) => {
      const newSystemTheme: Theme = e.matches ? 'dark' : 'light';
      setSystemTheme(newSystemTheme);

      // Only update theme if user hasn't set a manual preference
      const storedTheme = getStoredTheme();
      if (storedTheme === null) {
        setThemeState(newSystemTheme);
        applyTheme(newSystemTheme, true);
      }
    };

    mediaQuery.addEventListener('change', handleChange);
    return () => mediaQuery.removeEventListener('change', handleChange);
  }, []);

  /**
   * Sets the theme to a specific value
   */
  const setTheme = useCallback((newTheme: Theme) => {
    setThemeState(newTheme);
    storeTheme(newTheme);
    applyTheme(newTheme, true);
  }, []);

  /**
   * Toggles between light and dark themes
   */
  const toggleTheme = useCallback(() => {
    setThemeState((current) => {
      const newTheme: Theme = current === 'light' ? 'dark' : 'light';
      storeTheme(newTheme);
      applyTheme(newTheme, true);
      return newTheme;
    });
  }, []);

  return {
    theme,
    toggleTheme,
    setTheme,
    systemTheme,
  };
}

export default useTheme;
