/**
 * Theme Management Hook.
 * Owner: Scenario 7 - Dark Mode Toggle
 *
 * Manages light/dark theme state with localStorage persistence.
 * Defaults to dark mode as per PRD requirements.
 */

import { useState, useEffect, useCallback } from 'react';
import type { Theme } from '../types';
import { getStoredTheme, saveTheme, applyTheme, getDefaultTheme } from '../utils/theme';

export interface UseThemeReturn {
  /** Current theme ('light' or 'dark') */
  theme: Theme;
  /** Toggles between light and dark themes */
  toggleTheme: () => void;
  /** Sets a specific theme */
  setTheme: (theme: Theme) => void;
}

/**
 * Hook for managing theme state with persistence.
 * @returns Object containing theme state and control functions.
 */
export function useTheme(): UseThemeReturn {
  // Initialize with default theme (dark per PRD)
  const [theme, setThemeState] = useState<Theme>(() => getDefaultTheme());

  // Apply theme to document on mount and when theme changes
  useEffect(() => {
    applyTheme(theme);
  }, [theme]);

  // Load stored preference on mount
  useEffect(() => {
    const stored = getStoredTheme();
    if (stored) {
      setThemeState(stored);
    }
  }, []);

  const setTheme = useCallback((newTheme: Theme) => {
    setThemeState(newTheme);
    saveTheme(newTheme);
  }, []);

  const toggleTheme = useCallback(() => {
    setThemeState((current) => {
      const newTheme: Theme = current === 'dark' ? 'light' : 'dark';
      saveTheme(newTheme);
      return newTheme;
    });
  }, []);

  return { theme, toggleTheme, setTheme };
}
