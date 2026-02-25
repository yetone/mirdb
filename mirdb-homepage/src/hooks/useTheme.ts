/**
 * Theme Hook for managing dark/light mode.
 * Owner: Scenario 6 - Theme Toggle
 *
 * Features:
 * - LocalStorage persistence
 * - System preference detection
 * - CSS class management via data-theme attribute on document element
 */

import { useState, useEffect, useCallback } from 'react';
import type { ThemeMode } from '../types';

const STORAGE_KEY = 'mirdb-theme-preference';

export interface UseThemeReturn {
  theme: ThemeMode;
  toggleTheme: () => void;
  setTheme: (theme: ThemeMode) => void;
}

function getSystemTheme(): ThemeMode {
  if (typeof window === 'undefined') return 'light';
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
}

function getStoredTheme(): ThemeMode | null {
  if (typeof window === 'undefined') return null;
  try {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (stored === 'light' || stored === 'dark') {
      return stored;
    }
    return null;
  } catch {
    return null;
  }
}

function applyTheme(theme: ThemeMode): void {
  if (typeof document !== 'undefined') {
    document.documentElement.setAttribute('data-theme', theme);
  }
}

function storeTheme(theme: ThemeMode): void {
  if (typeof window !== 'undefined') {
    try {
      localStorage.setItem(STORAGE_KEY, theme);
    } catch {
      // Silently fail if localStorage is not available
    }
  }
}

export function useTheme(): UseThemeReturn {
  const [theme, setThemeState] = useState<ThemeMode>(() => {
    const stored = getStoredTheme();
    if (stored) return stored;
    return getSystemTheme();
  });

  // Apply theme to document on mount and when theme changes
  useEffect(() => {
    applyTheme(theme);
  }, [theme]);

  const setTheme = useCallback((newTheme: ThemeMode) => {
    setThemeState(newTheme);
    storeTheme(newTheme);
    applyTheme(newTheme);
  }, []);

  const toggleTheme = useCallback(() => {
    const newTheme = theme === 'light' ? 'dark' : 'light';
    setTheme(newTheme);
  }, [theme, setTheme]);

  return { theme, toggleTheme, setTheme };
}
