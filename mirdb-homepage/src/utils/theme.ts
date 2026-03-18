/**
 * Theme Persistence Utilities.
 * Owner: Scenario 7 - Dark Mode Toggle
 *
 * Provides functions for saving and loading theme preferences
 * from localStorage, and applying the theme class to the document.
 */

import { THEME_KEY } from './constants';
import type { Theme } from '../types';

/**
 * Gets the saved theme preference from localStorage.
 * @returns The saved theme or null if not set.
 */
export function getStoredTheme(): Theme | null {
  if (typeof window === 'undefined') {
    return null;
  }
  const stored = localStorage.getItem(THEME_KEY);
  if (stored === 'light' || stored === 'dark') {
    return stored;
  }
  return null;
}

/**
 * Saves the theme preference to localStorage.
 * @param theme - The theme to save ('light' or 'dark').
 */
export function saveTheme(theme: Theme): void {
  if (typeof window === 'undefined') {
    return;
  }
  localStorage.setItem(THEME_KEY, theme);
}

/**
 * Applies the theme by adding/removing the 'dark' class on the document root.
 * @param theme - The theme to apply.
 */
export function applyTheme(theme: Theme): void {
  if (typeof document === 'undefined') {
    return;
  }
  const root = document.documentElement;
  if (theme === 'dark') {
    root.classList.add('dark');
  } else {
    root.classList.remove('dark');
  }
}

/**
 * Gets the default theme based on system preference or falls back to dark.
 * @returns The detected system theme or 'dark' as default.
 */
export function getDefaultTheme(): Theme {
  // Per PRD requirements, dark mode is the default
  if (typeof window === 'undefined') {
    return 'dark';
  }

  // Check if user has a stored preference
  const stored = getStoredTheme();
  if (stored) {
    return stored;
  }

  // Default to dark mode as per PRD
  return 'dark';
}
