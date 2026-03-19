/**
 * Theme utilities for MirDB Homepage.
 */

import type { Theme } from '@/types';

export const THEME_STORAGE_KEY = 'mirdb-theme';

export function getStoredTheme(): Theme | null {
  if (typeof window === 'undefined') return null;
  return localStorage.getItem(THEME_STORAGE_KEY) as Theme | null;
}

export function setStoredTheme(theme: Theme): void {
  if (typeof window === 'undefined') return;
  localStorage.setItem(THEME_STORAGE_KEY, theme);
}

export function getSystemTheme(): 'light' | 'dark' {
  if (typeof window === 'undefined') return 'light';
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
}
