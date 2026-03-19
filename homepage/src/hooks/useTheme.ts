/**
 * useTheme hook for dark/light mode management.
 * Owner: Scenario 11 - Dark and Light Mode Toggle
 *
 * Returns:
 * - theme: 'light' | 'dark' | 'system'
 * - setTheme: (theme) => void
 * - resolvedTheme: 'light' | 'dark'
 *
 * Features:
 * - System preference detection
 * - LocalStorage persistence
 * - SSR-safe
 *
 * Requirements: REQ-10
 */

'use client';

import { useCallback, useEffect, useState } from 'react';
import type { Theme } from '@/types';
import { getStoredTheme, setStoredTheme, getSystemTheme } from '@/lib/theme';

export interface UseThemeReturn {
  theme: Theme;
  setTheme: (theme: Theme) => void;
  resolvedTheme: 'light' | 'dark';
}

function applyThemeToDocument(resolvedTheme: 'light' | 'dark'): void {
  if (typeof document === 'undefined') return;

  const root = document.documentElement;
  if (resolvedTheme === 'dark') {
    root.classList.add('dark');
  } else {
    root.classList.remove('dark');
  }
}

export function useTheme(): UseThemeReturn {
  const [theme, setThemeState] = useState<Theme>('system');
  const [resolvedTheme, setResolvedTheme] = useState<'light' | 'dark'>('light');
  const [mounted, setMounted] = useState(false);

  // Initialize theme from localStorage on mount
  useEffect(() => {
    setMounted(true);
    const stored = getStoredTheme();
    if (stored) {
      setThemeState(stored);
    }
  }, []);

  // Resolve and apply theme whenever theme or system preference changes
  useEffect(() => {
    if (!mounted) return;

    const updateResolvedTheme = () => {
      const resolved = theme === 'system' ? getSystemTheme() : theme;
      setResolvedTheme(resolved);
      applyThemeToDocument(resolved);
    };

    updateResolvedTheme();

    // Listen for system preference changes when in system mode
    if (theme === 'system') {
      const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
      const handleChange = () => updateResolvedTheme();

      mediaQuery.addEventListener('change', handleChange);
      return () => mediaQuery.removeEventListener('change', handleChange);
    }
  }, [theme, mounted]);

  const setTheme = useCallback((newTheme: Theme) => {
    setThemeState(newTheme);
    setStoredTheme(newTheme);
  }, []);

  return {
    theme,
    setTheme,
    resolvedTheme,
  };
}
