/**
 * Theme Hook
 * Owner: Scenario 5 - Dark/Light Mode Theme Support
 *
 * Custom hook for detecting and managing theme:
 * - Detects system color scheme preference
 * - Provides current theme value
 * - Listens for system preference changes
 *
 * Requirements: REQ-5, US-4
 */
import { useState, useEffect, useCallback } from 'react';

export type Theme = 'light' | 'dark';

export interface UseThemeReturn {
  theme: Theme;
  systemPreference: Theme;
  setTheme: (theme: Theme) => void;
}

const MEDIA_QUERY = '(prefers-color-scheme: dark)';

function getSystemPreference(): Theme {
  if (typeof window === 'undefined') {
    return 'light';
  }
  return window.matchMedia(MEDIA_QUERY).matches ? 'dark' : 'light';
}

function applyTheme(theme: Theme): void {
  if (typeof document !== 'undefined') {
    document.documentElement.setAttribute('data-theme', theme);
  }
}

export function useTheme(): UseThemeReturn {
  const [systemPreference, setSystemPreference] = useState<Theme>(getSystemPreference);
  const [theme, setThemeState] = useState<Theme>(getSystemPreference);

  const setTheme = useCallback((newTheme: Theme) => {
    setThemeState(newTheme);
    applyTheme(newTheme);
  }, []);

  useEffect(() => {
    // Apply initial theme
    applyTheme(theme);

    // Listen for system preference changes
    const mediaQuery = window.matchMedia(MEDIA_QUERY);

    const handleChange = (event: MediaQueryListEvent) => {
      const newPreference: Theme = event.matches ? 'dark' : 'light';
      setSystemPreference(newPreference);
      setThemeState(newPreference);
      applyTheme(newPreference);
    };

    // Add event listener (using modern API with fallback)
    if (mediaQuery.addEventListener) {
      mediaQuery.addEventListener('change', handleChange);
    } else {
      // Fallback for older browsers
      mediaQuery.addListener(handleChange);
    }

    return () => {
      if (mediaQuery.removeEventListener) {
        mediaQuery.removeEventListener('change', handleChange);
      } else {
        mediaQuery.removeListener(handleChange);
      }
    };
  }, [theme]);

  return {
    theme,
    systemPreference,
    setTheme,
  };
}
