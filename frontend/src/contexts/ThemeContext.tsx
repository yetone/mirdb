/**
 * Theme provider supporting all DaisyUI / Tailwind themes.
 * Owner: Scenario 6 - Theme System Compatibility
 *
 * Expected exports:
 * - ThemeProvider
 * - useTheme(): { theme: Theme; setTheme: (t: Theme) => void }
 * - Persists selection in localStorage
 */

import React, { createContext, useContext, useState, useEffect } from 'react';
import type { Theme } from '../types';
import { DEFAULT_THEME, SUPPORTED_THEMES } from '../utils/constants';

interface ThemeContextValue {
  theme: Theme;
  setTheme: (theme: Theme) => void;
}

const ThemeContext = createContext<ThemeContextValue>({
  theme: DEFAULT_THEME,
  setTheme: () => {},
});

function isValidTheme(value: string | null): value is Theme {
  return value !== null && (SUPPORTED_THEMES as readonly string[]).includes(value);
}

function getInitialTheme(): Theme {
  if (typeof window === 'undefined') {
    return DEFAULT_THEME;
  }
  const stored = localStorage.getItem('theme');
  return isValidTheme(stored) ? stored : DEFAULT_THEME;
}

export function ThemeProvider({ children }: { children: React.ReactNode }) {
  const [theme, setThemeState] = useState<Theme>(getInitialTheme);

  const setTheme = (newTheme: Theme) => {
    setThemeState(newTheme);
    if (typeof window !== 'undefined') {
      localStorage.setItem('theme', newTheme);
    }
  };

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme);
  }, [theme]);

  return (
    <ThemeContext.Provider value={{ theme, setTheme }}>
      {children}
    </ThemeContext.Provider>
  );
}

export function useTheme(): ThemeContextValue {
  return useContext(ThemeContext);
}
