/**
 * Theme Context Provider
 * Owner: Scenario 5 - Dark/Light Mode Theme Support
 *
 * Provides theme state and controls to the entire application:
 * - Current theme value
 * - System preference
 * - Theme setter function
 *
 * Requirements: REQ-5, US-4
 */
import React, { createContext, useContext, ReactNode } from 'react';
import { useTheme, UseThemeReturn, Theme } from '../hooks/useTheme';

const ThemeContext = createContext<UseThemeReturn | null>(null);

export interface ThemeProviderProps {
  children: ReactNode;
}

export function ThemeProvider({ children }: ThemeProviderProps): React.ReactElement {
  const themeState = useTheme();

  return (
    <ThemeContext.Provider value={themeState}>
      {children}
    </ThemeContext.Provider>
  );
}

export function useThemeContext(): UseThemeReturn {
  const context = useContext(ThemeContext);
  if (context === null) {
    throw new Error('useThemeContext must be used within a ThemeProvider');
  }
  return context;
}

export type { Theme };
