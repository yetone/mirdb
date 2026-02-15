/**
 * Theme Store
 * Owner: Scenario 4 - Theme Switching
 *
 * Zustand store for managing theme state across the application.
 * Persists theme selection to localStorage and syncs with DaisyUI theme system.
 */

import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import type { Theme, ThemeState } from '../types/home';
import { AVAILABLE_THEMES } from '../types/home';

const THEME_STORAGE_KEY = 'linksnip-theme';

/**
 * Get the default theme based on system preference or stored value
 */
const getDefaultTheme = (): Theme => {
  // Check localStorage first
  if (typeof window !== 'undefined') {
    const stored = localStorage.getItem(THEME_STORAGE_KEY);
    if (stored) {
      try {
        const parsed = JSON.parse(stored);
        if (parsed?.state?.theme && AVAILABLE_THEMES.includes(parsed.state.theme)) {
          return parsed.state.theme;
        }
      } catch {
        // Invalid JSON, ignore
      }
    }

    // Check system preference
    if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
      return 'dark';
    }
  }
  return 'light';
};

/**
 * Apply theme to the document by setting the data-theme attribute
 */
const applyTheme = (theme: Theme): void => {
  if (typeof document !== 'undefined') {
    document.documentElement.setAttribute('data-theme', theme);
  }
};

/**
 * Theme store using Zustand with persistence
 */
export const useThemeStore = create<ThemeState>()(
  persist(
    (set, get) => ({
      theme: getDefaultTheme(),

      setTheme: (theme: Theme) => {
        applyTheme(theme);
        set({ theme });
      },

      toggleTheme: () => {
        const currentTheme = get().theme;
        const currentIndex = AVAILABLE_THEMES.indexOf(currentTheme);
        const nextIndex = (currentIndex + 1) % AVAILABLE_THEMES.length;
        const nextTheme = AVAILABLE_THEMES[nextIndex];
        applyTheme(nextTheme);
        set({ theme: nextTheme });
      },
    }),
    {
      name: THEME_STORAGE_KEY,
      onRehydrateStorage: () => (state) => {
        // Apply theme on rehydration
        if (state?.theme) {
          applyTheme(state.theme);
        }
      },
    }
  )
);

// Apply initial theme on module load
if (typeof document !== 'undefined') {
  const initialTheme = getDefaultTheme();
  applyTheme(initialTheme);
}

export default useThemeStore;
