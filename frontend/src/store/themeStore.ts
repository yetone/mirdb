import { create } from 'zustand';

export type Theme = 'light' | 'dark' | 'cyberpunk' | 'synthwave' | 'retro' | 'valentine' | 'night';

export const AVAILABLE_THEMES: Theme[] = ['light', 'dark', 'cyberpunk', 'synthwave', 'retro', 'valentine', 'night'];

interface ThemeState {
  theme: Theme;
  setTheme: (theme: Theme) => void;
}

const getInitialTheme = (): Theme => {
  if (typeof window === 'undefined') return 'light';
  const stored = localStorage.getItem('theme') as Theme | null;
  if (stored && AVAILABLE_THEMES.includes(stored)) {
    return stored;
  }
  return 'light';
};

export const useThemeStore = create<ThemeState>((set) => ({
  theme: getInitialTheme(),
  setTheme: (theme: Theme) => {
    localStorage.setItem('theme', theme);
    set({ theme });
  },
}));
