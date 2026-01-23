import { create } from 'zustand';

type Theme = 'light' | 'dark' | 'cyberpunk' | 'synthwave' | 'retro' | 'valentine' | 'night';

interface ThemeState {
  theme: Theme;
  setTheme: (theme: Theme) => void;
}

interface UIState {
  isSidebarOpen: boolean;
  toggleSidebar: () => void;
  setSidebarOpen: (state: boolean) => void;
}

export const useThemeStore = create<ThemeState>((set) => ({
  theme: (localStorage.getItem('theme') as Theme) || 'light',
  setTheme: (theme) => {
    localStorage.setItem('theme', theme);
    set({ theme });
  },
}));

export const useUIStore = create<UIState>((set) => ({
  isSidebarOpen: false,
  toggleSidebar: () => set((state) => ({ isSidebarOpen: !state.isSidebarOpen })),
  setSidebarOpen: (state) => set({ isSidebarOpen: state }),
}));
