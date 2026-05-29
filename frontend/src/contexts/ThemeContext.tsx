import { createContext, useContext, useState, useCallback, useMemo, ReactNode, useEffect } from 'react';

export type Theme = 'light' | 'dark' | 'cyberpunk' | 'synthwave';

export interface ThemeContextType {
  theme: Theme;
  setTheme: (theme: Theme) => void;
  toggleTheme: () => void;
  isDark: boolean;
}

const ThemeContext = createContext<ThemeContextType>({
  theme: 'light',
  setTheme: () => {},
  toggleTheme: () => {},
  isDark: false,
});

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [theme, setThemeState] = useState<Theme>('light');

  useEffect(() => {
    try {
      const saved = localStorage.getItem('app-theme');
      if (saved && ['light', 'dark', 'cyberpunk', 'synthwave'].includes(saved)) {
        setThemeState(saved as Theme);
      } else if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
        setThemeState('dark');
      }
    } catch {
      // localStorage not available (e.g., SSR, private mode)
    }
  }, []);

  const isDark = theme === 'dark' || theme === 'cyberpunk' || theme === 'synthwave';

  const handleSetTheme = useCallback((newTheme: Theme) => {
    setThemeState(newTheme);
    try {
      localStorage.setItem('app-theme', newTheme);
    } catch {
      // localStorage not available
    }
    if (typeof document !== 'undefined') {
      document.documentElement.setAttribute('data-theme', newTheme);
      if (newTheme === 'dark' || newTheme === 'cyberpunk' || newTheme === 'synthwave') {
        document.documentElement.classList.add('dark');
      } else {
        document.documentElement.classList.remove('dark');
      }
    }
  }, []);

  const toggleTheme = useCallback(() => {
    const themes: Theme[] = ['light', 'dark', 'cyberpunk', 'synthwave'];
    const currentIndex = themes.indexOf(theme);
    const nextTheme = themes[(currentIndex + 1) % themes.length];
    handleSetTheme(nextTheme);
  }, [theme, handleSetTheme]);

  const value = useMemo(
    () => ({
      theme,
      setTheme: handleSetTheme,
      toggleTheme,
      isDark,
    }),
    [theme, handleSetTheme, toggleTheme, isDark]
  );

  return <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>;
}

export function useTheme(): ThemeContextType {
  return useContext(ThemeContext);
}
