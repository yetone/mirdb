import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  ReactNode,
} from 'react';
import { THEMES } from '../utils/constants';
import { Theme } from '../types/homepage';

export const THEME_STORAGE_KEY = 'theme';
export const DEFAULT_THEME: Theme = 'light';

export interface ThemeContextValue {
  theme: Theme;
  setTheme: (theme: Theme) => void;
  toggleTheme: () => void;
}

export function isValidTheme(value: unknown): value is Theme {
  return (
    typeof value === 'string' && (THEMES as readonly string[]).includes(value)
  );
}

function readStoredTheme(): Theme {
  if (typeof window === 'undefined') return DEFAULT_THEME;
  try {
    const stored = window.localStorage.getItem(THEME_STORAGE_KEY);
    return isValidTheme(stored) ? stored : DEFAULT_THEME;
  } catch {
    return DEFAULT_THEME;
  }
}

function applyTheme(theme: Theme) {
  if (typeof document !== 'undefined' && document.documentElement) {
    document.documentElement.setAttribute('data-theme', theme);
  }
}

const defaultContextValue: ThemeContextValue = {
  theme: DEFAULT_THEME,
  setTheme: () => {},
  toggleTheme: () => {},
};

const ThemeContext = createContext<ThemeContextValue>(defaultContextValue);

export function useTheme(): ThemeContextValue {
  return useContext(ThemeContext);
}

interface ThemeProviderProps {
  children: ReactNode;
  initialTheme?: Theme;
}

export function ThemeProvider({ children, initialTheme }: ThemeProviderProps) {
  const [theme, setThemeState] = useState<Theme>(() => {
    if (initialTheme && isValidTheme(initialTheme)) return initialTheme;
    return readStoredTheme();
  });

  useEffect(() => {
    applyTheme(theme);
    try {
      window.localStorage.setItem(THEME_STORAGE_KEY, theme);
    } catch {
      // Storage may be unavailable (private mode, quota); ignore.
    }
  }, [theme]);

  const setTheme = useCallback((next: Theme) => {
    if (isValidTheme(next)) {
      setThemeState(next);
      return;
    }
    const env = (globalThis as { process?: { env?: { NODE_ENV?: string } } })
      .process?.env;
    if (env && env.NODE_ENV !== 'production') {
      // eslint-disable-next-line no-console
      console.warn(
        `[ThemeProvider] Ignored invalid theme "${String(next)}". Valid themes: ${THEMES.join(', ')}.`,
      );
    }
  }, []);

  const toggleTheme = useCallback(() => {
    setThemeState((current) => (current === 'dark' ? 'light' : 'dark'));
  }, []);

  const value = useMemo<ThemeContextValue>(
    () => ({ theme, setTheme, toggleTheme }),
    [theme, setTheme, toggleTheme],
  );

  return (
    <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>
  );
}

export default ThemeContext;
