/**
 * Theme Toggle Component (React Island).
 * Owner: Scenario 11 - Dark Mode Support
 *
 * Features:
 * - Toggle between light/dark themes
 * - Persist preference in localStorage
 * - Respect system prefers-color-scheme
 * - Sun/moon icons for visual feedback
 */
import { useState, useEffect } from 'react';

type Theme = 'dark' | 'light';

const STORAGE_KEY = 'mirdb-theme';

function getInitialTheme(): Theme {
  if (typeof window === 'undefined') {
    return 'dark';
  }

  const stored = localStorage.getItem(STORAGE_KEY);
  if (stored === 'light' || stored === 'dark') {
    return stored;
  }

  if (window.matchMedia('(prefers-color-scheme: light)').matches) {
    return 'light';
  }

  return 'dark';
}

function applyTheme(theme: Theme): void {
  const html = document.documentElement;
  if (theme === 'dark') {
    html.classList.add('dark');
    html.classList.remove('light');
  } else {
    html.classList.add('light');
    html.classList.remove('dark');
  }
}

export default function ThemeToggle() {
  const [theme, setTheme] = useState<Theme>('dark');
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    const initialTheme = getInitialTheme();
    setTheme(initialTheme);
    applyTheme(initialTheme);
    setMounted(true);

    const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
    const handleChange = (e: MediaQueryListEvent) => {
      const stored = localStorage.getItem(STORAGE_KEY);
      if (!stored) {
        const newTheme = e.matches ? 'dark' : 'light';
        setTheme(newTheme);
        applyTheme(newTheme);
      }
    };

    mediaQuery.addEventListener('change', handleChange);
    return () => mediaQuery.removeEventListener('change', handleChange);
  }, []);

  const toggleTheme = () => {
    const newTheme: Theme = theme === 'dark' ? 'light' : 'dark';
    setTheme(newTheme);
    applyTheme(newTheme);
    localStorage.setItem(STORAGE_KEY, newTheme);
  };

  if (!mounted) {
    return (
      <button
        aria-label="Toggle theme"
        className="p-2 rounded-lg bg-surface hover:bg-border transition-colors"
        disabled
      >
        <span className="w-5 h-5 block" />
      </button>
    );
  }

  return (
    <button
      onClick={toggleTheme}
      aria-label={theme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'}
      data-testid="theme-toggle"
      data-theme={theme}
      className="p-2 rounded-lg bg-surface hover:bg-border transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-accent"
    >
      {theme === 'dark' ? (
        <svg
          xmlns="http://www.w3.org/2000/svg"
          className="w-5 h-5 text-text-primary"
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
          aria-hidden="true"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M12 3v1m0 16v1m9-9h-1M4 12H3m15.364 6.364l-.707-.707M6.343 6.343l-.707-.707m12.728 0l-.707.707M6.343 17.657l-.707.707M16 12a4 4 0 11-8 0 4 4 0 018 0z"
          />
        </svg>
      ) : (
        <svg
          xmlns="http://www.w3.org/2000/svg"
          className="w-5 h-5 text-text-primary"
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
          aria-hidden="true"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z"
          />
        </svg>
      )}
    </button>
  );
}
