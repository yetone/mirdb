import React from 'react';
import { useTheme, ThemeProvider } from '../../hooks/useTheme';
import { Sun, Moon } from 'lucide-react';

export { useTheme, ThemeProvider };

export default function ThemeToggle() {
  const { theme, toggleTheme } = useTheme();

  return (
    <button
      onClick={toggleTheme}
      aria-label={`Switch to ${theme === 'light' ? 'dark' : 'light'} mode`}
      className="p-2 rounded-lg bg-gray-200 dark:bg-gray-700 text-gray-800 dark:text-gray-200 hover:bg-gray-300 dark:hover:bg-gray-600 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-brand-500 transition-colors"
    >
      {theme === 'light' ? <Moon size={20} aria-hidden="true" /> : <Sun size={20} aria-hidden="true" />}
    </button>
  );
}
