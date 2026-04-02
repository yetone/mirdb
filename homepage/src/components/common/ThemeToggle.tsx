/**
 * Theme toggle component for dark/light mode.
 * Owner: Scenario 11 - Dark Mode Theme Support
 *
 * Displays a sun/moon icon button that toggles between themes.
 * Uses useTheme hook for state management and persistence.
 */

import React from 'react';
import { useTheme } from '@/hooks/useTheme';
import styles from './ThemeToggle.module.css';

interface ThemeToggleProps {
  className?: string;
}

export function ThemeToggle({ className }: ThemeToggleProps) {
  const { theme, toggleTheme, mounted } = useTheme();

  // Prevent hydration mismatch by not rendering until mounted
  if (!mounted) {
    return (
      <button
        type="button"
        className={`${styles.themeToggle} ${className || ''}`}
        aria-label="Toggle theme"
        data-testid="theme-toggle"
        disabled
      >
        <span className={styles.icon} aria-hidden="true">
          <SunIcon />
        </span>
      </button>
    );
  }

  const isDark = theme === 'dark';

  return (
    <button
      type="button"
      onClick={toggleTheme}
      className={`${styles.themeToggle} ${className || ''}`}
      aria-label={isDark ? 'Switch to light mode' : 'Switch to dark mode'}
      data-testid="theme-toggle"
    >
      <span className={styles.icon} aria-hidden="true">
        {isDark ? <MoonIcon /> : <SunIcon />}
      </span>
    </button>
  );
}

function SunIcon() {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width="24"
      height="24"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      data-testid="sun-icon"
    >
      <circle cx="12" cy="12" r="5" />
      <line x1="12" y1="1" x2="12" y2="3" />
      <line x1="12" y1="21" x2="12" y2="23" />
      <line x1="4.22" y1="4.22" x2="5.64" y2="5.64" />
      <line x1="18.36" y1="18.36" x2="19.78" y2="19.78" />
      <line x1="1" y1="12" x2="3" y2="12" />
      <line x1="21" y1="12" x2="23" y2="12" />
      <line x1="4.22" y1="19.78" x2="5.64" y2="18.36" />
      <line x1="18.36" y1="5.64" x2="19.78" y2="4.22" />
    </svg>
  );
}

function MoonIcon() {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width="24"
      height="24"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      data-testid="moon-icon"
    >
      <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" />
    </svg>
  );
}
