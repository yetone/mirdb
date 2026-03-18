/**
 * Theme Toggle Button Component.
 * Owner: Scenario 7 - Dark Mode Toggle
 *
 * A button that toggles between light and dark themes.
 * Uses sun/moon icons for visual indication of current and target theme.
 * Fully accessible with proper ARIA labels.
 */

import { useTheme } from '../../hooks/useTheme';

/**
 * Sun icon component for light mode indication.
 */
function SunIcon() {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      fill="none"
      viewBox="0 0 24 24"
      strokeWidth={1.5}
      stroke="currentColor"
      className="w-5 h-5"
      aria-hidden="true"
    >
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        d="M12 3v2.25m6.364.386l-1.591 1.591M21 12h-2.25m-.386 6.364l-1.591-1.591M12 18.75V21m-4.773-4.227l-1.591 1.591M5.25 12H3m4.227-4.773L5.636 5.636M15.75 12a3.75 3.75 0 11-7.5 0 3.75 3.75 0 017.5 0z"
      />
    </svg>
  );
}

/**
 * Moon icon component for dark mode indication.
 */
function MoonIcon() {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      fill="none"
      viewBox="0 0 24 24"
      strokeWidth={1.5}
      stroke="currentColor"
      className="w-5 h-5"
      aria-hidden="true"
    >
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        d="M21.752 15.002A9.718 9.718 0 0118 15.75c-5.385 0-9.75-4.365-9.75-9.75 0-1.33.266-2.597.748-3.752A9.753 9.753 0 003 11.25C3 16.635 7.365 21 12.75 21a9.753 9.753 0 009.002-5.998z"
      />
    </svg>
  );
}

export interface ThemeToggleProps {
  /** Optional CSS class name for styling */
  className?: string;
}

/**
 * Theme toggle button that switches between light and dark modes.
 * Displays a sun icon in dark mode (to switch to light) and
 * a moon icon in light mode (to switch to dark).
 */
export function ThemeToggle({ className = '' }: ThemeToggleProps) {
  const { theme, toggleTheme } = useTheme();

  const ariaLabel = theme === 'dark'
    ? 'Switch to light mode'
    : 'Switch to dark mode';

  return (
    <button
      type="button"
      onClick={toggleTheme}
      className={`
        p-2 rounded-lg transition-colors duration-200
        bg-slate-200 dark:bg-slate-700
        hover:bg-slate-300 dark:hover:bg-slate-600
        text-slate-700 dark:text-slate-200
        focus:outline-none focus:ring-2 focus:ring-primary-500 focus:ring-offset-2
        dark:focus:ring-offset-slate-900
        ${className}
      `}
      aria-label={ariaLabel}
      data-testid="theme-toggle"
    >
      {theme === 'dark' ? <SunIcon /> : <MoonIcon />}
    </button>
  );
}
