/**
 * Theme Toggle Component
 * Owner: Scenario 7 - Dark Mode and Theming
 *
 * Toggle button for switching between light and dark themes.
 * Shows sun icon in dark mode (to switch to light)
 * Shows moon icon in light mode (to switch to dark)
 *
 * Requirements: NFR-4, Story 7 (Dark Mode)
 */

import { Sun, Moon } from 'lucide-react';
import { useTheme } from '../../hooks/useTheme';
import type { Theme } from '../../types';

export interface ThemeToggleProps {
  /** Optional theme override (for controlled usage) */
  theme?: Theme;
  /** Optional toggle handler override (for controlled usage) */
  onToggle?: () => void;
  /** Additional CSS classes */
  className?: string;
  /** Size of the toggle button */
  size?: 'sm' | 'md' | 'lg';
  /** Whether to show the theme label */
  showLabel?: boolean;
}

const sizeClasses = {
  sm: 'p-1.5 min-w-[36px] min-h-[36px]',
  md: 'p-2 min-w-[44px] min-h-[44px]',
  lg: 'p-3 min-w-[52px] min-h-[52px]',
};

const iconSizes = {
  sm: 'w-4 h-4',
  md: 'w-5 h-5',
  lg: 'w-6 h-6',
};

/**
 * A toggle button for switching between light and dark themes.
 *
 * @example
 * ```tsx
 * // Uncontrolled usage (uses useTheme hook internally)
 * <ThemeToggle />
 *
 * // Controlled usage
 * <ThemeToggle theme="dark" onToggle={() => setTheme('light')} />
 * ```
 */
export function ThemeToggle({
  theme: themeProp,
  onToggle: onToggleProp,
  className = '',
  size = 'md',
  showLabel = false,
}: ThemeToggleProps) {
  const hookResult = useTheme();

  // Use props if provided, otherwise use hook values
  const theme = themeProp ?? hookResult.theme;
  const onToggle = onToggleProp ?? hookResult.toggleTheme;

  const isDark = theme === 'dark';
  const Icon = isDark ? Sun : Moon;
  const label = isDark ? 'Switch to light mode' : 'Switch to dark mode';

  return (
    <button
      type="button"
      onClick={onToggle}
      className={`
        inline-flex items-center justify-center
        rounded-lg
        text-gray-500 hover:text-gray-700
        dark:text-gray-400 dark:hover:text-gray-200
        bg-gray-100 hover:bg-gray-200
        dark:bg-gray-800 dark:hover:bg-gray-700
        transition-all duration-200 ease-in-out
        focus:outline-none focus:ring-2 focus:ring-primary-500 focus:ring-offset-2
        dark:focus:ring-offset-gray-900
        ${sizeClasses[size]}
        ${className}
      `}
      aria-label={label}
      title={label}
      data-testid="theme-toggle"
      data-theme={theme}
    >
      <Icon
        className={`
          ${iconSizes[size]}
          transition-transform duration-200
          ${isDark ? 'rotate-0' : 'rotate-0'}
        `}
        aria-hidden="true"
      />
      {showLabel && (
        <span className="ml-2 text-sm font-medium">
          {isDark ? 'Light' : 'Dark'}
        </span>
      )}
    </button>
  );
}

export default ThemeToggle;
