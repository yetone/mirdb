/**
 * Theme Toggle component.
 * Owner: Scenario 11 - Dark and Light Mode Toggle
 *
 * Features:
 * - Sun/Moon icon toggle
 * - Accessible button
 * - Persists preference to localStorage
 *
 * Requirements: REQ-10
 */

'use client';

import { Moon, Sun } from 'lucide-react';
import { useTheme } from '@/hooks/useTheme';
import { cn } from '@/lib/utils';

export interface ThemeToggleProps {
  className?: string;
}

export function ThemeToggle({ className }: ThemeToggleProps) {
  const { resolvedTheme, setTheme } = useTheme();

  const toggleTheme = () => {
    setTheme(resolvedTheme === 'light' ? 'dark' : 'light');
  };

  const isDark = resolvedTheme === 'dark';

  return (
    <button
      type="button"
      onClick={toggleTheme}
      className={cn(
        'relative inline-flex items-center justify-center',
        'w-10 h-10 rounded-full',
        'bg-gray-100 dark:bg-gray-800',
        'text-gray-700 dark:text-gray-200',
        'hover:bg-gray-200 dark:hover:bg-gray-700',
        'focus:outline-none focus:ring-2 focus:ring-primary-500 focus:ring-offset-2',
        'dark:focus:ring-offset-gray-900',
        'transition-colors duration-200',
        className
      )}
      aria-label={isDark ? 'Switch to light mode' : 'Switch to dark mode'}
      aria-pressed={isDark}
      data-testid="theme-toggle"
    >
      <Sun
        className={cn(
          'h-5 w-5 transition-all duration-200',
          isDark ? 'scale-0 rotate-90 opacity-0' : 'scale-100 rotate-0 opacity-100'
        )}
        aria-hidden="true"
      />
      <Moon
        className={cn(
          'absolute h-5 w-5 transition-all duration-200',
          isDark ? 'scale-100 rotate-0 opacity-100' : 'scale-0 -rotate-90 opacity-0'
        )}
        aria-hidden="true"
      />
    </button>
  );
}
