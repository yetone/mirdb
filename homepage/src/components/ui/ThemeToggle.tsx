/**
 * Theme Toggle Component for dark/light mode.
 * Owner: Scenario 7 - Dark Mode and Light Mode Toggle
 *
 * Features:
 * - Toggle between light and dark themes
 * - Sun/moon icon indication
 * - Accessible button with aria-label
 *
 * Requirements:
 * - REQ-8: Dark mode/light mode toggle
 * - US-6: Theme preference remembered
 */

import { Moon, Sun } from 'lucide-react'
import { useThemeContext } from '@/contexts/ThemeContext'

export interface ThemeToggleProps {
  className?: string
  size?: 'sm' | 'md' | 'lg'
}

const sizeClasses = {
  sm: 'w-8 h-8',
  md: 'w-10 h-10',
  lg: 'w-12 h-12',
}

const iconSizes = {
  sm: 16,
  md: 20,
  lg: 24,
}

export function ThemeToggle({ className = '', size = 'md' }: ThemeToggleProps) {
  const { theme, toggleTheme } = useThemeContext()

  const isDark = theme === 'dark'
  const ariaLabel = isDark ? 'Switch to light mode' : 'Switch to dark mode'

  return (
    <button
      type="button"
      onClick={toggleTheme}
      className={`
        ${sizeClasses[size]}
        inline-flex items-center justify-center
        rounded-lg
        text-gray-600 dark:text-gray-300
        hover:bg-gray-100 dark:hover:bg-gray-800
        hover:text-gray-900 dark:hover:text-white
        focus:outline-none focus:ring-2 focus:ring-primary-500 focus:ring-offset-2
        dark:focus:ring-offset-gray-900
        transition-colors duration-200
        ${className}
      `.trim().replace(/\s+/g, ' ')}
      aria-label={ariaLabel}
      aria-pressed={isDark}
      data-testid="theme-toggle"
    >
      {isDark ? (
        <Sun
          size={iconSizes[size]}
          aria-hidden="true"
          className="transition-transform duration-200"
        />
      ) : (
        <Moon
          size={iconSizes[size]}
          aria-hidden="true"
          className="transition-transform duration-200"
        />
      )}
    </button>
  )
}

export default ThemeToggle
