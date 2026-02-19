/**
 * Theme Toggle Component
 * Owner: Scenario 6 - Dark Mode & Theme System
 *
 * Button to toggle between light and dark themes:
 * - Sun/moon icon based on current theme
 * - Accessible label for screen readers
 * - Smooth transition animation
 *
 * Requirements: REQ-11
 * User Story: US-7
 */
import { useTheme } from '@/hooks/useTheme'
import { Icon } from '@/components/ui/Icon/Icon'
import styles from './ThemeToggle.module.css'

export function ThemeToggle() {
  const { theme, toggleTheme } = useTheme()

  // Show sun icon in dark mode (to switch to light)
  // Show moon icon in light mode (to switch to dark)
  const iconName = theme === 'dark' ? 'sun' : 'moon'
  const ariaLabel = theme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'

  return (
    <button
      className={styles.toggle}
      onClick={toggleTheme}
      aria-label={ariaLabel}
      data-testid="theme-toggle"
      type="button"
    >
      <Icon name={iconName} size={20} />
    </button>
  )
}
