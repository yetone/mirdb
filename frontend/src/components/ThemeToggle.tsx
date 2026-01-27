/**
 * Theme Toggle Component
 * Owner: Scenario 5 - Theme Switching
 *
 * Provides a UI control for switching between themes:
 * - Dropdown to select from all available themes
 * - Quick toggle button for light/dark switching
 * - Visual feedback for current theme
 *
 * Requirements: REQ-8 - Support theme switching (light/dark mode)
 * User Stories: US-6 - Toggle Theme
 */

import { useTheme, Theme, AVAILABLE_THEMES } from '../contexts/ThemeContext'

export interface ThemeToggleProps {
  /** Optional additional CSS classes */
  className?: string
  /** Show dropdown with all themes (default: true) */
  showDropdown?: boolean
}

/** Theme display names and icons */
const THEME_CONFIG: Record<Theme, { label: string; icon: string }> = {
  light: { label: 'Light', icon: '☀️' },
  dark: { label: 'Dark', icon: '🌙' },
  cyberpunk: { label: 'Cyberpunk', icon: '🤖' },
  synthwave: { label: 'Synthwave', icon: '🌆' },
}

export function ThemeToggle({ className = '', showDropdown = true }: ThemeToggleProps) {
  const { theme, setTheme, toggleTheme } = useTheme()

  if (!showDropdown) {
    // Simple toggle button for light/dark only
    return (
      <button
        type="button"
        className={`btn btn-ghost btn-circle ${className}`}
        onClick={toggleTheme}
        aria-label={`Switch to ${theme === 'light' ? 'dark' : 'light'} theme`}
        data-testid="theme-toggle-button"
      >
        <span className="text-xl" aria-hidden="true">
          {theme === 'light' ? '🌙' : '☀️'}
        </span>
      </button>
    )
  }

  return (
    <div className={`dropdown dropdown-end ${className}`} data-testid="theme-toggle">
      <button
        type="button"
        tabIndex={0}
        className="btn btn-ghost"
        aria-label="Select theme"
        aria-haspopup="listbox"
        data-testid="theme-toggle-button"
      >
        <span className="text-xl" aria-hidden="true">
          {THEME_CONFIG[theme].icon}
        </span>
        <span className="hidden sm:inline ml-1">{THEME_CONFIG[theme].label}</span>
        <svg
          xmlns="http://www.w3.org/2000/svg"
          fill="none"
          viewBox="0 0 24 24"
          className="w-4 h-4 stroke-current ml-1"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth="2"
            d="M19 9l-7 7-7-7"
          />
        </svg>
      </button>
      <ul
        tabIndex={0}
        className="dropdown-content menu p-2 shadow-lg bg-base-100 rounded-box w-52 mt-2 z-50"
        role="listbox"
        aria-label="Theme options"
        data-testid="theme-dropdown-menu"
      >
        {AVAILABLE_THEMES.map((themeOption) => (
          <li key={themeOption} role="option" aria-selected={theme === themeOption}>
            <button
              type="button"
              className={`justify-between ${theme === themeOption ? 'active' : ''}`}
              onClick={() => setTheme(themeOption)}
              data-testid={`theme-option-${themeOption}`}
            >
              <span className="flex items-center gap-2">
                <span aria-hidden="true">{THEME_CONFIG[themeOption].icon}</span>
                {THEME_CONFIG[themeOption].label}
              </span>
              {theme === themeOption && (
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  fill="none"
                  viewBox="0 0 24 24"
                  className="w-4 h-4 stroke-current"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth="2"
                    d="M5 13l4 4L19 7"
                  />
                </svg>
              )}
            </button>
          </li>
        ))}
      </ul>
    </div>
  )
}

export default ThemeToggle
