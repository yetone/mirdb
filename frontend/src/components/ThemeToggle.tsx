/**
 * Theme Toggle Component
 *
 * A dropdown button that allows users to switch between available themes.
 * Uses the ThemeContext to manage theme state.
 *
 * Requirements: REQ-8 - Support theme switching (light/dark mode)
 * User Story: US-6 - Toggle Theme
 */

import { useTheme, type ThemeName } from '../contexts/ThemeContext'

export interface ThemeToggleProps {
  className?: string
}

const THEME_ICONS: Record<ThemeName, string> = {
  light: '☀️',
  dark: '🌙',
  cyberpunk: '🤖',
  synthwave: '🌆',
}

const THEME_LABELS: Record<ThemeName, string> = {
  light: 'Light',
  dark: 'Dark',
  cyberpunk: 'Cyberpunk',
  synthwave: 'Synthwave',
}

export function ThemeToggle({ className = '' }: ThemeToggleProps) {
  const { theme, setTheme, availableThemes } = useTheme()

  return (
    <div className={`dropdown dropdown-end ${className}`} data-testid="theme-toggle">
      <button
        tabIndex={0}
        className="btn btn-ghost btn-circle"
        aria-label={`Current theme: ${THEME_LABELS[theme]}. Click to change theme.`}
        aria-haspopup="listbox"
        data-testid="theme-toggle-button"
      >
        <span className="text-xl" aria-hidden="true">
          {THEME_ICONS[theme]}
        </span>
      </button>
      <ul
        tabIndex={0}
        className="dropdown-content menu p-2 shadow-lg bg-base-100 rounded-box w-52 mt-3"
        role="listbox"
        aria-label="Select theme"
        data-testid="theme-dropdown"
      >
        {availableThemes.map((themeName) => (
          <li key={themeName} role="option" aria-selected={theme === themeName}>
            <button
              onClick={() => setTheme(themeName)}
              className={`justify-between ${theme === themeName ? 'active' : ''}`}
              data-testid={`theme-option-${themeName}`}
              aria-label={`Switch to ${THEME_LABELS[themeName]} theme`}
            >
              <span>{THEME_LABELS[themeName]}</span>
              <span aria-hidden="true">{THEME_ICONS[themeName]}</span>
            </button>
          </li>
        ))}
      </ul>
    </div>
  )
}

export default ThemeToggle
