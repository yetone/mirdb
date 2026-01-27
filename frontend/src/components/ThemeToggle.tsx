/**
 * ThemeToggle Component
 *
 * Purpose: Theme switcher component that allows users to switch between
 * available themes (light, dark, cyberpunk, synthwave, system).
 *
 * Uses ThemeContext for state management and persists preference to localStorage.
 */

import { useTheme, type Theme, type ThemePreference } from '../contexts/ThemeContext'

const THEME_OPTIONS: { value: ThemePreference; label: string }[] = [
  { value: 'light', label: 'Light' },
  { value: 'dark', label: 'Dark' },
  { value: 'cyberpunk', label: 'Cyberpunk' },
  { value: 'synthwave', label: 'Synthwave' },
  { value: 'system', label: 'System' },
]

export function ThemeToggle() {
  const { theme, themePreference, setThemePreference, isDarkMode } = useTheme()

  const handleThemeChange = (newPreference: ThemePreference) => {
    setThemePreference(newPreference)
  }

  // Quick toggle between light and dark
  const toggleDarkMode = () => {
    if (isDarkMode) {
      setThemePreference('light')
    } else {
      setThemePreference('dark')
    }
  }

  return (
    <div className="flex items-center gap-2" data-testid="theme-toggle">
      {/* Quick dark mode toggle button */}
      <button
        onClick={toggleDarkMode}
        className="btn btn-ghost btn-sm btn-circle"
        aria-label={isDarkMode ? 'Switch to light mode' : 'Switch to dark mode'}
        data-testid="theme-toggle-button"
      >
        {isDarkMode ? (
          <svg
            xmlns="http://www.w3.org/2000/svg"
            className="h-5 w-5"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
            data-testid="sun-icon"
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
            className="h-5 w-5"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
            data-testid="moon-icon"
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

      {/* Theme dropdown for all options */}
      <div className="dropdown dropdown-end" data-testid="theme-dropdown">
        <label
          tabIndex={0}
          className="btn btn-ghost btn-sm"
          aria-label="Select theme"
          data-testid="theme-dropdown-trigger"
        >
          <span className="capitalize">{themePreference}</span>
          <svg
            xmlns="http://www.w3.org/2000/svg"
            className="h-4 w-4 ml-1"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M19 9l-7 7-7-7"
            />
          </svg>
        </label>
        <ul
          tabIndex={0}
          className="dropdown-content z-[1] menu p-2 shadow bg-base-100 rounded-box w-40"
          data-testid="theme-options-menu"
        >
          {THEME_OPTIONS.map((option) => (
            <li key={option.value}>
              <button
                onClick={() => handleThemeChange(option.value)}
                className={`${themePreference === option.value ? 'active' : ''}`}
                data-testid={`theme-option-${option.value}`}
                aria-pressed={themePreference === option.value}
              >
                {option.label}
                {themePreference === option.value && (
                  <svg
                    xmlns="http://www.w3.org/2000/svg"
                    className="h-4 w-4 ml-auto"
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                  >
                    <path
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      strokeWidth={2}
                      d="M5 13l4 4L19 7"
                    />
                  </svg>
                )}
              </button>
            </li>
          ))}
        </ul>
      </div>
    </div>
  )
}

export default ThemeToggle
