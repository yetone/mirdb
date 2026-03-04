/**
 * Theme Toggle Component
 *
 * Provides a UI control for switching between available themes.
 * Supports light, dark, cyberpunk, and synthwave themes.
 *
 * Requirements:
 * - REQ-7: Theme adaptation for user preferences
 * - NFR-1: WCAG 2.1 AA accessibility
 */
import { motion } from 'framer-motion'
import { Sun, Moon, Palette } from 'lucide-react'
import { useSafeTheme, Theme } from '../contexts/ThemeContext'

export interface ThemeToggleProps {
  showLabel?: boolean
  className?: string
}

const themeIcons: Record<Theme, React.ReactNode> = {
  light: <Sun className="w-5 h-5" aria-hidden="true" />,
  dark: <Moon className="w-5 h-5" aria-hidden="true" />,
  cyberpunk: <Palette className="w-5 h-5" aria-hidden="true" />,
  synthwave: <Palette className="w-5 h-5" aria-hidden="true" />,
}

const themeLabels: Record<Theme, string> = {
  light: 'Light',
  dark: 'Dark',
  cyberpunk: 'Cyberpunk',
  synthwave: 'Synthwave',
}

export function ThemeToggle({ showLabel = false, className = '' }: ThemeToggleProps) {
  const { theme, setTheme, availableThemes } = useSafeTheme()

  return (
    <div
      className={`dropdown dropdown-end ${className}`}
      data-testid="theme-toggle"
    >
      <motion.button
        tabIndex={0}
        className="btn btn-ghost btn-sm gap-2"
        aria-label={`Current theme: ${themeLabels[theme]}. Click to change theme.`}
        aria-haspopup="listbox"
        data-testid="theme-toggle-button"
        whileHover={{ scale: 1.05 }}
        whileTap={{ scale: 0.95 }}
      >
        <motion.span
          key={theme}
          initial={{ rotate: -180, opacity: 0 }}
          animate={{ rotate: 0, opacity: 1 }}
          transition={{ duration: 0.3 }}
        >
          {themeIcons[theme]}
        </motion.span>
        {showLabel && <span className="hidden sm:inline">{themeLabels[theme]}</span>}
      </motion.button>
      <ul
        tabIndex={0}
        className="dropdown-content menu p-2 shadow-lg bg-base-200 rounded-box w-40 z-50"
        role="listbox"
        aria-label="Select theme"
        data-testid="theme-dropdown"
      >
        {availableThemes.map((t) => (
          <li key={t} role="option" aria-selected={theme === t}>
            <button
              className={`flex items-center gap-2 ${theme === t ? 'active' : ''}`}
              onClick={() => setTheme(t)}
              data-testid={`theme-option-${t}`}
              aria-label={`Switch to ${themeLabels[t]} theme`}
            >
              {themeIcons[t]}
              <span>{themeLabels[t]}</span>
              {theme === t && (
                <motion.span
                  className="ml-auto text-primary"
                  initial={{ scale: 0 }}
                  animate={{ scale: 1 }}
                >
                  ✓
                </motion.span>
              )}
            </button>
          </li>
        ))}
      </ul>
    </div>
  )
}

export default ThemeToggle
