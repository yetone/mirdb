/**
 * Theme Toggle Component
 *
 * Dropdown component for switching between themes.
 * Uses the ThemeContext to manage theme state.
 * Supports: light, dark, cyberpunk, synthwave themes.
 */

import { useTheme } from '../contexts/ThemeContext'
import { Palette } from 'lucide-react'

type Theme = 'light' | 'dark' | 'cyberpunk' | 'synthwave' | 'retro' | 'valentine'

const themes: { value: Theme; label: string }[] = [
  { value: 'light', label: 'Light' },
  { value: 'dark', label: 'Dark' },
  { value: 'cyberpunk', label: 'Cyberpunk' },
  { value: 'synthwave', label: 'Synthwave' },
]

export interface ThemeToggleProps {
  className?: string
}

export function ThemeToggle({ className = '' }: ThemeToggleProps) {
  const { theme, setTheme } = useTheme()

  return (
    <div className={`dropdown dropdown-end ${className}`} data-testid="theme-toggle">
      <label
        tabIndex={0}
        className="btn btn-ghost btn-circle"
        aria-label="Change theme"
      >
        <Palette className="w-5 h-5" />
      </label>
      <ul
        tabIndex={0}
        className="dropdown-content z-[100] menu p-2 shadow-lg bg-base-200 rounded-box w-52"
        data-testid="theme-menu"
        role="menu"
        aria-label="Theme options"
      >
        {themes.map((t) => (
          <li key={t.value} role="none">
            <button
              role="menuitem"
              className={`${theme === t.value ? 'active' : ''}`}
              onClick={() => setTheme(t.value)}
              data-testid={`theme-option-${t.value}`}
              aria-pressed={theme === t.value}
            >
              {t.label}
              {theme === t.value && (
                <span className="badge badge-primary badge-sm ml-auto">Active</span>
              )}
            </button>
          </li>
        ))}
      </ul>
    </div>
  )
}

export default ThemeToggle
