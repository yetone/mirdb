/**
 * Theme toggle component.
 *
 * Allows users to switch between available themes.
 */

import React from 'react'
import { useTheme } from '../contexts/ThemeContext'

export function ThemeToggle() {
  const { theme, setTheme, availableThemes } = useTheme()

  return (
    <div className="dropdown dropdown-end">
      <label tabIndex={0} className="btn btn-ghost btn-circle" aria-label="Change theme">
        <svg
          xmlns="http://www.w3.org/2000/svg"
          className="h-5 w-5"
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M7 21a4 4 0 01-4-4V5a2 2 0 012-2h4a2 2 0 012 2v12a4 4 0 01-4 4zm0 0h12a2 2 0 002-2v-4a2 2 0 00-2-2h-2.343M11 7.343l1.657-1.657a2 2 0 012.828 0l2.829 2.829a2 2 0 010 2.828l-8.486 8.485M7 17h.01"
          />
        </svg>
      </label>
      <ul
        tabIndex={0}
        className="dropdown-content menu p-2 shadow-lg bg-base-200 rounded-box w-52 z-50"
        role="listbox"
        aria-label="Theme options"
      >
        {availableThemes.map((t) => (
          <li key={t} role="option" aria-selected={theme === t}>
            <button
              onClick={() => setTheme(t)}
              className={theme === t ? 'active' : ''}
            >
              {t.charAt(0).toUpperCase() + t.slice(1)}
            </button>
          </li>
        ))}
      </ul>
    </div>
  )
}
