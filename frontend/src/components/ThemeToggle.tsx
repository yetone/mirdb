import React from 'react'
import { useTheme } from '../contexts/ThemeContext'

const themes = ['light', 'dark', 'cyberpunk', 'synthwave', 'retro', 'valentine', 'night'] as const

export function ThemeToggle() {
  const { theme, setTheme } = useTheme()

  return (
    <select
      value={theme}
      onChange={(e) => setTheme(e.target.value as typeof themes[number])}
      className="select select-bordered select-sm"
    >
      {themes.map((t) => (
        <option key={t} value={t}>
          {t.charAt(0).toUpperCase() + t.slice(1)}
        </option>
      ))}
    </select>
  )
}

export default ThemeToggle
