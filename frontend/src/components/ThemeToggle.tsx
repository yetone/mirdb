/**
 * Theme toggle button component
 */

import React from 'react'
import { useTheme } from '@/contexts/ThemeContext'
import { Moon, Sun, Palette } from 'lucide-react'

const ThemeToggle: React.FC = () => {
  const { theme, toggleTheme } = useTheme()

  const getIcon = () => {
    switch (theme) {
      case 'light':
        return <Sun className="w-5 h-5" />
      case 'dark':
        return <Moon className="w-5 h-5" />
      default:
        return <Palette className="w-5 h-5" />
    }
  }

  return (
    <button
      onClick={toggleTheme}
      className="btn btn-ghost btn-circle"
      aria-label="Toggle theme"
    >
      {getIcon()}
    </button>
  )
}

export default ThemeToggle
