import { motion } from 'framer-motion'
import { Sun, Moon, Monitor, Sparkles, Waves } from 'lucide-react'
import { useTheme, ThemePreference } from '../contexts/ThemeContext'

interface ThemeToggleProps {
  'data-testid'?: string
}

const themeOptions: { value: ThemePreference; icon: React.ElementType; label: string }[] = [
  { value: 'system', icon: Monitor, label: 'System' },
  { value: 'light', icon: Sun, label: 'Light' },
  { value: 'dark', icon: Moon, label: 'Dark' },
  { value: 'cyberpunk', icon: Sparkles, label: 'Cyberpunk' },
  { value: 'synthwave', icon: Waves, label: 'Synthwave' },
]

export default function ThemeToggle({ 'data-testid': testId }: ThemeToggleProps) {
  const { themePreference, setThemePreference } = useTheme()

  const cycleTheme = () => {
    const currentIndex = themeOptions.findIndex(opt => opt.value === themePreference)
    const nextIndex = (currentIndex + 1) % themeOptions.length
    setThemePreference(themeOptions[nextIndex].value)
  }

  const currentOption = themeOptions.find(opt => opt.value === themePreference) || themeOptions[0]
  const CurrentIcon = currentOption.icon

  return (
    <div data-testid={testId} className="relative">
      {/* Simple toggle button for cycling themes */}
      <motion.button
        whileHover={{ scale: 1.05 }}
        whileTap={{ scale: 0.95 }}
        onClick={cycleTheme}
        className="btn btn-ghost btn-circle"
        aria-label={`Current theme: ${currentOption.label}. Click to change theme.`}
        data-testid="theme-toggle-button"
      >
        <CurrentIcon className="w-5 h-5" data-testid="theme-icon" />
      </motion.button>

      {/* Dropdown for direct theme selection */}
      <div className="dropdown dropdown-end">
        <motion.button
          whileHover={{ scale: 1.02 }}
          tabIndex={0}
          className="btn btn-ghost btn-sm gap-2"
          aria-label="Select theme"
          aria-haspopup="listbox"
          data-testid="theme-dropdown-trigger"
        >
          <CurrentIcon className="w-4 h-4" />
          <span className="hidden sm:inline">{currentOption.label}</span>
          <svg className="w-3 h-3 opacity-60" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
          </svg>
        </motion.button>
        <ul
          tabIndex={0}
          className="dropdown-content z-50 menu p-2 shadow-lg bg-base-200 rounded-box w-44"
          role="listbox"
          aria-label="Theme options"
          data-testid="theme-dropdown-menu"
        >
          {themeOptions.map(option => {
            const Icon = option.icon
            const isSelected = themePreference === option.value
            return (
              <li key={option.value}>
                <button
                  onClick={() => setThemePreference(option.value)}
                  className={`flex items-center gap-2 ${isSelected ? 'active' : ''}`}
                  role="option"
                  aria-selected={isSelected}
                  data-testid={`theme-option-${option.value}`}
                >
                  <Icon className="w-4 h-4" />
                  {option.label}
                  {isSelected && (
                    <svg className="w-4 h-4 ml-auto text-primary" fill="currentColor" viewBox="0 0 20 20">
                      <path fillRule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clipRule="evenodd" />
                    </svg>
                  )}
                </button>
              </li>
            )
          })}
        </ul>
      </div>
    </div>
  )
}
