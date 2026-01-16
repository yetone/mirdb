import { useTheme, Theme, ThemePreference } from '../contexts/ThemeContext'
import { Sun, Moon, Monitor, Sparkles, Waves } from 'lucide-react'
import { clsx } from 'clsx'
import { motion, AnimatePresence } from 'framer-motion'
import { useState, useRef, useEffect } from 'react'

interface ThemeToggleProps {
  'data-testid'?: string
}

interface ThemeOption {
  value: ThemePreference
  label: string
  icon: React.ReactNode
}

const themeOptions: ThemeOption[] = [
  { value: 'light', label: 'Light', icon: <Sun className="w-4 h-4" /> },
  { value: 'dark', label: 'Dark', icon: <Moon className="w-4 h-4" /> },
  { value: 'system', label: 'System', icon: <Monitor className="w-4 h-4" /> },
  { value: 'cyberpunk', label: 'Cyberpunk', icon: <Sparkles className="w-4 h-4" /> },
  { value: 'synthwave', label: 'Synthwave', icon: <Waves className="w-4 h-4" /> },
]

function getThemeIcon(theme: Theme): React.ReactNode {
  switch (theme) {
    case 'light':
      return <Sun className="w-5 h-5" />
    case 'dark':
      return <Moon className="w-5 h-5" />
    case 'cyberpunk':
      return <Sparkles className="w-5 h-5" />
    case 'synthwave':
      return <Waves className="w-5 h-5" />
    default:
      return <Sun className="w-5 h-5" />
  }
}

export default function ThemeToggle({ 'data-testid': testId }: ThemeToggleProps) {
  const { theme, themePreference, setThemePreference } = useTheme()
  const [isOpen, setIsOpen] = useState(false)
  const dropdownRef = useRef<HTMLDivElement>(null)

  // Close dropdown when clicking outside
  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setIsOpen(false)
      }
    }

    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [])

  // Close dropdown on escape key
  useEffect(() => {
    function handleEscape(event: KeyboardEvent) {
      if (event.key === 'Escape') {
        setIsOpen(false)
      }
    }

    document.addEventListener('keydown', handleEscape)
    return () => document.removeEventListener('keydown', handleEscape)
  }, [])

  const handleThemeSelect = (newTheme: ThemePreference) => {
    setThemePreference(newTheme)
    setIsOpen(false)
  }

  return (
    <div
      ref={dropdownRef}
      className="relative"
      data-testid={testId || 'theme-toggle'}
    >
      <motion.button
        whileHover={{ scale: 1.05 }}
        whileTap={{ scale: 0.95 }}
        onClick={() => setIsOpen(!isOpen)}
        className={clsx(
          'flex items-center gap-2 px-3 py-2 rounded-lg',
          'bg-base-200 hover:bg-base-300 transition-colors',
          'text-base-content border border-base-300',
          'focus:outline-none focus:ring-2 focus:ring-primary focus:ring-offset-2',
          'min-w-[44px] min-h-[44px]'
        )}
        aria-label="Toggle theme"
        aria-expanded={isOpen}
        aria-haspopup="listbox"
        data-testid="theme-toggle-button"
      >
        <span data-testid="theme-toggle-icon">
          {getThemeIcon(theme)}
        </span>
        <span className="hidden sm:inline text-sm font-medium" data-testid="theme-toggle-label">
          {themeOptions.find(opt => opt.value === themePreference)?.label || 'Theme'}
        </span>
        <motion.svg
          animate={{ rotate: isOpen ? 180 : 0 }}
          className="w-4 h-4 hidden sm:block"
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
        >
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
        </motion.svg>
      </motion.button>

      <AnimatePresence>
        {isOpen && (
          <motion.div
            initial={{ opacity: 0, y: -10 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -10 }}
            transition={{ duration: 0.15 }}
            className={clsx(
              'absolute right-0 mt-2 w-48 rounded-lg shadow-lg',
              'bg-base-100 border border-base-300',
              'z-50 overflow-hidden'
            )}
            role="listbox"
            aria-label="Theme options"
            data-testid="theme-dropdown"
          >
            {themeOptions.map((option) => (
              <button
                key={option.value}
                onClick={() => handleThemeSelect(option.value)}
                className={clsx(
                  'w-full flex items-center gap-3 px-4 py-3 text-left',
                  'hover:bg-base-200 transition-colors',
                  'text-base-content',
                  themePreference === option.value && 'bg-primary/10 text-primary'
                )}
                role="option"
                aria-selected={themePreference === option.value}
                data-testid={`theme-option-${option.value}`}
              >
                {option.icon}
                <span className="font-medium">{option.label}</span>
                {themePreference === option.value && (
                  <motion.svg
                    initial={{ scale: 0 }}
                    animate={{ scale: 1 }}
                    className="w-4 h-4 ml-auto text-primary"
                    fill="none"
                    stroke="currentColor"
                    viewBox="0 0 24 24"
                  >
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                  </motion.svg>
                )}
              </button>
            ))}
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  )
}
