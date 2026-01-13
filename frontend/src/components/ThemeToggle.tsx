import { motion } from 'framer-motion'
import { Sun, Moon } from 'lucide-react'
import { useTheme } from '../contexts/ThemeContext'

interface ThemeToggleProps {
  'data-testid'?: string
}

/**
 * ThemeToggle component for switching between light and dark modes
 * Implements REQ-7: Support theme switching consistent with existing dark/light mode system
 * Implements US-6: Toggle theme with immediate visual feedback and persistence
 */
export function ThemeToggle({ 'data-testid': testId = 'theme-toggle' }: ThemeToggleProps) {
  const { theme, toggleTheme } = useTheme()

  return (
    <motion.button
      data-testid={testId}
      onClick={toggleTheme}
      className="btn btn-ghost btn-circle"
      aria-label={`Switch to ${theme === 'light' ? 'dark' : 'light'} mode`}
      whileHover={{ scale: 1.1 }}
      whileTap={{ scale: 0.9 }}
    >
      <motion.div
        initial={false}
        animate={{ rotate: theme === 'dark' ? 0 : 180 }}
        transition={{ duration: 0.3, ease: 'easeInOut' }}
      >
        {theme === 'dark' ? (
          <Moon className="w-5 h-5" data-testid="moon-icon" />
        ) : (
          <Sun className="w-5 h-5" data-testid="sun-icon" />
        )}
      </motion.div>
    </motion.button>
  )
}

export default ThemeToggle
