/**
 * Theme Utilities
 * Owner: Scenario 15 - Dark Mode Support
 *
 * Provides utilities for detecting, applying, and managing dark/light themes.
 * Supports system preference detection, manual theme switching, and theme change listeners.
 */

/**
 * Gets the user's preferred theme based on system settings
 * Uses the prefers-color-scheme media query to detect system preference
 * @returns {'light' | 'dark'} The preferred theme
 */
export function getPreferredTheme() {
  // Check if we're in a browser environment
  if (typeof window === 'undefined' || typeof window.matchMedia === 'undefined') {
    return 'light'
  }

  // Use matchMedia to check system preference
  const darkModeQuery = window.matchMedia('(prefers-color-scheme: dark)')
  return darkModeQuery.matches ? 'dark' : 'light'
}

/**
 * Applies the specified theme to the document
 * Adds or removes the 'dark' class on the html element
 * @param {'light' | 'dark'} theme - The theme to apply
 * @param {HTMLElement} [element] - Optional element to apply theme to (defaults to document.documentElement)
 */
export function setTheme(theme, element) {
  const targetElement = element || (typeof document !== 'undefined' ? document.documentElement : null)

  if (!targetElement) {
    return
  }

  if (theme === 'dark') {
    targetElement.classList.add('dark')
  } else {
    targetElement.classList.remove('dark')
  }

  // Optionally persist to localStorage for future visits
  if (typeof localStorage !== 'undefined') {
    try {
      localStorage.setItem('theme', theme)
    } catch {
      // Ignore storage errors (e.g., in private browsing)
    }
  }
}

/**
 * Registers a callback to be called when the system theme preference changes
 * @param {(theme: 'light' | 'dark') => void} callback - Function to call when theme changes
 * @returns {() => void} Cleanup function to remove the listener
 */
export function onThemeChange(callback) {
  // Check if we're in a browser environment
  if (typeof window === 'undefined' || typeof window.matchMedia === 'undefined') {
    return () => {}
  }

  const darkModeQuery = window.matchMedia('(prefers-color-scheme: dark)')

  const handleChange = (event) => {
    callback(event.matches ? 'dark' : 'light')
  }

  // Use addEventListener for modern browsers
  if (darkModeQuery.addEventListener) {
    darkModeQuery.addEventListener('change', handleChange)
    return () => darkModeQuery.removeEventListener('change', handleChange)
  }

  // Fallback for older browsers
  if (darkModeQuery.addListener) {
    darkModeQuery.addListener(handleChange)
    return () => darkModeQuery.removeListener(handleChange)
  }

  return () => {}
}

/**
 * Gets the stored theme preference from localStorage
 * Falls back to system preference if no stored value exists
 * @returns {'light' | 'dark'} The theme preference
 */
export function getStoredTheme() {
  if (typeof localStorage !== 'undefined') {
    try {
      const storedTheme = localStorage.getItem('theme')
      if (storedTheme === 'light' || storedTheme === 'dark') {
        return storedTheme
      }
    } catch {
      // Ignore storage errors
    }
  }

  return getPreferredTheme()
}

/**
 * Initializes the theme system
 * - Applies stored or system preference theme on load
 * - Sets up listener for system theme changes
 * @returns {() => void} Cleanup function to remove listeners
 */
export function initTheme() {
  const initialTheme = getStoredTheme()
  setTheme(initialTheme)

  // Listen for system theme changes and apply them if no manual override
  return onThemeChange((newTheme) => {
    // Only apply system changes if user hasn't manually set a preference
    if (typeof localStorage !== 'undefined') {
      try {
        const storedTheme = localStorage.getItem('theme')
        if (!storedTheme) {
          setTheme(newTheme)
        }
      } catch {
        setTheme(newTheme)
      }
    } else {
      setTheme(newTheme)
    }
  })
}

/**
 * Toggles between light and dark themes
 * @param {HTMLElement} [element] - Optional element to apply theme to
 * @returns {'light' | 'dark'} The new theme after toggle
 */
export function toggleTheme(element) {
  const targetElement = element || (typeof document !== 'undefined' ? document.documentElement : null)

  if (!targetElement) {
    return 'light'
  }

  const isDark = targetElement.classList.contains('dark')
  const newTheme = isDark ? 'light' : 'dark'
  setTheme(newTheme, targetElement)

  return newTheme
}

export default {
  getPreferredTheme,
  setTheme,
  onThemeChange,
  getStoredTheme,
  initTheme,
  toggleTheme
}
