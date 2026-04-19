/**
 * Theme Detection Module
 * Owner: Scenario 10 - Dark Mode Theme Support
 *
 * Detects system color scheme preference and provides utilities
 * for theme-aware functionality. Uses CSS custom properties
 * which automatically update based on prefers-color-scheme.
 *
 * Features:
 * - Detects prefers-color-scheme media query
 * - Listens for system theme changes in real-time
 * - Provides programmatic access to current theme
 */

/**
 * Get the current system theme preference
 * @returns {'dark' | 'light'} The current system color scheme
 */
export function getSystemTheme() {
  if (typeof window === 'undefined' || !window.matchMedia) {
    return 'light'; // Default to light in non-browser environments
  }
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
}

/**
 * Check if the system prefers dark mode
 * @returns {boolean} True if system prefers dark mode
 */
export function prefersDarkMode() {
  return getSystemTheme() === 'dark';
}

/**
 * Register a callback for theme changes
 * @param {function('dark' | 'light'): void} callback - Called when theme changes
 * @returns {function(): void} Cleanup function to remove the listener
 */
export function onThemeChange(callback) {
  if (typeof window === 'undefined' || !window.matchMedia) {
    return () => {}; // No-op cleanup in non-browser environments
  }

  const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');

  const handleChange = (event) => {
    callback(event.matches ? 'dark' : 'light');
  };

  // Modern browsers support addEventListener
  if (mediaQuery.addEventListener) {
    mediaQuery.addEventListener('change', handleChange);
    return () => mediaQuery.removeEventListener('change', handleChange);
  }

  // Fallback for older browsers
  if (mediaQuery.addListener) {
    mediaQuery.addListener(handleChange);
    return () => mediaQuery.removeListener(handleChange);
  }

  return () => {}; // No-op if no support
}

/**
 * Initialize theme detection and logging
 * Sets up listeners for theme changes and logs the current theme.
 * @returns {object} Theme info object with current theme and cleanup function
 */
export function initTheme() {
  const currentTheme = getSystemTheme();

  // Set data attribute on root element for CSS hooks
  if (typeof document !== 'undefined') {
    document.documentElement.dataset.theme = currentTheme;
  }

  // Listen for theme changes and update the data attribute
  const cleanup = onThemeChange((newTheme) => {
    if (typeof document !== 'undefined') {
      document.documentElement.dataset.theme = newTheme;
    }
    console.log(`Theme changed to: ${newTheme}`);
  });

  return {
    theme: currentTheme,
    cleanup
  };
}
