/**
 * Theme Utility
 * Owner: Scenario 13 - Dark Mode Support
 *
 * Expected exports:
 * - getPreferredColorScheme(): 'light' | 'dark' - Detect system preference
 * - applyTheme(theme: 'light' | 'dark'): void - Apply theme to document
 * - watchColorSchemeChange(callback: Function): void - Listen for preference changes
 * - initTheme(): void - Initialize theme detection and watching
 */

/**
 * Detect the user's preferred color scheme from system settings
 * @returns {'light' | 'dark'} The preferred color scheme
 */
export function getPreferredColorScheme() {
  // Check if matchMedia is supported
  if (typeof window !== 'undefined' && window.matchMedia) {
    // Check for dark mode preference
    if (window.matchMedia('(prefers-color-scheme: dark)').matches) {
      return 'dark';
    }
  }
  // Default to light mode
  return 'light';
}

/**
 * Apply a specific theme to the document
 * @param {'light' | 'dark'} theme - The theme to apply
 */
export function applyTheme(theme) {
  if (typeof document !== 'undefined') {
    // Set data-theme attribute on the document element
    document.documentElement.setAttribute('data-theme', theme);

    // Update meta theme-color for mobile browsers
    const metaThemeColor = document.querySelector('meta[name="theme-color"]');
    if (metaThemeColor) {
      metaThemeColor.setAttribute('content', theme === 'dark' ? '#0d1117' : '#ffffff');
    }
  }
}

/**
 * Watch for color scheme preference changes
 * @param {Function} callback - Function to call when preference changes
 * @returns {Function} Cleanup function to stop watching
 */
export function watchColorSchemeChange(callback) {
  if (typeof window !== 'undefined' && window.matchMedia) {
    const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');

    // Handler for change events
    const handleChange = (event) => {
      const newTheme = event.matches ? 'dark' : 'light';
      callback(newTheme);
    };

    // Add listener - use modern addEventListener if available
    if (mediaQuery.addEventListener) {
      mediaQuery.addEventListener('change', handleChange);
    } else if (mediaQuery.addListener) {
      // Fallback for older browsers
      mediaQuery.addListener(handleChange);
    }

    // Return cleanup function
    return () => {
      if (mediaQuery.removeEventListener) {
        mediaQuery.removeEventListener('change', handleChange);
      } else if (mediaQuery.removeListener) {
        mediaQuery.removeListener(handleChange);
      }
    };
  }

  // Return no-op cleanup if matchMedia not supported
  return () => {};
}

/**
 * Initialize theme detection and set up change watching
 * Applies the current preferred theme and watches for changes
 */
export function initTheme() {
  // Get and apply the initial preferred theme
  const preferredTheme = getPreferredColorScheme();
  applyTheme(preferredTheme);

  // Watch for system preference changes and apply them
  watchColorSchemeChange((newTheme) => {
    applyTheme(newTheme);
  });
}

/**
 * Check if the current color scheme is dark
 * @returns {boolean} True if dark mode is active
 */
export function isDarkMode() {
  if (typeof document !== 'undefined') {
    // First check data-theme attribute
    const dataTheme = document.documentElement.getAttribute('data-theme');
    if (dataTheme) {
      return dataTheme === 'dark';
    }
  }
  // Fall back to system preference
  return getPreferredColorScheme() === 'dark';
}

/**
 * Toggle between light and dark themes
 * @returns {'light' | 'dark'} The new theme after toggling
 */
export function toggleTheme() {
  const currentTheme = isDarkMode() ? 'dark' : 'light';
  const newTheme = currentTheme === 'dark' ? 'light' : 'dark';
  applyTheme(newTheme);
  return newTheme;
}
