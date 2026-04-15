/**
 * Theme Management
 * Owner: Scenario 8 - Theme Switching
 *
 * Core theme management functionality including:
 * - LocalStorage persistence
 * - System preference detection
 * - Theme application
 */

/** LocalStorage key for theme preference */
export const STORAGE_KEY = 'mirdb-theme';

/** Valid theme values */
export const THEMES = ['light', 'dark'];

/**
 * Get the current theme from the document
 * @returns {'light' | 'dark'} Current theme
 */
export function getTheme() {
  if (typeof document !== 'undefined') {
    return document.documentElement.getAttribute('data-theme') || 'light';
  }
  return 'light';
}

/**
 * Set the theme and persist to localStorage
 * @param {'light' | 'dark'} theme - Theme to set
 */
export function setTheme(theme) {
  if (!THEMES.includes(theme)) {
    console.warn(`Invalid theme: ${theme}. Using 'light' instead.`);
    theme = 'light';
  }

  // Apply theme to document
  applyTheme(theme);

  // Persist to localStorage
  if (typeof localStorage !== 'undefined') {
    localStorage.setItem(STORAGE_KEY, theme);
  }
}

/**
 * Toggle between light and dark themes
 * @returns {'light' | 'dark'} The new theme
 */
export function toggleTheme() {
  const currentTheme = getTheme();
  const newTheme = currentTheme === 'light' ? 'dark' : 'light';
  setTheme(newTheme);
  return newTheme;
}

/**
 * Apply theme to the document without persisting
 * @param {'light' | 'dark'} theme - Theme to apply
 */
export function applyTheme(theme) {
  if (typeof document !== 'undefined') {
    document.documentElement.setAttribute('data-theme', theme);
  }
}

/**
 * Load stored theme preference from localStorage
 * @returns {'light' | 'dark' | null} Stored theme or null if not set
 */
export function loadStoredTheme() {
  if (typeof localStorage !== 'undefined') {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (stored && THEMES.includes(stored)) {
      return stored;
    }
  }
  return null;
}

/**
 * Detect system color scheme preference
 * @returns {'light' | 'dark'} System preference
 */
export function detectSystemPreference() {
  if (typeof window !== 'undefined' && window.matchMedia) {
    if (window.matchMedia('(prefers-color-scheme: dark)').matches) {
      return 'dark';
    }
  }
  return 'light';
}

/**
 * Initialize theme on page load
 * Priority: localStorage > system preference > default (light)
 */
export function initTheme() {
  const storedTheme = loadStoredTheme();

  if (storedTheme) {
    applyTheme(storedTheme);
  } else {
    const systemTheme = detectSystemPreference();
    applyTheme(systemTheme);
  }
}

// Auto-initialize on script load (before DOM ready to prevent flash)
initTheme();
