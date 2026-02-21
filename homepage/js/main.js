/**
 * MirDB Homepage JavaScript
 * Owner: Scenario 8 - Theme Support
 *
 * Features:
 * - Theme toggle (dark/light mode)
 * - LocalStorage persistence for theme preference
 * - System preference detection (prefers-color-scheme)
 *
 * Progressive Enhancement:
 * - Page works without JS (uses CSS media queries)
 * - JS enhances UX with toggle and persistence
 */

(function() {
    'use strict';

    // Constants
    const THEME_STORAGE_KEY = 'mirdb-theme';
    const THEME_LIGHT = 'light';
    const THEME_DARK = 'dark';

    /**
     * Get the user's system color scheme preference
     * @returns {string} 'dark' or 'light'
     */
    function getSystemTheme() {
        if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
            return THEME_DARK;
        }
        return THEME_LIGHT;
    }

    /**
     * Get the stored theme preference from localStorage
     * @returns {string|null} The stored theme or null
     */
    function getStoredTheme() {
        try {
            return localStorage.getItem(THEME_STORAGE_KEY);
        } catch (e) {
            // localStorage might not be available (private browsing, etc.)
            return null;
        }
    }

    /**
     * Store the theme preference in localStorage
     * @param {string} theme - The theme to store ('light' or 'dark')
     */
    function storeTheme(theme) {
        try {
            localStorage.setItem(THEME_STORAGE_KEY, theme);
        } catch (e) {
            // Silently fail if localStorage is not available
        }
    }

    /**
     * Get the current effective theme
     * Priority: stored preference > system preference
     * @returns {string} 'dark' or 'light'
     */
    function getCurrentTheme() {
        const stored = getStoredTheme();
        if (stored === THEME_LIGHT || stored === THEME_DARK) {
            return stored;
        }
        return getSystemTheme();
    }

    /**
     * Apply the theme to the document
     * @param {string} theme - The theme to apply ('light' or 'dark')
     */
    function applyTheme(theme) {
        document.documentElement.setAttribute('data-theme', theme);

        // Update the toggle button's aria-label for accessibility
        const toggleButton = document.getElementById('theme-toggle');
        if (toggleButton) {
            const label = theme === THEME_DARK
                ? 'Switch to light mode'
                : 'Switch to dark mode';
            toggleButton.setAttribute('aria-label', label);
        }
    }

    /**
     * Toggle between light and dark themes
     */
    function toggleTheme() {
        const current = document.documentElement.getAttribute('data-theme') || getCurrentTheme();
        const newTheme = current === THEME_DARK ? THEME_LIGHT : THEME_DARK;

        applyTheme(newTheme);
        storeTheme(newTheme);
    }

    /**
     * Initialize the theme toggle functionality
     */
    function initTheme() {
        // Apply the initial theme
        const theme = getCurrentTheme();
        applyTheme(theme);

        // Set up the toggle button
        const toggleButton = document.getElementById('theme-toggle');
        if (toggleButton) {
            toggleButton.addEventListener('click', toggleTheme);
        }

        // Listen for system preference changes
        if (window.matchMedia) {
            const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');

            // Only respond to system changes if user hasn't set a preference
            mediaQuery.addEventListener('change', function(e) {
                const storedTheme = getStoredTheme();
                if (!storedTheme) {
                    applyTheme(e.matches ? THEME_DARK : THEME_LIGHT);
                }
            });
        }
    }

    // Initialize when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initTheme);
    } else {
        initTheme();
    }
})();
