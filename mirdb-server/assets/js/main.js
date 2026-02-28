/**
 * MirDB Homepage JavaScript
 *
 * Owner: Scenario 6 - Theme Toggle Functionality
 *
 * Expected functions:
 * - initTheme() - Initialize theme from localStorage or system preference
 * - toggleTheme() - Switch between dark and light mode
 * - saveThemePreference(theme) - Persist to localStorage
 * - refreshMetrics() - Fetch and update metrics display (Scenario 5 may add)
 */

(function() {
    'use strict';

    // Theme constants
    var THEME_KEY = 'mirdb-theme';
    var THEME_DARK = 'dark';
    var THEME_LIGHT = 'light';

    // Icon constants (moon for dark mode toggle, sun for light mode toggle)
    var ICON_MOON = '\u263E';  // ☾
    var ICON_SUN = '\u2600';   // ☀

    /**
     * Get the current theme from localStorage or system preference
     * @returns {string} 'dark' or 'light'
     */
    function getStoredTheme() {
        try {
            return localStorage.getItem(THEME_KEY);
        } catch (e) {
            // localStorage not available
            return null;
        }
    }

    /**
     * Get system preference for color scheme
     * @returns {string} 'dark' or 'light'
     */
    function getSystemTheme() {
        if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
            return THEME_DARK;
        }
        return THEME_LIGHT;
    }

    /**
     * Save theme preference to localStorage
     * @param {string} theme - 'dark' or 'light'
     */
    function saveThemePreference(theme) {
        try {
            localStorage.setItem(THEME_KEY, theme);
        } catch (e) {
            // localStorage not available, fail silently
        }
    }

    /**
     * Apply theme to the document
     * @param {string} theme - 'dark' or 'light'
     */
    function applyTheme(theme) {
        var root = document.documentElement;

        if (theme === THEME_DARK) {
            root.setAttribute('data-theme', THEME_DARK);
        } else {
            root.removeAttribute('data-theme');
        }

        // Update the toggle button icon
        updateToggleIcon(theme);
    }

    /**
     * Update the theme toggle button icon
     * @param {string} currentTheme - current theme
     */
    function updateToggleIcon(currentTheme) {
        var toggleBtn = document.getElementById('theme-toggle');
        if (toggleBtn) {
            var iconEl = toggleBtn.querySelector('.theme-icon');
            if (iconEl) {
                // Show sun icon in dark mode (to switch to light)
                // Show moon icon in light mode (to switch to dark)
                iconEl.textContent = currentTheme === THEME_DARK ? ICON_SUN : ICON_MOON;
            }
            toggleBtn.setAttribute('aria-label',
                currentTheme === THEME_DARK ? 'Switch to light mode' : 'Switch to dark mode'
            );
        }
    }

    /**
     * Get the current active theme
     * @returns {string} 'dark' or 'light'
     */
    function getCurrentTheme() {
        var root = document.documentElement;
        return root.getAttribute('data-theme') === THEME_DARK ? THEME_DARK : THEME_LIGHT;
    }

    /**
     * Toggle between dark and light theme
     */
    function toggleTheme() {
        var currentTheme = getCurrentTheme();
        var newTheme = currentTheme === THEME_DARK ? THEME_LIGHT : THEME_DARK;

        applyTheme(newTheme);
        saveThemePreference(newTheme);
    }

    /**
     * Initialize theme based on stored preference or system preference
     */
    function initTheme() {
        // First check localStorage, then system preference, default to light
        var storedTheme = getStoredTheme();
        var theme;

        if (storedTheme === THEME_DARK || storedTheme === THEME_LIGHT) {
            theme = storedTheme;
        } else {
            theme = getSystemTheme();
        }

        applyTheme(theme);

        // Set up toggle button click handler
        var toggleBtn = document.getElementById('theme-toggle');
        if (toggleBtn) {
            toggleBtn.addEventListener('click', toggleTheme);
        }

        // Listen for system theme changes
        if (window.matchMedia) {
            var darkModeMediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
            // Only update if user hasn't set a preference
            darkModeMediaQuery.addEventListener('change', function(e) {
                if (!getStoredTheme()) {
                    applyTheme(e.matches ? THEME_DARK : THEME_LIGHT);
                }
            });
        }
    }

    // Initialize on DOM ready
    document.addEventListener('DOMContentLoaded', initTheme);

    // Expose functions globally for testing
    window.MirDBTheme = {
        initTheme: initTheme,
        toggleTheme: toggleTheme,
        getCurrentTheme: getCurrentTheme,
        saveThemePreference: saveThemePreference,
        THEME_KEY: THEME_KEY,
        THEME_DARK: THEME_DARK,
        THEME_LIGHT: THEME_LIGHT
    };
})();
