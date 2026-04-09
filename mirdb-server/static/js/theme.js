/**
 * MirDB Homepage - Theme Toggle Logic
 *
 * Owner: Scenario 7 - Dark/Light Mode Toggle
 *
 * Responsibilities:
 * - Initialize theme from localStorage or system preference
 * - Toggle between light/dark themes
 * - Persist preference to localStorage
 * - Update UI toggle button state
 */

(function() {
    'use strict';

    // Theme constants
    const THEME_KEY = 'mirdb-theme';
    const THEMES = {
        LIGHT: 'light',
        DARK: 'dark'
    };

    // Initialize MirDB namespace
    window.MirDB = window.MirDB || {};

    /**
     * Get the system's preferred color scheme
     * @returns {string} 'dark' or 'light'
     */
    function getSystemPreference() {
        if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
            return THEMES.DARK;
        }
        return THEMES.LIGHT;
    }

    /**
     * Get the stored theme preference from localStorage
     * @returns {string|null} Stored theme or null if not set
     */
    function getStoredTheme() {
        try {
            return localStorage.getItem(THEME_KEY);
        } catch (e) {
            // localStorage may be unavailable (private mode, etc.)
            console.warn('Could not access localStorage:', e);
            return null;
        }
    }

    /**
     * Save theme preference to localStorage
     * @param {string} theme - The theme to save ('light' or 'dark')
     */
    function setStoredTheme(theme) {
        try {
            localStorage.setItem(THEME_KEY, theme);
        } catch (e) {
            console.warn('Could not save to localStorage:', e);
        }
    }

    /**
     * Get the current theme
     * @returns {string} Current theme ('light' or 'dark')
     */
    function getCurrentTheme() {
        return document.documentElement.getAttribute('data-theme') || THEMES.LIGHT;
    }

    /**
     * Apply a theme to the document
     * @param {string} theme - The theme to apply ('light' or 'dark')
     * @param {boolean} animate - Whether to animate the transition
     */
    function applyTheme(theme, animate = false) {
        const html = document.documentElement;

        // Add transition class for smooth animation
        if (animate) {
            html.classList.add('theme-transition');
            setTimeout(() => {
                html.classList.remove('theme-transition');
            }, 300);
        }

        // Apply the theme
        html.setAttribute('data-theme', theme);

        // Update toggle button ARIA label
        const toggleButton = document.getElementById('theme-toggle');
        if (toggleButton) {
            const nextTheme = theme === THEMES.DARK ? THEMES.LIGHT : THEMES.DARK;
            toggleButton.setAttribute('aria-label', `Switch to ${nextTheme} mode`);
        }

        // Dispatch custom event for other scripts to listen to
        document.dispatchEvent(new CustomEvent('mirdb:theme-changed', {
            detail: { theme: theme }
        }));
    }

    /**
     * Toggle between light and dark themes
     */
    function toggleTheme() {
        const currentTheme = getCurrentTheme();
        const newTheme = currentTheme === THEMES.DARK ? THEMES.LIGHT : THEMES.DARK;

        applyTheme(newTheme, true);
        setStoredTheme(newTheme);

        return newTheme;
    }

    /**
     * Initialize the theme based on stored preference or system preference
     */
    function initializeTheme() {
        // Priority: 1) Stored preference, 2) System preference, 3) Light (default)
        const storedTheme = getStoredTheme();
        const theme = storedTheme || getSystemPreference();

        applyTheme(theme, false);
    }

    /**
     * Set up theme toggle button click handler
     */
    function setupToggleButton() {
        const toggleButton = document.getElementById('theme-toggle');
        if (toggleButton) {
            toggleButton.addEventListener('click', function(e) {
                e.preventDefault();
                toggleTheme();
            });

            // Set initial ARIA label
            const currentTheme = getCurrentTheme();
            const nextTheme = currentTheme === THEMES.DARK ? THEMES.LIGHT : THEMES.DARK;
            toggleButton.setAttribute('aria-label', `Switch to ${nextTheme} mode`);
        }
    }

    /**
     * Set up listener for system preference changes
     */
    function setupSystemPreferenceListener() {
        if (window.matchMedia) {
            const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');

            // Only respond to system changes if user hasn't set a preference
            mediaQuery.addEventListener('change', function(e) {
                const storedTheme = getStoredTheme();
                if (!storedTheme) {
                    const newTheme = e.matches ? THEMES.DARK : THEMES.LIGHT;
                    applyTheme(newTheme, true);
                }
            });
        }
    }

    /**
     * Initialize theme module when DOM is ready
     */
    function init() {
        initializeTheme();
        setupToggleButton();
        setupSystemPreferenceListener();
    }

    // Export theme module to MirDB namespace
    window.MirDB.theme = {
        toggle: toggleTheme,
        get: getCurrentTheme,
        set: function(theme) {
            if (theme === THEMES.LIGHT || theme === THEMES.DARK) {
                applyTheme(theme, true);
                setStoredTheme(theme);
                return theme;
            }
            throw new Error('Invalid theme. Use "light" or "dark".');
        },
        getSystemPreference: getSystemPreference,
        getStoredTheme: getStoredTheme,
        THEMES: THEMES,
        STORAGE_KEY: THEME_KEY
    };

    // Initialize when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
