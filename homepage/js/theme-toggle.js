/**
 * MirDB Homepage - Theme Toggle
 * Owner: Scenario 18 - Theme Toggle
 *
 * Functionality:
 * - Detect system preference via prefers-color-scheme
 * - Load saved preference from localStorage
 * - Toggle between light/dark themes on button click
 * - Save preference to localStorage
 * - Apply theme class to document root
 *
 * Expected exports: None (IIFE or module)
 */

(function() {
    'use strict';

    const STORAGE_KEY = 'mirdb-theme';
    const DARK_CLASS = 'dark-theme';
    const LIGHT_CLASS = 'light-theme';

    /**
     * Get the system's preferred color scheme
     * @returns {'dark' | 'light'} The system preference
     */
    function getSystemPreference() {
        if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
            return 'dark';
        }
        return 'light';
    }

    /**
     * Get the saved theme preference from localStorage
     * @returns {string | null} The saved theme or null if not set
     */
    function getSavedTheme() {
        try {
            return localStorage.getItem(STORAGE_KEY);
        } catch (e) {
            // localStorage might be blocked
            return null;
        }
    }

    /**
     * Save the theme preference to localStorage
     * @param {string} theme - The theme to save ('dark' or 'light')
     */
    function saveTheme(theme) {
        try {
            localStorage.setItem(STORAGE_KEY, theme);
        } catch (e) {
            // localStorage might be blocked
            console.warn('Unable to save theme preference to localStorage');
        }
    }

    /**
     * Apply the specified theme to the document
     * @param {string} theme - The theme to apply ('dark' or 'light')
     */
    function applyTheme(theme) {
        const root = document.documentElement;

        if (theme === 'dark') {
            root.classList.add(DARK_CLASS);
            root.classList.remove(LIGHT_CLASS);
        } else {
            root.classList.add(LIGHT_CLASS);
            root.classList.remove(DARK_CLASS);
        }

        // Update the toggle button's aria-label and icon
        updateToggleButton(theme);
    }

    /**
     * Get the current applied theme
     * @returns {'dark' | 'light'} The current theme
     */
    function getCurrentTheme() {
        if (document.documentElement.classList.contains(DARK_CLASS)) {
            return 'dark';
        }
        if (document.documentElement.classList.contains(LIGHT_CLASS)) {
            return 'light';
        }
        // No explicit class set, return based on system preference
        return getSystemPreference();
    }

    /**
     * Toggle between dark and light themes
     */
    function toggleTheme() {
        const currentTheme = getCurrentTheme();
        const newTheme = currentTheme === 'dark' ? 'light' : 'dark';

        applyTheme(newTheme);
        saveTheme(newTheme);
    }

    /**
     * Update the toggle button's appearance and accessibility attributes
     * @param {string} theme - The current theme
     */
    function updateToggleButton(theme) {
        const toggleButton = document.getElementById('theme-toggle');
        if (!toggleButton) return;

        const sunIcon = toggleButton.querySelector('.theme-icon-sun');
        const moonIcon = toggleButton.querySelector('.theme-icon-moon');

        if (theme === 'dark') {
            toggleButton.setAttribute('aria-label', 'Switch to light mode');
            if (sunIcon) sunIcon.style.display = 'block';
            if (moonIcon) moonIcon.style.display = 'none';
        } else {
            toggleButton.setAttribute('aria-label', 'Switch to dark mode');
            if (sunIcon) sunIcon.style.display = 'none';
            if (moonIcon) moonIcon.style.display = 'block';
        }
    }

    /**
     * Initialize the theme based on saved preference or system preference
     */
    function initTheme() {
        const savedTheme = getSavedTheme();

        if (savedTheme) {
            // Use saved preference
            applyTheme(savedTheme);
        } else {
            // Use system preference
            const systemTheme = getSystemPreference();
            applyTheme(systemTheme);
        }
    }

    /**
     * Set up the theme toggle button click handler
     */
    function setupToggleButton() {
        const toggleButton = document.getElementById('theme-toggle');
        if (!toggleButton) {
            console.warn('Theme toggle button not found');
            return;
        }

        toggleButton.addEventListener('click', toggleTheme);

        // Also support keyboard activation
        toggleButton.addEventListener('keydown', function(event) {
            if (event.key === 'Enter' || event.key === ' ') {
                event.preventDefault();
                toggleTheme();
            }
        });
    }

    /**
     * Listen for system preference changes
     */
    function setupSystemPreferenceListener() {
        if (!window.matchMedia) return;

        const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');

        mediaQuery.addEventListener('change', function(event) {
            // Only update if user hasn't set a manual preference
            const savedTheme = getSavedTheme();
            if (!savedTheme) {
                applyTheme(event.matches ? 'dark' : 'light');
            }
        });
    }

    // Initialize when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', function() {
            initTheme();
            setupToggleButton();
            setupSystemPreferenceListener();
        });
    } else {
        // DOM is already ready
        initTheme();
        setupToggleButton();
        setupSystemPreferenceListener();
    }

    // Also apply theme immediately to prevent flash
    // This runs before DOMContentLoaded for faster application
    (function() {
        const savedTheme = getSavedTheme();
        if (savedTheme) {
            document.documentElement.classList.toggle(DARK_CLASS, savedTheme === 'dark');
            document.documentElement.classList.toggle(LIGHT_CLASS, savedTheme === 'light');
        }
    })();
})();
