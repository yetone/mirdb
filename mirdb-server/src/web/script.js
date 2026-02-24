/*
  MirDB Homepage JavaScript

  Owner: Scenario 4 - Theme Toggle & Interactions

  Functionality:
  - Theme toggle (light/dark) with localStorage persistence
  - Copy-to-clipboard for code blocks with "Copied!" feedback
  - Mobile hamburger menu toggle
  - Smooth scroll for navigation links
  - Keyboard accessibility for all interactive elements

  Functions:
  - toggleTheme(): Switch between light/dark modes
  - copyToClipboard(text): Copy text and show feedback
  - initTheme(): Load theme preference from localStorage
*/

(function() {
    'use strict';

    // Constants
    var THEME_KEY = 'theme';
    var DARK_THEME_CLASS = 'dark-theme';
    var MOBILE_OPEN_CLASS = 'mobile-open';
    var COPIED_CLASS = 'copied';
    var COPY_FEEDBACK_DURATION = 2000;

    // DOM Elements
    var themeToggle = document.getElementById('theme-toggle');
    var mobileMenuToggle = document.getElementById('mobile-menu-toggle');
    var mainNav = document.querySelector('.main-nav');

    /**
     * Get the current theme from localStorage or system preference
     * @returns {string} 'dark' or 'light'
     */
    function getStoredTheme() {
        return localStorage.getItem(THEME_KEY);
    }

    /**
     * Get system color scheme preference
     * @returns {string} 'dark' or 'light'
     */
    function getSystemTheme() {
        if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
            return 'dark';
        }
        return 'light';
    }

    /**
     * Save theme to localStorage
     * @param {string} theme - 'dark' or 'light'
     */
    function saveTheme(theme) {
        localStorage.setItem(THEME_KEY, theme);
    }

    /**
     * Apply theme to the page
     * @param {string} theme - 'dark' or 'light'
     */
    function applyTheme(theme) {
        if (theme === 'dark') {
            document.body.classList.add(DARK_THEME_CLASS);
        } else {
            document.body.classList.remove(DARK_THEME_CLASS);
        }
    }

    /**
     * Initialize theme based on localStorage or system preference
     */
    function initTheme() {
        var storedTheme = getStoredTheme();
        var theme;

        if (storedTheme) {
            theme = storedTheme;
        } else {
            theme = getSystemTheme();
        }

        applyTheme(theme);
    }

    /**
     * Toggle between light and dark themes
     */
    function toggleTheme() {
        var isDark = document.body.classList.contains(DARK_THEME_CLASS);
        var newTheme = isDark ? 'light' : 'dark';

        applyTheme(newTheme);
        saveTheme(newTheme);
    }

    /**
     * Copy text to clipboard and show visual feedback
     * @param {string} text - Text to copy
     * @param {HTMLElement} button - The button element to show feedback on
     */
    function copyToClipboard(text, button) {
        if (!navigator.clipboard) {
            // Fallback for older browsers
            fallbackCopyToClipboard(text, button);
            return;
        }

        navigator.clipboard.writeText(text).then(function() {
            showCopyFeedback(button);
        }).catch(function() {
            fallbackCopyToClipboard(text, button);
        });
    }

    /**
     * Fallback copy method for browsers without clipboard API
     * @param {string} text - Text to copy
     * @param {HTMLElement} button - The button element to show feedback on
     */
    function fallbackCopyToClipboard(text, button) {
        var textArea = document.createElement('textarea');
        textArea.value = text;
        textArea.style.position = 'fixed';
        textArea.style.left = '-9999px';
        textArea.style.top = '-9999px';
        document.body.appendChild(textArea);
        textArea.focus();
        textArea.select();

        try {
            document.execCommand('copy');
            showCopyFeedback(button);
        } catch (err) {
            console.error('Failed to copy text:', err);
        }

        document.body.removeChild(textArea);
    }

    /**
     * Show "Copied!" feedback on button
     * @param {HTMLElement} button - The button to show feedback on
     */
    function showCopyFeedback(button) {
        button.classList.add(COPIED_CLASS);

        setTimeout(function() {
            button.classList.remove(COPIED_CLASS);
        }, COPY_FEEDBACK_DURATION);
    }

    /**
     * Toggle mobile navigation menu
     */
    function toggleMobileMenu() {
        var isExpanded = mobileMenuToggle.getAttribute('aria-expanded') === 'true';

        mobileMenuToggle.setAttribute('aria-expanded', !isExpanded);

        if (isExpanded) {
            mainNav.classList.remove(MOBILE_OPEN_CLASS);
        } else {
            mainNav.classList.add(MOBILE_OPEN_CLASS);
        }
    }

    /**
     * Close mobile menu when clicking a navigation link
     */
    function closeMobileMenu() {
        mobileMenuToggle.setAttribute('aria-expanded', 'false');
        mainNav.classList.remove(MOBILE_OPEN_CLASS);
    }

    /**
     * Handle keyboard events for interactive elements
     * @param {KeyboardEvent} event
     */
    function handleKeyboardActivation(event) {
        if (event.key === 'Enter' || event.key === ' ') {
            event.preventDefault();
            event.target.click();
        }
    }

    /**
     * Initialize copy button event listeners
     */
    function initCopyButtons() {
        var copyButtons = document.querySelectorAll('.copy-btn');

        copyButtons.forEach(function(button) {
            // Click handler
            button.addEventListener('click', function(event) {
                var textToCopy = button.getAttribute('data-copy');
                if (textToCopy) {
                    copyToClipboard(textToCopy, button);
                }
            });

            // Keyboard handler - ensure buttons are keyboard accessible
            button.addEventListener('keydown', handleKeyboardActivation);
        });
    }

    /**
     * Initialize theme toggle button
     */
    function initThemeToggle() {
        if (themeToggle) {
            themeToggle.addEventListener('click', toggleTheme);
            themeToggle.addEventListener('keydown', handleKeyboardActivation);
        }
    }

    /**
     * Initialize mobile menu toggle
     */
    function initMobileMenu() {
        if (mobileMenuToggle && mainNav) {
            mobileMenuToggle.addEventListener('click', toggleMobileMenu);
            mobileMenuToggle.addEventListener('keydown', handleKeyboardActivation);

            // Close menu when clicking navigation links
            var navLinks = mainNav.querySelectorAll('a');
            navLinks.forEach(function(link) {
                link.addEventListener('click', closeMobileMenu);
            });
        }
    }

    /**
     * Listen for system theme changes
     */
    function initSystemThemeListener() {
        if (window.matchMedia) {
            var mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');

            mediaQuery.addEventListener('change', function(e) {
                // Only update if user hasn't set a preference
                if (!getStoredTheme()) {
                    applyTheme(e.matches ? 'dark' : 'light');
                }
            });
        }
    }

    /**
     * Initialize all JavaScript functionality
     */
    function init() {
        initTheme();
        initThemeToggle();
        initCopyButtons();
        initMobileMenu();
        initSystemThemeListener();
    }

    // Initialize when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

    // Expose functions for testing purposes
    window.MirDB = {
        toggleTheme: toggleTheme,
        copyToClipboard: copyToClipboard,
        initTheme: initTheme,
        getStoredTheme: getStoredTheme,
        getSystemTheme: getSystemTheme,
        toggleMobileMenu: toggleMobileMenu
    };
})();
