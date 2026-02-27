/**
 * Theme Toggle Module
 * Owner: Scenario 6 - Theme Toggle & Accessibility
 *
 * Expected exports:
 * - initThemeToggle(): void - Initialize theme toggle button
 * - getStoredTheme(): 'light' | 'dark' | null
 * - setTheme(theme: 'light' | 'dark'): void
 * - getSystemPreference(): 'light' | 'dark'
 *
 * Uses localStorage for persistence, falls back to system preference.
 */

// Storage key for theme preference
const THEME_STORAGE_KEY = 'mirdb-theme';

/**
 * Get the stored theme from localStorage
 * @returns {'light' | 'dark' | null} The stored theme or null if not set
 */
function getStoredTheme() {
    try {
        const storedTheme = localStorage.getItem(THEME_STORAGE_KEY);
        if (storedTheme === 'light' || storedTheme === 'dark') {
            return storedTheme;
        }
        return null;
    } catch (e) {
        // localStorage may not be available (e.g., private browsing)
        return null;
    }
}

/**
 * Get the user's system color scheme preference
 * @returns {'light' | 'dark'} The system preference
 */
function getSystemPreference() {
    if (typeof window !== 'undefined' && window.matchMedia) {
        return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
    }
    return 'light';
}

/**
 * Get the effective theme (stored preference or system preference)
 * @returns {'light' | 'dark'} The theme to use
 */
function getEffectiveTheme() {
    const storedTheme = getStoredTheme();
    if (storedTheme) {
        return storedTheme;
    }
    return getSystemPreference();
}

/**
 * Apply a theme to the document
 * @param {'light' | 'dark'} theme - The theme to apply
 * @param {boolean} [withTransition=true] - Whether to apply transition animation
 */
function applyTheme(theme, withTransition) {
    if (withTransition === undefined) {
        withTransition = true;
    }

    if (withTransition && typeof document !== 'undefined') {
        document.documentElement.classList.add('theme-transition');
        setTimeout(function() {
            document.documentElement.classList.remove('theme-transition');
        }, 300);
    }

    if (typeof document !== 'undefined') {
        document.documentElement.setAttribute('data-theme', theme);
    }
}

/**
 * Set and persist a theme
 * @param {'light' | 'dark'} theme - The theme to set
 */
function setTheme(theme) {
    try {
        localStorage.setItem(THEME_STORAGE_KEY, theme);
    } catch (e) {
        // localStorage may not be available
    }
    applyTheme(theme, true);
}

/**
 * Toggle between light and dark theme
 */
function toggleTheme() {
    var currentTheme = document.documentElement.getAttribute('data-theme') || getEffectiveTheme();
    var newTheme = currentTheme === 'dark' ? 'light' : 'dark';
    setTheme(newTheme);
}

/**
 * Create the theme toggle button element
 * @returns {HTMLButtonElement} The theme toggle button
 */
function createThemeToggleButton() {
    var button = document.createElement('button');
    button.type = 'button';
    button.className = 'theme-toggle';
    button.setAttribute('aria-label', 'Toggle dark/light theme');
    button.setAttribute('title', 'Toggle theme');

    // Sun icon (shown in dark mode to switch to light)
    var sunIcon = '<svg class="icon-sun" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' +
        '<circle cx="12" cy="12" r="5"/>' +
        '<line x1="12" y1="1" x2="12" y2="3"/>' +
        '<line x1="12" y1="21" x2="12" y2="23"/>' +
        '<line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/>' +
        '<line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/>' +
        '<line x1="1" y1="12" x2="3" y2="12"/>' +
        '<line x1="21" y1="12" x2="23" y2="12"/>' +
        '<line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/>' +
        '<line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/>' +
        '</svg>';

    // Moon icon (shown in light mode to switch to dark)
    var moonIcon = '<svg class="icon-moon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' +
        '<path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/>' +
        '</svg>';

    button.innerHTML = sunIcon + moonIcon;

    button.addEventListener('click', toggleTheme);

    // Support keyboard activation
    button.addEventListener('keydown', function(e) {
        if (e.key === 'Enter' || e.key === ' ') {
            e.preventDefault();
            toggleTheme();
        }
    });

    return button;
}

/**
 * Initialize the theme toggle functionality
 * Should be called when the DOM is ready
 */
function initThemeToggle() {
    // Apply the effective theme immediately (without transition to prevent flash)
    var effectiveTheme = getEffectiveTheme();
    applyTheme(effectiveTheme, false);

    // Create and insert the toggle button
    var button = createThemeToggleButton();
    document.body.appendChild(button);

    // Listen for system preference changes
    if (window.matchMedia) {
        var mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
        // Use the modern addEventListener if available, otherwise fall back
        if (mediaQuery.addEventListener) {
            mediaQuery.addEventListener('change', function(e) {
                // Only update if there's no stored preference
                if (!getStoredTheme()) {
                    applyTheme(e.matches ? 'dark' : 'light', true);
                }
            });
        } else if (mediaQuery.addListener) {
            // Fallback for older browsers
            mediaQuery.addListener(function(e) {
                if (!getStoredTheme()) {
                    applyTheme(e.matches ? 'dark' : 'light', true);
                }
            });
        }
    }
}

// Export functions for testing and external use
if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        getStoredTheme: getStoredTheme,
        setTheme: setTheme,
        getSystemPreference: getSystemPreference,
        getEffectiveTheme: getEffectiveTheme,
        applyTheme: applyTheme,
        toggleTheme: toggleTheme,
        initThemeToggle: initThemeToggle,
        THEME_STORAGE_KEY: THEME_STORAGE_KEY
    };
}
