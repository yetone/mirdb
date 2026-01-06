/**
 * MirDB Homepage JavaScript
 * Handles copy-to-clipboard functionality with visual feedback
 * and dark mode theme toggling
 */

(function() {
    'use strict';

    const THEME_STORAGE_KEY = 'mirdb-theme';

    /**
     * Initialize theme based on stored preference or system preference
     */
    function initTheme() {
        const storedTheme = localStorage.getItem(THEME_STORAGE_KEY);

        if (storedTheme) {
            // User has explicitly set a preference
            document.documentElement.setAttribute('data-theme', storedTheme);
        }
        // If no stored theme, let CSS handle system preference via @media (prefers-color-scheme)
    }

    /**
     * Get the current effective theme
     * @returns {string} 'light' or 'dark'
     */
    function getCurrentTheme() {
        const storedTheme = localStorage.getItem(THEME_STORAGE_KEY);
        if (storedTheme) {
            return storedTheme;
        }
        // Check system preference
        if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
            return 'dark';
        }
        return 'light';
    }

    /**
     * Toggle between light and dark theme
     */
    function toggleTheme() {
        const currentTheme = getCurrentTheme();
        const newTheme = currentTheme === 'dark' ? 'light' : 'dark';

        document.documentElement.setAttribute('data-theme', newTheme);
        localStorage.setItem(THEME_STORAGE_KEY, newTheme);

        // Dispatch custom event for testing
        document.dispatchEvent(new CustomEvent('theme-changed', {
            detail: { theme: newTheme }
        }));
    }

    /**
     * Initialize theme toggle button
     */
    function initThemeToggle() {
        const themeToggle = document.querySelector('[data-testid="theme-toggle"]');

        if (themeToggle) {
            themeToggle.addEventListener('click', toggleTheme);
        }
    }

    /**
     * Listen for system theme changes
     */
    function initSystemThemeListener() {
        if (window.matchMedia) {
            const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');

            mediaQuery.addEventListener('change', (e) => {
                // Only update if user hasn't set a manual preference
                const storedTheme = localStorage.getItem(THEME_STORAGE_KEY);
                if (!storedTheme) {
                    // Dispatch event for components that need to know
                    document.dispatchEvent(new CustomEvent('theme-changed', {
                        detail: { theme: e.matches ? 'dark' : 'light', source: 'system' }
                    }));
                }
            });
        }
    }

    /**
     * Initialize copy functionality for all copy buttons
     */
    function initCopyButtons() {
        const copyButtons = document.querySelectorAll('.copy-btn');

        copyButtons.forEach(button => {
            button.addEventListener('click', handleCopyClick);
        });
    }

    /**
     * Handle click on copy button
     * @param {Event} event - Click event
     */
    async function handleCopyClick(event) {
        const button = event.currentTarget;
        const codeBlock = button.closest('.code-block');
        const codeElement = codeBlock.querySelector('code');

        if (!codeElement) {
            console.error('Code element not found');
            return;
        }

        const textToCopy = codeElement.textContent;

        try {
            await copyToClipboard(textToCopy);
            showCopyFeedback(button, true);
        } catch (error) {
            console.error('Failed to copy:', error);
            showCopyFeedback(button, false);
        }
    }

    /**
     * Copy text to clipboard
     * @param {string} text - Text to copy
     * @returns {Promise<void>}
     */
    async function copyToClipboard(text) {
        // Use modern Clipboard API if available
        if (navigator.clipboard && navigator.clipboard.writeText) {
            return navigator.clipboard.writeText(text);
        }

        // Fallback for older browsers
        return new Promise((resolve, reject) => {
            const textArea = document.createElement('textarea');
            textArea.value = text;
            textArea.style.position = 'fixed';
            textArea.style.left = '-9999px';
            textArea.style.top = '-9999px';
            textArea.setAttribute('readonly', '');
            document.body.appendChild(textArea);

            try {
                textArea.select();
                textArea.setSelectionRange(0, 99999);
                const successful = document.execCommand('copy');
                document.body.removeChild(textArea);

                if (successful) {
                    resolve();
                } else {
                    reject(new Error('execCommand failed'));
                }
            } catch (error) {
                document.body.removeChild(textArea);
                reject(error);
            }
        });
    }

    /**
     * Show visual feedback after copy attempt
     * @param {HTMLElement} button - The copy button
     * @param {boolean} success - Whether copy was successful
     */
    function showCopyFeedback(button, success) {
        const copyText = button.querySelector('.copy-text');
        const copyIcon = button.querySelector('.copy-icon');

        if (success) {
            button.classList.add('copied');
            copyText.textContent = 'Copied';
            copyIcon.textContent = '✓';

            // Dispatch custom event for testing
            button.dispatchEvent(new CustomEvent('copy-success', {
                bubbles: true,
                detail: { success: true }
            }));
        } else {
            copyText.textContent = 'Failed';
            copyIcon.textContent = '✗';
        }

        // Reset after 2 seconds
        setTimeout(() => {
            button.classList.remove('copied');
            copyText.textContent = 'Copy';
            copyIcon.textContent = '📋';
        }, 2000);
    }

    /**
     * Initialize image error handlers for graceful fallback
     */
    function initImageErrorHandlers() {
        const images = document.querySelectorAll('img');

        images.forEach(img => {
            // Handle images that may have already failed (cached failures)
            if (img.complete && img.naturalHeight === 0) {
                handleImageError(img);
            }

            // Add error handler for future load failures
            img.addEventListener('error', function() {
                handleImageError(this);
            });
        });
    }

    /**
     * Handle image load error
     * @param {HTMLImageElement} img - The image element that failed to load
     */
    function handleImageError(img) {
        // Mark the image as having an error for CSS styling
        img.setAttribute('data-error', 'true');

        // Dispatch custom event for testing
        img.dispatchEvent(new CustomEvent('image-error', {
            bubbles: true,
            detail: { src: img.src, alt: img.alt }
        }));
    }

    /**
     * Initialize all functionality
     */
    function init() {
        initTheme();
        initThemeToggle();
        initSystemThemeListener();
        initCopyButtons();
        initImageErrorHandlers();
    }

    // Initialize when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
