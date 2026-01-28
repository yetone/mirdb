/**
 * MirDB Homepage JavaScript
 *
 * Handles interactive functionality for the homepage.
 *
 * Features:
 * - Copy-to-clipboard for code examples
 * - Theme toggle (dark/light mode)
 * - Smooth scroll navigation
 */

(function() {
    'use strict';

    /**
     * Copy code to clipboard functionality
     */
    function initCopyButtons() {
        const copyButtons = document.querySelectorAll('.copy-btn');

        copyButtons.forEach(function(button) {
            button.addEventListener('click', function() {
                const codeContainer = button.closest('.code-container');
                const codeElement = codeContainer.querySelector('code');

                if (codeElement) {
                    const textToCopy = codeElement.textContent;

                    navigator.clipboard.writeText(textToCopy).then(function() {
                        const originalText = button.textContent;
                        button.textContent = 'Copied!';
                        button.classList.add('copied');

                        setTimeout(function() {
                            button.textContent = originalText;
                            button.classList.remove('copied');
                        }, 2000);
                    }).catch(function(err) {
                        console.error('Failed to copy text:', err);
                        button.textContent = 'Failed';
                        setTimeout(function() {
                            button.textContent = 'Copy';
                        }, 2000);
                    });
                }
            });
        });
    }

    /**
     * Smooth scroll for anchor links
     */
    function initSmoothScroll() {
        document.querySelectorAll('a[href^="#"]').forEach(function(anchor) {
            anchor.addEventListener('click', function(e) {
                const targetId = this.getAttribute('href');

                if (targetId === '#') return;

                const targetElement = document.querySelector(targetId);

                if (targetElement) {
                    e.preventDefault();
                    targetElement.scrollIntoView({
                        behavior: 'smooth',
                        block: 'start'
                    });

                    // Update URL without triggering scroll
                    if (history.pushState) {
                        history.pushState(null, null, targetId);
                    }
                }
            });
        });
    }

    /**
     * Initialize all functionality when DOM is ready
     */
    function init() {
        initCopyButtons();
        initSmoothScroll();
    }

    // Initialize when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
