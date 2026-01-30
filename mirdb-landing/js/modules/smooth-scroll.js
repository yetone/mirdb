/**
 * Smooth Scroll Module
 * Owner: Scenario 2 - Navigation and Smooth Scrolling
 *
 * Expected exports:
 * - initSmoothScroll(): Initialize smooth scrolling for anchor links
 *
 * Uses scroll-behavior: smooth CSS with JS enhancement for hash updates
 */

(function() {
    'use strict';

    /**
     * Initialize smooth scrolling for anchor links
     * Uses CSS scroll-behavior: smooth as the primary method
     * JavaScript handles hash updates and focus management
     */
    function initSmoothScroll() {
        const anchorLinks = document.querySelectorAll('a[href^="#"]');

        anchorLinks.forEach(function(link) {
            link.addEventListener('click', handleAnchorClick);
        });

        // Handle initial hash in URL
        if (window.location.hash) {
            handleInitialHash();
        }
    }

    /**
     * Handle click on anchor links
     * @param {Event} event - Click event
     */
    function handleAnchorClick(event) {
        const href = this.getAttribute('href');

        // Skip if it's just "#" or empty
        if (href === '#' || href === '') {
            return;
        }

        const targetId = href.slice(1);
        const targetElement = document.getElementById(targetId);

        if (targetElement) {
            event.preventDefault();

            // Scroll to target (CSS handles smooth behavior)
            targetElement.scrollIntoView({
                behavior: 'smooth',
                block: 'start'
            });

            // Update URL hash without jumping
            if (history.pushState) {
                history.pushState(null, null, href);
            } else {
                window.location.hash = href;
            }

            // Set focus on target for accessibility
            targetElement.setAttribute('tabindex', '-1');
            targetElement.focus({ preventScroll: true });

            // Remove tabindex after blur to not affect tab order
            targetElement.addEventListener('blur', function removeTabindex() {
                targetElement.removeAttribute('tabindex');
                targetElement.removeEventListener('blur', removeTabindex);
            });

            // Close mobile menu if open
            closeMobileMenu();
        }
    }

    /**
     * Handle initial hash in URL on page load
     */
    function handleInitialHash() {
        // Small delay to ensure page is fully loaded
        setTimeout(function() {
            const hash = window.location.hash;
            if (hash) {
                const targetElement = document.querySelector(hash);
                if (targetElement) {
                    targetElement.scrollIntoView({
                        behavior: 'smooth',
                        block: 'start'
                    });
                }
            }
        }, 100);
    }

    /**
     * Close mobile navigation menu
     */
    function closeMobileMenu() {
        const toggle = document.querySelector('.nav__toggle');
        const links = document.querySelector('.nav__links');

        if (toggle && links) {
            toggle.setAttribute('aria-expanded', 'false');
            links.classList.remove('nav__links--open');
        }
    }

    // Export for use
    window.initSmoothScroll = initSmoothScroll;

    // Auto-initialize when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initSmoothScroll);
    } else {
        initSmoothScroll();
    }
})();
