/**
 * Navigation Module - Mobile Menu Toggle
 * Owner: Scenario 6 - Responsive Design
 *
 * Handles hamburger menu toggle for mobile viewports.
 * Progressive enhancement - navigation works without JS.
 *
 * Expected exports:
 * - initNavigation(): Initialize mobile menu toggle
 */

/**
 * Initialize mobile navigation menu toggle functionality
 * Sets up event listeners for hamburger button and overlay
 */
function initNavigation() {
    const hamburgerBtn = document.querySelector('.hamburger-btn');
    const navMenu = document.querySelector('.nav-menu');
    const navOverlay = document.querySelector('.nav-overlay');

    if (!hamburgerBtn || !navMenu) {
        return;
    }

    /**
     * Toggle menu open/closed state
     */
    function toggleMenu() {
        const isExpanded = hamburgerBtn.getAttribute('aria-expanded') === 'true';

        hamburgerBtn.setAttribute('aria-expanded', !isExpanded);
        navMenu.classList.toggle('is-open', !isExpanded);

        if (navOverlay) {
            navOverlay.classList.toggle('is-visible', !isExpanded);
        }

        // Prevent body scroll when menu is open
        document.body.style.overflow = !isExpanded ? 'hidden' : '';
    }

    /**
     * Close the menu
     */
    function closeMenu() {
        hamburgerBtn.setAttribute('aria-expanded', 'false');
        navMenu.classList.remove('is-open');

        if (navOverlay) {
            navOverlay.classList.remove('is-visible');
        }

        document.body.style.overflow = '';
    }

    // Toggle menu on hamburger button click
    hamburgerBtn.addEventListener('click', function(e) {
        e.stopPropagation();
        toggleMenu();
    });

    // Close menu when clicking overlay
    if (navOverlay) {
        navOverlay.addEventListener('click', closeMenu);
    }

    // Close menu when clicking a nav link
    const navLinks = navMenu.querySelectorAll('.nav-link');
    navLinks.forEach(function(link) {
        link.addEventListener('click', closeMenu);
    });

    // Close menu on Escape key
    document.addEventListener('keydown', function(e) {
        if (e.key === 'Escape' && navMenu.classList.contains('is-open')) {
            closeMenu();
            hamburgerBtn.focus();
        }
    });

    // Close menu on window resize to desktop
    let resizeTimeout;
    window.addEventListener('resize', function() {
        clearTimeout(resizeTimeout);
        resizeTimeout = setTimeout(function() {
            if (window.innerWidth >= 1025) {
                closeMenu();
            }
        }, 100);
    });
}

// Auto-initialize when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initNavigation);
} else {
    initNavigation();
}

// Export for use by other modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { initNavigation };
}
