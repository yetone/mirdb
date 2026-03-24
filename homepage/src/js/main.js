/**
 * Main JavaScript Entry Point
 * Owner: First scenario builder
 *
 * Initializes all JavaScript modules:
 * - Theme toggle
 * - Copy-to-clipboard
 * - Smooth scroll
 * - Mobile navigation
 */

document.addEventListener('DOMContentLoaded', function() {
    // Initialize modules when they are available
    if (typeof initTheme === 'function') {
        initTheme();
    }

    if (typeof initCopyButtons === 'function') {
        initCopyButtons();
    }

    if (typeof initSmoothScroll === 'function') {
        initSmoothScroll();
    }

    if (typeof initMobileNav === 'function') {
        initMobileNav();
    }
});
