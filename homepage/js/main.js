/**
 * MirDB Homepage JavaScript Entry Point
 *
 * Imports and initializes all interactive modules.
 * Designed for progressive enhancement - page works without JS.
 *
 * Modules:
 * - theme-toggle.js (Scenario 6)
 * - clipboard.js (Scenario 3)
 * - smooth-scroll.js (Scenario 1)
 */

// Main initialization
document.addEventListener('DOMContentLoaded', function() {
    // Initialize smooth scroll for navigation (Scenario 1)
    if (typeof initSmoothScroll === 'function') {
        initSmoothScroll();
    }

    // Initialize theme toggle (Scenario 6)
    if (typeof initThemeToggle === 'function') {
        initThemeToggle();
    }

    // Initialize clipboard functionality (Scenario 3)
    if (typeof initClipboard === 'function') {
        initClipboard();
    }

    // Highlight code blocks with Prism.js if available
    if (typeof Prism !== 'undefined') {
        Prism.highlightAll();
    }
});
