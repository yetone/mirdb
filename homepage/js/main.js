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
    // Highlight code blocks with Prism.js if available
    if (typeof Prism !== 'undefined') {
        Prism.highlightAll();
    }
});
