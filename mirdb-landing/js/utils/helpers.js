/**
 * Utility Helpers
 * Owner: Shared - First Builder
 *
 * Expected exports:
 * - debounce(fn, delay): Debounce function calls
 * - throttle(fn, limit): Throttle function calls
 * - prefersReducedMotion(): Check reduced motion preference
 */

(function() {
    'use strict';

    /**
     * Debounce function - delays execution until after wait milliseconds
     * have elapsed since the last time it was invoked
     * @param {Function} fn - Function to debounce
     * @param {number} delay - Delay in milliseconds
     * @returns {Function} Debounced function
     */
    function debounce(fn, delay) {
        var timeoutId;
        return function() {
            var context = this;
            var args = arguments;
            clearTimeout(timeoutId);
            timeoutId = setTimeout(function() {
                fn.apply(context, args);
            }, delay);
        };
    }

    /**
     * Throttle function - ensures function is called at most once per limit
     * @param {Function} fn - Function to throttle
     * @param {number} limit - Minimum time between calls in milliseconds
     * @returns {Function} Throttled function
     */
    function throttle(fn, limit) {
        var inThrottle;
        return function() {
            var context = this;
            var args = arguments;
            if (!inThrottle) {
                fn.apply(context, args);
                inThrottle = true;
                setTimeout(function() {
                    inThrottle = false;
                }, limit);
            }
        };
    }

    /**
     * Check if user prefers reduced motion
     * @returns {boolean} True if user prefers reduced motion
     */
    function prefersReducedMotion() {
        return window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    }

    // Export to window for non-module usage
    window.debounce = debounce;
    window.throttle = throttle;
    window.prefersReducedMotion = prefersReducedMotion;
})();
