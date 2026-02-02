/**
 * Shared Utility Functions
 * Owner: First Builder
 *
 * Common helper functions used across modules:
 * - DOM query helpers
 * - Event delegation utilities
 * - Browser feature detection
 *
 * Expected exports:
 * - $(selector): querySelector wrapper
 * - $$(selector): querySelectorAll wrapper
 * - onReady(callback): DOMContentLoaded wrapper
 */

/**
 * Query selector wrapper
 * @param {string} selector - CSS selector
 * @param {Element} context - Context element (default: document)
 * @returns {Element|null}
 */
export function $(selector, context = document) {
    return context.querySelector(selector);
}

/**
 * Query selector all wrapper
 * @param {string} selector - CSS selector
 * @param {Element} context - Context element (default: document)
 * @returns {NodeList}
 */
export function $$(selector, context = document) {
    return context.querySelectorAll(selector);
}

/**
 * Execute callback when DOM is ready
 * @param {Function} callback - Function to execute
 */
export function onReady(callback) {
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', callback);
    } else {
        callback();
    }
}

/**
 * Check if browser supports a feature
 * @param {string} feature - Feature name
 * @returns {boolean}
 */
export function supports(feature) {
    const features = {
        clipboard: 'clipboard' in navigator,
        intersectionObserver: 'IntersectionObserver' in window,
        cssVariables: window.CSS && CSS.supports('color', 'var(--test)'),
    };
    return features[feature] ?? false;
}
