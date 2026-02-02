/**
 * Shared Utility Functions
 * Owner: First Builder
 *
 * Common helper functions used across modules.
 */

/**
 * Query selector wrapper
 * @param {string} selector - CSS selector
 * @param {Element} context - Optional context element
 * @returns {Element|null}
 */
export const $ = (selector, context = document) => context.querySelector(selector);

/**
 * Query selector all wrapper
 * @param {string} selector - CSS selector
 * @param {Element} context - Optional context element
 * @returns {NodeList}
 */
export const $$ = (selector, context = document) => context.querySelectorAll(selector);

/**
 * DOMContentLoaded wrapper
 * @param {Function} callback - Function to run when DOM is ready
 */
export const onReady = (callback) => {
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', callback);
  } else {
    callback();
  }
};

/**
 * Check if browser supports Clipboard API
 * @returns {boolean}
 */
export const supportsClipboard = () => {
  return !!(navigator.clipboard && navigator.clipboard.writeText);
};
