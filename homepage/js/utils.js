/**
 * Utility Functions
 * Owner: First Builder
 *
 * Contains:
 * - DOM helper functions
 * - Event utilities
 * - Common operations
 */

(function(window) {
  'use strict';

  const Utils = {
    /**
     * Debounce function execution
     * @param {Function} func - Function to debounce
     * @param {number} wait - Wait time in milliseconds
     * @returns {Function} Debounced function
     */
    debounce: function(func, wait) {
      let timeout;
      return function executedFunction(...args) {
        const later = function() {
          clearTimeout(timeout);
          func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
      };
    },

    /**
     * Check if element is in viewport
     * @param {Element} element - DOM element to check
     * @returns {boolean} True if element is visible
     */
    isInViewport: function(element) {
      const rect = element.getBoundingClientRect();
      return (
        rect.top >= 0 &&
        rect.left >= 0 &&
        rect.bottom <= (window.innerHeight || document.documentElement.clientHeight) &&
        rect.right <= (window.innerWidth || document.documentElement.clientWidth)
      );
    },

    /**
     * Copy text to clipboard
     * @param {string} text - Text to copy
     * @returns {Promise<void>}
     */
    copyToClipboard: async function(text) {
      if (navigator.clipboard && navigator.clipboard.writeText) {
        return navigator.clipboard.writeText(text);
      }
      // Fallback for older browsers
      const textArea = document.createElement('textarea');
      textArea.value = text;
      textArea.style.position = 'fixed';
      textArea.style.left = '-9999px';
      document.body.appendChild(textArea);
      textArea.select();
      try {
        document.execCommand('copy');
      } finally {
        document.body.removeChild(textArea);
      }
    }
  };

  // Export to global scope
  window.MirDBUtils = Utils;
})(window);
