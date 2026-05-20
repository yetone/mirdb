/**
 * Shared JavaScript Utilities
 * Owner: First Builder
 *
 * Contains:
 * - DOMContentLoaded wrapper
 * - debounce() / throttle() utilities
 * - Element visibility helpers
 * - Clipboard fallback polyfill
 *
 * Expected exports (global or module):
 * - ready(callback): Execute callback when DOM is ready
 * - debounce(fn, delay): Debounced function wrapper
 * - throttle(fn, delay): Throttled function wrapper
 */

function ready(callback) {
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', callback);
  } else {
    callback();
  }
}

function debounce(fn, delay) {
  let timer;
  return function (...args) {
    clearTimeout(timer);
    timer = setTimeout(() => fn.apply(this, args), delay);
  };
}

function throttle(fn, delay) {
  let lastCall = 0;
  return function (...args) {
    const now = Date.now();
    if (now - lastCall >= delay) {
      lastCall = now;
      fn.apply(this, args);
    }
  };
}

function isElementVisible(el) {
  const rect = el.getBoundingClientRect();
  return rect.top >= 0 && rect.bottom <= window.innerHeight;
}

// Export for module systems or attach to window
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { ready, debounce, throttle, isElementVisible };
} else {
  window.mirDB = { ready, debounce, throttle, isElementVisible };
}
