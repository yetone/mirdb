/**
 * Toast notification helper.
 * Owner: Scenario 3 (consumed by clipboard.js)
 *
 * Renders an aria-live region update so screen readers announce the result
 * of the user's copy action. The toast is removed after `durationMs`.
 *
 * Dual-mode: exports via CommonJS for Jest tests, attaches to window
 * (`window.MirdbToast`) when loaded as a plain script in the browser.
 */

(function (root) {
  'use strict';

  const DEFAULT_DURATION_MS = 2500;
  const REGION_ID = 'toast-region';

  function ensureRegion(doc) {
    let region = doc.getElementById(REGION_ID);
    if (!region) {
      region = doc.createElement('div');
      region.id = REGION_ID;
      region.setAttribute('role', 'status');
      region.setAttribute('aria-live', 'polite');
      region.setAttribute('aria-atomic', 'true');
      doc.body.appendChild(region);
    }
    return region;
  }

  function showToast(message, opts) {
    const options = opts || {};
    const variant = options.variant === 'error' ? 'error' : 'success';
    const durationMs = typeof options.durationMs === 'number'
      ? options.durationMs
      : DEFAULT_DURATION_MS;
    const doc = options.document || (typeof document !== 'undefined' ? document : null);
    if (!doc) return null;

    const region = ensureRegion(doc);
    const toast = doc.createElement('div');
    toast.className = 'toast toast--' + variant;
    toast.setAttribute('data-variant', variant);
    toast.textContent = message;
    region.appendChild(toast);

    if (typeof setTimeout === 'function') {
      setTimeout(function () {
        if (toast.parentNode === region) {
          region.removeChild(toast);
        }
      }, durationMs);
    }

    return toast;
  }

  const api = { showToast: showToast };

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = api;
  }
  if (root) {
    root.MirdbToast = api;
  }
})(typeof self !== 'undefined' ? self : typeof window !== 'undefined' ? window : null);
