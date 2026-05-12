/**
 * Shared scroll-reveal helper. No-op in jsdom test runs (IntersectionObserver
 * is unavailable), where the function simply returns without binding.
 *
 * Dual-mode: exports via CommonJS for Jest tests, attaches to window
 * (`window.MirdbAnimations`) when loaded as a plain script in the browser.
 */

(function (root) {
  'use strict';

  function initAnimations(rootDoc) {
    const doc = rootDoc || (typeof document !== 'undefined' ? document : null);
    if (!doc) return;
    if (typeof IntersectionObserver === 'undefined') return;

    const observer = new IntersectionObserver(
      function (entries) {
        for (let i = 0; i < entries.length; i++) {
          const entry = entries[i];
          if (entry.isIntersecting) {
            entry.target.classList.add('is-revealed');
            observer.unobserve(entry.target);
          }
        }
      },
      { threshold: 0.1 }
    );

    const targets = doc.querySelectorAll('[data-reveal]');
    targets.forEach(function (el) {
      observer.observe(el);
    });
  }

  const api = { initAnimations: initAnimations };
  if (typeof module !== 'undefined' && module.exports) {
    module.exports = api;
  }
  if (root) {
    root.MirdbAnimations = api;
  }
})(typeof self !== 'undefined' ? self : typeof window !== 'undefined' ? window : null);
