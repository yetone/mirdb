/**
 * Homepage JS entrypoint.
 * Created by the first scenario builder.
 *
 * Responsibilities:
 *   - Initialize clipboard handler (Scenario 3)
 *   - Initialize scroll-reveal animations (Shared)
 *
 * Loaded as a plain <script> after clipboard.js / toast.js / animations.js so
 * the helpers are available on `window` (MirdbClipboard, MirdbToast).
 */

(function () {
  'use strict';

  function init() {
    if (typeof window === 'undefined') return;
    if (window.MirdbClipboard && typeof window.MirdbClipboard.initClipboard === 'function') {
      window.MirdbClipboard.initClipboard(document);
    }
    if (window.MirdbAnimations && typeof window.MirdbAnimations.initAnimations === 'function') {
      window.MirdbAnimations.initAnimations(document);
    }
  }

  if (typeof document !== 'undefined') {
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', init);
    } else {
      init();
    }
  }
})();
