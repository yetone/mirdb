/**
 * Copy-to-clipboard for the code-example section.
 * Owner: Scenario 3 — Get Started code example
 *
 * Exports:
 *   initClipboard(root?: Document) -> void
 *     Binds click on every `.copy-btn` inside the document. On click,
 *     copies the neighboring <code> element's text via navigator.clipboard
 *     and surfaces a success / error toast.
 *
 *   copyText(text, opts?) -> Promise<boolean>
 *     Low-level helper. Resolves true on successful clipboard write,
 *     false otherwise. Always emits a toast unless `opts.toast === false`.
 *
 * Dual-mode: exports via CommonJS for Jest tests, attaches to window
 * (`window.MirdbClipboard`) when loaded as a plain script in the browser.
 */

(function (root) {
  'use strict';

  const DEFAULT_SUCCESS_MESSAGE = 'Copied to clipboard';
  const DEFAULT_ERROR_MESSAGE = 'Could not copy to clipboard';

  function loadToast() {
    if (typeof module !== 'undefined' && module.exports && typeof require === 'function') {
      try {
        return require('./toast.js');
      } catch (_err) {
        /* fall through */
      }
    }
    if (root && root.MirdbToast) return root.MirdbToast;
    return null;
  }

  function resolveToast(opts) {
    if (opts && typeof opts.toast === 'function') return opts.toast;
    const mod = loadToast();
    if (mod && typeof mod.showToast === 'function') return mod.showToast;
    return function () {};
  }

  function getClipboard(opts) {
    if (opts && opts.clipboard) return opts.clipboard;
    if (typeof navigator !== 'undefined' && navigator.clipboard) {
      return navigator.clipboard;
    }
    return null;
  }

  async function copyText(text, opts) {
    const showToast = resolveToast(opts);
    const clipboard = getClipboard(opts);
    const emitToast = !(opts && opts.toast === false);

    if (!clipboard || typeof clipboard.writeText !== 'function') {
      if (emitToast) showToast(DEFAULT_ERROR_MESSAGE, { variant: 'error' });
      return false;
    }

    try {
      await clipboard.writeText(String(text == null ? '' : text));
      if (emitToast) showToast(DEFAULT_SUCCESS_MESSAGE, { variant: 'success' });
      return true;
    } catch (_err) {
      if (emitToast) showToast(DEFAULT_ERROR_MESSAGE, { variant: 'error' });
      return false;
    }
  }

  function findCodeText(button) {
    const block =
      (button.closest && button.closest('.code-block')) || button.parentElement;
    if (!block) return '';
    const code = block.querySelector('pre code') || block.querySelector('code');
    if (!code) return '';
    return code.textContent || '';
  }

  function initClipboard(rootDoc) {
    const doc = rootDoc || (typeof document !== 'undefined' ? document : null);
    if (!doc) return;

    const buttons = doc.querySelectorAll('.copy-btn');
    buttons.forEach(function (button) {
      if (button.dataset.clipboardBound === 'true') return;
      button.dataset.clipboardBound = 'true';
      button.addEventListener('click', async function () {
        const text = findCodeText(button);
        await copyText(text);
      });
    });
  }

  const api = { initClipboard: initClipboard, copyText: copyText };

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = api;
  }
  if (root) {
    root.MirdbClipboard = api;
  }
})(typeof self !== 'undefined' ? self : typeof window !== 'undefined' ? window : null);
