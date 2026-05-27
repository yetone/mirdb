/**
 * Copy-to-clipboard functionality for code blocks.
 * Owner: Scenario 3 - Quick Start & Code Examples
 *
 * Expected behavior:
 * - Adds copy button to all <pre><code> blocks
 * - On click: copies code text to clipboard
 * - Shows visual feedback (icon change + "Copied!" text)
 * - Uses Clipboard API with fallback for older browsers
 * - Respects reduced-motion preference
 */

(function () {
  'use strict';

  document.addEventListener('DOMContentLoaded', function () {
    // Copy buttons are already in the HTML; just wire them up
    document.querySelectorAll('.copy-btn').forEach(function (btn) {
      btn.addEventListener('click', function () {
        const targetId = this.getAttribute('data-target');
        const codeEl = document.getElementById(targetId);
        if (!codeEl) return;

        const text = codeEl.textContent;
        const originalLabel = this.querySelector('.copy-label')?.textContent || 'Copy';

        // Use Clipboard API if available
        if (navigator.clipboard && navigator.clipboard.writeText) {
          navigator.clipboard.writeText(text).then(function () {
            showFeedback(btn, originalLabel);
          }).catch(function () {
            fallbackCopy(text, btn, originalLabel);
          });
        } else {
          fallbackCopy(text, btn, originalLabel);
        }
      });
    });

    function fallbackCopy(text, btn, originalLabel) {
      const textarea = document.createElement('textarea');
      textarea.value = text;
      textarea.setAttribute('aria-hidden', 'true');
      textarea.style.position = 'fixed';
      textarea.style.opacity = '0';
      document.body.appendChild(textarea);
      textarea.select();
      try {
        document.execCommand('copy');
        showFeedback(btn, originalLabel);
      } catch (e) {
        // Copy failed silently
      }
      document.body.removeChild(textarea);
    }

    function showFeedback(btn, originalLabel) {
      const label = btn.querySelector('.copy-label');
      if (label) {
        label.textContent = 'Copied!';
      }
      btn.setAttribute('aria-label', 'Copied to clipboard');
      setTimeout(function () {
        if (label) {
          label.textContent = originalLabel;
        }
        btn.setAttribute('aria-label', 'Copy installation command');
      }, 2000);
    }
  });
})();
