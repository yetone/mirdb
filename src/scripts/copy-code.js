/**
 * Copy-to-Clipboard Module
 * Owner: Scenario 3 - Quick Start Code Examples
 *
 * Expected exports/functions:
 * - initCopyButtons(): Attaches click handlers to all .copy-btn elements
 * - copyToClipboard(text): Promise-based clipboard write with fallback
 * - showFeedback(button): Visual feedback (checkmark icon, tooltip)
 */

(function() {
  function copyToClipboard(text) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      return navigator.clipboard.writeText(text);
    }
    // Fallback for older browsers
    var textarea = document.createElement('textarea');
    textarea.value = text;
    textarea.style.position = 'fixed';
    textarea.style.opacity = '0';
    document.body.appendChild(textarea);
    textarea.select();
    try {
      document.execCommand('copy');
      document.body.removeChild(textarea);
      return Promise.resolve();
    } catch (err) {
      document.body.removeChild(textarea);
      return Promise.reject(err);
    }
  }

  function showFeedback(button) {
    var originalText = button.querySelector('.copy-icon').textContent;
    button.querySelector('.copy-icon').textContent = 'Copied!';
    setTimeout(function() {
      button.querySelector('.copy-icon').textContent = originalText;
    }, 2000);
  }

  function initCopyButtons() {
    var buttons = document.querySelectorAll('.copy-btn');
    buttons.forEach(function(button) {
      button.addEventListener('click', function() {
        var code = button.getAttribute('data-code');
        copyToClipboard(code).then(function() {
          showFeedback(button);
        });
      });
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initCopyButtons);
  } else {
    initCopyButtons();
  }
})();
