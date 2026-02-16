/**
 * Copy to Clipboard Module
 *
 * Provides shared copy functionality for code blocks.
 */

/**
 * Initialize all copy buttons on the page
 */
function initCopyButtons() {
  const copyButtons = document.querySelectorAll('[data-copy-target]');

  copyButtons.forEach(function(button) {
    button.addEventListener('click', function() {
      const targetSelector = button.getAttribute('data-copy-target');
      const targetElement = document.querySelector(targetSelector);

      if (targetElement) {
        const text = targetElement.textContent || targetElement.innerText;
        copyToClipboard(text, button);
      }
    });
  });
}

/**
 * Copy text to clipboard
 * @param {string} text - Text to copy
 * @param {HTMLElement} button - Button element for feedback
 */
function copyToClipboard(text, button) {
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(text)
      .then(function() {
        showCopyFeedback(button, true);
      })
      .catch(function() {
        fallbackCopy(text, button);
      });
  } else {
    fallbackCopy(text, button);
  }
}

/**
 * Fallback copy method for older browsers
 * @param {string} text - Text to copy
 * @param {HTMLElement} button - Button element for feedback
 */
function fallbackCopy(text, button) {
  var textArea = document.createElement('textarea');
  textArea.value = text;
  textArea.style.position = 'fixed';
  textArea.style.left = '-9999px';
  document.body.appendChild(textArea);
  textArea.focus();
  textArea.select();

  try {
    document.execCommand('copy');
    showCopyFeedback(button, true);
  } catch (err) {
    showCopyFeedback(button, false);
  }

  document.body.removeChild(textArea);
}

/**
 * Show visual feedback on copy
 * @param {HTMLElement} button - Button element
 * @param {boolean} success - Whether copy was successful
 */
function showCopyFeedback(button, success) {
  var copiedClass = button.getAttribute('data-copied-class') || 'getting-started__copy-btn--copied';
  var feedbackDuration = 2000;

  if (success) {
    button.classList.add(copiedClass);
    button.setAttribute('aria-label', 'Copied!');

    setTimeout(function() {
      button.classList.remove(copiedClass);
      button.setAttribute('aria-label', 'Copy to clipboard');
    }, feedbackDuration);
  } else {
    button.style.backgroundColor = 'var(--color-error)';
    button.setAttribute('aria-label', 'Copy failed');

    setTimeout(function() {
      button.style.backgroundColor = '';
      button.setAttribute('aria-label', 'Copy to clipboard');
    }, feedbackDuration);
  }
}

// Auto-initialize on DOMContentLoaded
document.addEventListener('DOMContentLoaded', initCopyButtons);
