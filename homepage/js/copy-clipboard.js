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
      const targetId = button.getAttribute('data-copy-target');
      const targetElement = document.getElementById(targetId);

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
  var originalText = button.textContent;
  button.textContent = success ? 'Copied!' : 'Failed';
  button.classList.add(success ? 'copy-success' : 'copy-error');

  setTimeout(function() {
    button.textContent = originalText;
    button.classList.remove('copy-success', 'copy-error');
  }, 2000);
}
