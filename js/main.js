/**
 * MirDB Homepage JavaScript
 * Minimal JavaScript for copy-to-clipboard functionality
 */

function copyToClipboard(text) {
  if (navigator.clipboard && navigator.clipboard.writeText) {
    return navigator.clipboard.writeText(text);
  }
  // Fallback for older browsers
  const textarea = document.createElement('textarea');
  textarea.value = text;
  textarea.style.position = 'fixed';
  textarea.style.opacity = '0';
  document.body.appendChild(textarea);
  textarea.select();
  document.execCommand('copy');
  document.body.removeChild(textarea);
  return Promise.resolve();
}

function showCopyFeedback(element) {
  const originalText = element.textContent;
  element.textContent = 'Copied!';
  setTimeout(() => {
    element.textContent = originalText;
  }, 2000);
}

function initCopyButtons() {
  const copyButtons = document.querySelectorAll('.copy-btn');
  copyButtons.forEach(btn => {
    btn.addEventListener('click', () => {
      const text = btn.getAttribute('data-clipboard');
      if (text) {
        copyToClipboard(text).then(() => {
          showCopyFeedback(btn);
        });
      }
    });
  });
}

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', initCopyButtons);
