/**
 * Core JavaScript for MirDB Homepage.
 *
 * Created by the first scenario builder.
 * Contains:
 * - DOMContentLoaded initialization
 * - Smooth scroll for anchor links
 * - Copy-to-clipboard functionality for code blocks
 *
 * Expected exports (for testing):
 * - initSmoothScroll()
 * - initCopyButtons()
 * - copyToClipboard(text): Promise<void>
 */

function initSmoothScroll() {
  document.querySelectorAll('a[href^="#"]').forEach(function(anchor) {
    anchor.addEventListener('click', function(e) {
      var href = this.getAttribute('href');
      var target = document.querySelector(href);
      if (target) {
        e.preventDefault();
        if (typeof target.scrollIntoView === 'function') {
          target.scrollIntoView({ behavior: 'smooth' });
        }
        // Update URL hash without triggering page reload
        if (window.history && window.history.pushState) {
          window.history.pushState(null, null, href);
        }
      }
    });
  });
}

async function copyToClipboard(text) {
  if (navigator.clipboard && navigator.clipboard.writeText) {
    return navigator.clipboard.writeText(text);
  }
  // Fallback for browsers without clipboard API support
  const textarea = document.createElement('textarea');
  textarea.value = text;
  textarea.style.position = 'fixed';
  textarea.style.left = '-9999px';
  textarea.style.top = '0';
  document.body.appendChild(textarea);
  textarea.focus();
  textarea.select();
  try {
    document.execCommand('copy');
  } catch (err) {
    console.error('Fallback copy failed:', err);
    throw new Error('Clipboard API not supported and fallback failed');
  } finally {
    document.body.removeChild(textarea);
  }
}

function initCopyButtons() {
  document.querySelectorAll('.copy-button').forEach(function(button) {
    button.addEventListener('click', async function() {
      var wrapper = button.closest('.code-block-wrapper');
      if (!wrapper) return;

      var codeEl = wrapper.querySelector('code');
      if (!codeEl) return;

      var text = codeEl.textContent;

      try {
        await copyToClipboard(text);

        // Visual feedback
        var originalLabel = button.querySelector('.copy-label').textContent;
        button.querySelector('.copy-label').textContent = 'Copied!';
        button.classList.add('copied');

        setTimeout(function() {
          button.querySelector('.copy-label').textContent = originalLabel;
          button.classList.remove('copied');
        }, 2000);
      } catch (err) {
        // Fallback for environments without clipboard API
        console.error('Failed to copy:', err);
      }
    });
  });
}

document.addEventListener('DOMContentLoaded', function() {
  initSmoothScroll();
  initCopyButtons();
});

// Exports for testing
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { initSmoothScroll, initCopyButtons, copyToClipboard };
}
