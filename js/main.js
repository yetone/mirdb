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
        target.scrollIntoView({ behavior: 'smooth' });
        // Update URL hash without triggering page reload
        if (window.history && window.history.pushState) {
          window.history.pushState(null, null, href);
        }
      }
    });
  });
}

async function copyToClipboard(text) {
  await navigator.clipboard.writeText(text);
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

function initThemeToggle() {
  var toggle = document.querySelector('.theme-toggle');
  if (!toggle) return;

  toggle.addEventListener('click', function() {
    var currentTheme = document.documentElement.getAttribute('data-theme');
    var newTheme = currentTheme === 'dark' ? 'light' : 'dark';
    document.documentElement.setAttribute('data-theme', newTheme);
    toggle.setAttribute('aria-pressed', String(newTheme === 'dark'));
    try {
      localStorage.setItem('theme', newTheme);
    } catch (e) {
      // localStorage may not be available
    }
  });
}

document.addEventListener('DOMContentLoaded', function() {
  initSmoothScroll();
  initCopyButtons();
  initThemeToggle();
});

// Exports for testing
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { initSmoothScroll, initCopyButtons, copyToClipboard, initThemeToggle };
}
