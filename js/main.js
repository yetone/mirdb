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
      var target = document.querySelector(this.getAttribute('href'));
      if (target) {
        e.preventDefault();
        target.scrollIntoView({ behavior: 'smooth' });
      }
    });
  });
}

async function copyToClipboard(text) {
  await navigator.clipboard.writeText(text);
}

function initCopyButtons() {
  // To be implemented by Scenario 5 (Quick Start Section)
}

document.addEventListener('DOMContentLoaded', function() {
  initSmoothScroll();
  initCopyButtons();
});

// Exports for testing
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { initSmoothScroll, initCopyButtons, copyToClipboard };
}
