/**
 * MirDB Homepage - Main JavaScript Entry Point
 *
 * Initializes all interactive components:
 * - Navigation module (Scenario 7)
 * - Demo module (Scenario 3)
 * - Clipboard module (Scenario 4)
 *
 * Event listeners:
 * - DOMContentLoaded initialization
 * - Window resize handlers
 */

document.addEventListener('DOMContentLoaded', function() {
  // Initialize navigation if module is available
  if (typeof initNavigation === 'function') {
    initNavigation();
  }

  // Initialize demo if module is available
  if (typeof initDemo === 'function') {
    initDemo();
  }

  // Initialize clipboard if module is available
  if (typeof initClipboard === 'function') {
    initClipboard();
  }

  // Initialize progressive disclosure (Scenario 16)
  initProgressiveDisclosure();

  // Log initialization
  console.log('MirDB Homepage initialized');
});

/**
 * Initialize Progressive Disclosure functionality (Scenario 16)
 * Handles expand/collapse buttons for feature cards with additional details
 */
function initProgressiveDisclosure() {
  const expandButtons = document.querySelectorAll('.feature-card__expand-btn');

  expandButtons.forEach(function(button) {
    button.addEventListener('click', function() {
      const targetId = button.getAttribute('aria-controls');
      const detailsSection = document.getElementById(targetId);

      if (!detailsSection) {
        console.warn('Progressive disclosure: Target element not found:', targetId);
        return;
      }

      const isExpanded = button.getAttribute('aria-expanded') === 'true';

      // Toggle state
      button.setAttribute('aria-expanded', !isExpanded);
      detailsSection.setAttribute('aria-hidden', isExpanded);

      // Update button text
      const textSpan = button.querySelector('.feature-card__expand-text');
      if (textSpan) {
        textSpan.textContent = isExpanded ? 'Learn more' : 'Show less';
      }
    });
  });
}

// Expose for external access
window.initProgressiveDisclosure = initProgressiveDisclosure;
