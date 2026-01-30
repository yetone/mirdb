/**
 * MirDB Landing Page - Main JavaScript
 * Owner: First scenario builder
 *
 * This file initializes all JavaScript functionality:
 * - Import and initialize navigation
 * - Import and initialize clipboard functionality
 * - Import and initialize animations
 * - Set up event listeners for interactive elements
 */

document.addEventListener('DOMContentLoaded', function() {
  // Initialization will be handled by individual modules
  // when they are implemented by their respective scenario owners
  console.log('MirDB Landing Page initialized');

  // Initialize navigation - Owner: Scenario 8
  if (typeof initNavigation === 'function') {
    initNavigation();
  }

  // Initialize configuration section collapsible - Owner: Scenario 5
  initConfigurationToggle();
});

/**
 * Initialize the configuration section collapsible toggle
 * Owner: Scenario 5 - Configuration Section
 */
function initConfigurationToggle() {
  var toggleBtn = document.getElementById('config-toggle-btn');
  var content = document.getElementById('config-content');

  if (!toggleBtn || !content) {
    return;
  }

  toggleBtn.addEventListener('click', function() {
    var isExpanded = toggleBtn.getAttribute('aria-expanded') === 'true';

    // Toggle expanded state
    toggleBtn.setAttribute('aria-expanded', !isExpanded);

    // Toggle content visibility
    if (isExpanded) {
      content.classList.add('collapsed');
    } else {
      content.classList.remove('collapsed');
    }
  });
}
