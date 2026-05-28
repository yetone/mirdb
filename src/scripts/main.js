/**
 * Main Entry Point
 * Owner: First builder to initialize scripts
 *
 * Expected contents:
 * - DOMContentLoaded initialization
 * - Section scroll-spy for active nav highlighting
 * - Smooth scroll for anchor links
 */

document.addEventListener('DOMContentLoaded', function() {
  // Scenario 4: Platform tab switching for Installation section
  const platformTabs = document.querySelectorAll('.platform-tab');
  platformTabs.forEach(function(tab) {
    tab.addEventListener('click', function() {
      const targetId = tab.getAttribute('aria-controls');
      const tablist = tab.closest('[role="tablist"]');

      // Update tab states
      tablist.querySelectorAll('.platform-tab').forEach(function(t) {
        t.classList.remove('active');
        t.setAttribute('aria-selected', 'false');
      });
      tab.classList.add('active');
      tab.setAttribute('aria-selected', 'true');

      // Update panel visibility
      const panels = tablist.nextElementSibling.querySelectorAll('.platform-panel');
      panels.forEach(function(panel) {
        if (panel.id === targetId) {
          panel.classList.add('active');
          panel.removeAttribute('hidden');
        } else {
          panel.classList.remove('active');
          panel.setAttribute('hidden', '');
        }
      });
    });
  });

  // Scenario 12: Sticky header scroll handling
  var header = document.querySelector('.site-header');
  if (header) {
    function updateHeaderScroll() {
      if (window.scrollY > 10) {
        header.classList.add('is-scrolled');
      } else {
        header.classList.remove('is-scrolled');
      }
    }
    window.addEventListener('scroll', updateHeaderScroll, { passive: true });
    updateHeaderScroll();
  }
});
