/**
 * Main JavaScript entry point.
 * Owner: Scenario 1 - HTTP Server and Routing (base setup)
 *          Scenario 4 - Navigation Links (mobile menu toggle)
 *          Scenario 5 - CTA Buttons and Actions (click tracking)
 *          Scenario 8 - Dark Mode Theme (theme toggle, localStorage)
 *
 * Expected functionality:
 * - DOMContentLoaded initialization
 * - Theme preference detection and toggle
 * - Mobile navigation menu toggle
 * - Smooth scroll for anchor links
 */

window.App = window.App || {};

window.App.Nav = {
  init: function() {
    this.initMobileMenu();
    this.initAuthState();
  },

  initMobileMenu: function() {
    var toggle = document.querySelector('[data-testid="mobile-menu-toggle"]');
    var menu = document.getElementById('mobile-menu');
    if (!toggle || !menu) return;

    toggle.addEventListener('click', function() {
      var isExpanded = toggle.getAttribute('aria-expanded') === 'true';
      toggle.setAttribute('aria-expanded', String(!isExpanded));
      menu.setAttribute('aria-hidden', String(isExpanded));
      if (isExpanded) {
        menu.classList.add('hidden');
      } else {
        menu.classList.remove('hidden');
      }
    });
  },

  initAuthState: function() {
    var authState = localStorage.getItem('auth_state') || 'unauthenticated';
    this.setAuthState(authState);
  },

  setAuthState: function(state) {
    var isAuth = state === 'authenticated';
    var unauthGroups = document.querySelectorAll('.nav-unauth-links');
    var authGroups = document.querySelectorAll('.nav-auth-links');

    for (var i = 0; i < unauthGroups.length; i++) {
      if (isAuth) {
        unauthGroups[i].classList.add('hidden');
      } else {
        unauthGroups[i].classList.remove('hidden');
      }
    }

    for (var i = 0; i < authGroups.length; i++) {
      if (isAuth) {
        authGroups[i].classList.remove('hidden');
      } else {
        authGroups[i].classList.add('hidden');
      }
    }
  }
};

document.addEventListener('DOMContentLoaded', function() {
  // Initialize theme
  var savedTheme = localStorage.getItem('theme') || 'light';
  document.documentElement.setAttribute('data-theme', savedTheme);

  // Initialize navigation
  window.App.Nav.init();
});
