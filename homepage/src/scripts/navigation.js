/**
 * Navigation behavior — smooth scroll and mobile menu.
 * Owner: Scenario 4 — Navigation & external links
 *
 * Exports:
 *   initNavigation(root?: Document) -> void
 *     Binds smooth-scroll on internal anchors and toggles `.nav-menu` open
 *     when `.nav-toggle` is clicked. Updates aria-expanded.
 */

(function () {
  'use strict';

  function initNavigation(root) {
    var doc = root || (typeof document !== 'undefined' ? document : null);
    if (!doc) return;

    var toggle = doc.querySelector('.nav-toggle');
    var menu = doc.getElementById('nav-menu');

    if (toggle && menu) {
      toggle.addEventListener('click', function () {
        var isExpanded = toggle.getAttribute('aria-expanded') === 'true';
        var newState = !isExpanded;
        toggle.setAttribute('aria-expanded', String(newState));
        menu.classList.toggle('open', newState);
      });
    }

    // Smooth scroll for internal anchor links
    var internalAnchors = doc.querySelectorAll('a[href^="#"]');
    internalAnchors.forEach(function (anchor) {
      anchor.addEventListener('click', function (event) {
        var href = anchor.getAttribute('href');
        if (!href || href === '#') return;

        var target = doc.querySelector(href);
        if (target) {
          event.preventDefault();
          target.scrollIntoView({ behavior: 'smooth' });

          // Close mobile menu after navigating
          if (toggle && menu && toggle.getAttribute('aria-expanded') === 'true') {
            toggle.setAttribute('aria-expanded', 'false');
            menu.classList.remove('open');
          }
        }
      });
    });

    // Keyboard: close menu on Escape
    doc.addEventListener('keydown', function (event) {
      if (event.key === 'Escape' && toggle && menu) {
        if (toggle.getAttribute('aria-expanded') === 'true') {
          toggle.setAttribute('aria-expanded', 'false');
          menu.classList.remove('open');
        }
      }
    });
  }

  // Expose for tests and for main.js
  if (typeof window !== 'undefined') {
    window.MirdbNavigation = { initNavigation: initNavigation };
  }

  // Auto-initialize if this script is loaded standalone in a browser.
  // Tests can set `document.MirdbNavigationSkipAutoInit = true` before
  // requiring this module to avoid double-binding.
  if (typeof document !== 'undefined' && document.readyState !== 'undefined') {
    if (document.MirdbNavigationSkipAutoInit) {
      // skip — test will call initNavigation manually
    } else if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', function () {
        initNavigation(document);
      });
    } else {
      initNavigation(document);
    }
  }
})();
