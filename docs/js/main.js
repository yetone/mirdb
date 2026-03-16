/**
 * MirDB Homepage - Main JavaScript
 * Owner: Scenario 4 - Navigation Links
 *
 * This file contains:
 * - Smooth scrolling for anchor links
 * - Mobile navigation toggle
 * - Event delegation setup
 *
 * Note: Site should be functional without JS (progressive enhancement)
 */

(function() {
  'use strict';

  // Mobile navigation toggle
  function initMobileNav() {
    const navToggle = document.querySelector('.nav-toggle');
    const navLinks = document.querySelector('.nav-links');

    if (navToggle && navLinks) {
      navToggle.addEventListener('click', function() {
        const isOpen = navLinks.classList.toggle('open');
        navToggle.setAttribute('aria-expanded', isOpen);
      });

      // Close menu when clicking a link
      navLinks.addEventListener('click', function(e) {
        if (e.target.tagName === 'A') {
          navLinks.classList.remove('open');
          navToggle.setAttribute('aria-expanded', 'false');
        }
      });
    }
  }

  // Smooth scrolling for anchor links
  function initSmoothScroll() {
    document.addEventListener('click', function(e) {
      const target = e.target.closest('a[href^="#"]');
      if (!target) return;

      const hash = target.getAttribute('href');
      if (hash === '#') return;

      const element = document.querySelector(hash);
      if (element) {
        e.preventDefault();
        element.scrollIntoView({
          behavior: 'smooth',
          block: 'start'
        });

        // Update URL hash without jumping
        history.pushState(null, null, hash);
      }
    });
  }

  // Installation tabs functionality
  function initInstallationTabs() {
    const tabButtons = document.querySelectorAll('.installation-tabs .tab-button');
    const tabPanels = document.querySelectorAll('.tab-panels .tab-panel');

    if (tabButtons.length === 0 || tabPanels.length === 0) return;

    tabButtons.forEach(function(button) {
      button.addEventListener('click', function() {
        const targetTab = this.getAttribute('data-tab');

        // Update button states
        tabButtons.forEach(function(btn) {
          btn.classList.remove('active');
          btn.setAttribute('aria-selected', 'false');
        });
        this.classList.add('active');
        this.setAttribute('aria-selected', 'true');

        // Update panel visibility
        tabPanels.forEach(function(panel) {
          if (panel.id === targetTab + '-panel') {
            panel.classList.add('active');
            panel.removeAttribute('hidden');
          } else {
            panel.classList.remove('active');
            panel.setAttribute('hidden', '');
          }
        });
      });

      // Keyboard navigation
      button.addEventListener('keydown', function(e) {
        let targetButton = null;
        const currentIndex = Array.from(tabButtons).indexOf(this);

        if (e.key === 'ArrowRight' || e.key === 'ArrowDown') {
          e.preventDefault();
          targetButton = tabButtons[(currentIndex + 1) % tabButtons.length];
        } else if (e.key === 'ArrowLeft' || e.key === 'ArrowUp') {
          e.preventDefault();
          targetButton = tabButtons[(currentIndex - 1 + tabButtons.length) % tabButtons.length];
        }

        if (targetButton) {
          targetButton.focus();
          targetButton.click();
        }
      });
    });
  }

  // Initialize on DOM ready
  function init() {
    initMobileNav();
    initSmoothScroll();
    initInstallationTabs();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
