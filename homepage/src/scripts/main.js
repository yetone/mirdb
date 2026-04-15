/**
 * Main Entry Point
 *
 * Initializes all scripts and components on the homepage.
 */

import { initCodeBlock } from '../components/QuickStart/CodeBlock.js';

/**
 * Initialize mobile navigation toggle
 * Required for Scenario 6 - Responsive Design - Mobile
 */
function initMobileNav() {
  const toggle = document.querySelector('.nav__toggle');
  const navLinks = document.querySelector('.nav__links');

  if (toggle && navLinks) {
    toggle.addEventListener('click', () => {
      const isOpen = navLinks.classList.toggle('nav__links--open');
      toggle.setAttribute('aria-expanded', String(isOpen));
    });

    // Close menu when clicking a link
    navLinks.querySelectorAll('.nav__link').forEach(link => {
      link.addEventListener('click', () => {
        navLinks.classList.remove('nav__links--open');
        toggle.setAttribute('aria-expanded', 'false');
      });
    });
  }
}

document.addEventListener('DOMContentLoaded', () => {
  console.log('MirDB Homepage loaded');

  // Initialize code block functionality
  initCodeBlock();

  // Initialize mobile navigation
  initMobileNav();
});
