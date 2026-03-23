/**
 * Main JavaScript
 * Owner: First Builder
 *
 * Expected functionality:
 * - Smooth scroll navigation
 * - Mobile menu toggle
 * - Optional: Code block copy functionality
 *
 * Requirements:
 * - Vanilla JavaScript (no dependencies)
 * - Graceful degradation if JS disabled
 */

(function() {
  'use strict';

  // DOM Elements
  const navToggle = document.querySelector('.nav-toggle');
  const navLinks = document.querySelector('.nav-links');

  // Mobile menu toggle
  if (navToggle && navLinks) {
    navToggle.addEventListener('click', function() {
      const isExpanded = navToggle.getAttribute('aria-expanded') === 'true';
      navToggle.setAttribute('aria-expanded', !isExpanded);
      navLinks.classList.toggle('active');
    });

    // Close mobile menu when clicking a link
    navLinks.querySelectorAll('.nav-link').forEach(function(link) {
      link.addEventListener('click', function() {
        navLinks.classList.remove('active');
        navToggle.setAttribute('aria-expanded', 'false');
      });
    });
  }

  // Close mobile menu when clicking outside
  document.addEventListener('click', function(event) {
    if (navLinks && navLinks.classList.contains('active')) {
      if (!event.target.closest('.nav')) {
        navLinks.classList.remove('active');
        navToggle.setAttribute('aria-expanded', 'false');
      }
    }
  });

  // Smooth scroll for anchor links (fallback for browsers without CSS scroll-behavior)
  document.querySelectorAll('a[href^="#"]').forEach(function(anchor) {
    anchor.addEventListener('click', function(event) {
      const href = this.getAttribute('href');
      if (href === '#') return;

      const target = document.querySelector(href);
      if (target) {
        event.preventDefault();
        target.scrollIntoView({
          behavior: 'smooth',
          block: 'start'
        });

        // Update URL without triggering scroll
        if (history.pushState) {
          history.pushState(null, null, href);
        }
      }
    });
  });

  // Add keyboard support for navigation
  navLinks && navLinks.querySelectorAll('.nav-link').forEach(function(link, index, links) {
    link.addEventListener('keydown', function(event) {
      let targetIndex;

      switch(event.key) {
        case 'ArrowRight':
        case 'ArrowDown':
          targetIndex = (index + 1) % links.length;
          links[targetIndex].focus();
          event.preventDefault();
          break;
        case 'ArrowLeft':
        case 'ArrowUp':
          targetIndex = (index - 1 + links.length) % links.length;
          links[targetIndex].focus();
          event.preventDefault();
          break;
        case 'Home':
          links[0].focus();
          event.preventDefault();
          break;
        case 'End':
          links[links.length - 1].focus();
          event.preventDefault();
          break;
      }
    });
  });
})();
