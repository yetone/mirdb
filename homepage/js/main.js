/**
 * Main JavaScript for MirDB Homepage
 *
 * Functionality:
 * - Theme toggle (light/dark) with localStorage persistence
 * - Sticky header behavior with shadow on scroll
 * - Back-to-top button (placeholder)
 *
 * Dependencies: None (vanilla JS)
 */

(function() {
  'use strict';

  // Theme Toggle
  const themeToggle = document.querySelector('.theme-toggle');
  const html = document.documentElement;
  const lightIcon = document.querySelector('.theme-toggle__icon--light');
  const darkIcon = document.querySelector('.theme-toggle__icon--dark');

  // Initialize theme from localStorage or system preference
  function initTheme() {
    const savedTheme = localStorage.getItem('theme');

    if (savedTheme) {
      html.setAttribute('data-theme', savedTheme);
      updateThemeIcon(savedTheme);
    } else if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
      html.setAttribute('data-theme', 'dark');
      updateThemeIcon('dark');
    }
  }

  function updateThemeIcon(theme) {
    if (lightIcon && darkIcon) {
      if (theme === 'dark') {
        lightIcon.style.display = 'none';
        darkIcon.style.display = 'block';
      } else {
        lightIcon.style.display = 'block';
        darkIcon.style.display = 'none';
      }
    }
  }

  function toggleTheme() {
    const currentTheme = html.getAttribute('data-theme');
    const newTheme = currentTheme === 'dark' ? 'light' : 'dark';

    html.setAttribute('data-theme', newTheme);
    localStorage.setItem('theme', newTheme);
    updateThemeIcon(newTheme);
  }

  if (themeToggle) {
    themeToggle.addEventListener('click', toggleTheme);
  }

  // Sticky Header with Shadow
  const header = document.querySelector('.header');
  let lastScroll = 0;

  function handleScroll() {
    const currentScroll = window.pageYOffset;

    if (currentScroll > 50) {
      header.classList.add('scrolled');
    } else {
      header.classList.remove('scrolled');
    }

    lastScroll = currentScroll;
  }

  if (header) {
    window.addEventListener('scroll', handleScroll, { passive: true });
  }

  // Back to Top Button
  const backToTopButton = document.getElementById('back-to-top');

  function handleBackToTopVisibility() {
    if (window.pageYOffset > 300) {
      backToTopButton.classList.add('visible');
    } else {
      backToTopButton.classList.remove('visible');
    }
  }

  function scrollToTop() {
    window.scrollTo({
      top: 0,
      behavior: 'smooth'
    });
  }

  if (backToTopButton) {
    window.addEventListener('scroll', handleBackToTopVisibility, { passive: true });
    backToTopButton.addEventListener('click', scrollToTop);
  }

  // Initialize
  initTheme();
})();
