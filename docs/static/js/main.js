/**
 * Main JavaScript for MirDB Homepage
 * Features: Theme Toggle, Copy to Clipboard, Mobile Menu, Smooth Scroll
 */

(function() {
  'use strict';

  // Theme Toggle
  const initThemeToggle = () => {
    const themeToggle = document.getElementById('theme-toggle');
    const html = document.documentElement;

    // Check for saved theme preference or system preference
    const savedTheme = localStorage.getItem('theme');
    const systemPrefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;

    if (savedTheme === 'dark' || (!savedTheme && systemPrefersDark)) {
      html.classList.add('dark');
    }

    if (themeToggle) {
      themeToggle.addEventListener('click', () => {
        html.classList.toggle('dark');
        const isDark = html.classList.contains('dark');
        localStorage.setItem('theme', isDark ? 'dark' : 'light');
        themeToggle.setAttribute('aria-label', isDark ? 'Switch to light mode' : 'Switch to dark mode');
      });
    }
  };

  // Copy to Clipboard
  const initCopyToClipboard = () => {
    document.querySelectorAll('.copy-button').forEach(button => {
      button.addEventListener('click', async () => {
        const codeBlock = button.closest('.code-block-wrapper').querySelector('code');
        if (codeBlock) {
          try {
            await navigator.clipboard.writeText(codeBlock.textContent);
            button.classList.add('copied');
            button.setAttribute('aria-label', 'Copied!');
            setTimeout(() => {
              button.classList.remove('copied');
              button.setAttribute('aria-label', 'Copy to clipboard');
            }, 2000);
          } catch (err) {
            console.error('Failed to copy:', err);
          }
        }
      });
    });
  };

  // Mobile Menu
  const initMobileMenu = () => {
    const menuButton = document.getElementById('mobile-menu-button');
    const mobileMenu = document.getElementById('mobile-menu');

    if (menuButton && mobileMenu) {
      menuButton.addEventListener('click', () => {
        const isExpanded = menuButton.getAttribute('aria-expanded') === 'true';
        menuButton.setAttribute('aria-expanded', !isExpanded);
        mobileMenu.classList.toggle('hidden');
      });

      // Close menu on Escape key
      document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape' && !mobileMenu.classList.contains('hidden')) {
          menuButton.setAttribute('aria-expanded', 'false');
          mobileMenu.classList.add('hidden');
          menuButton.focus();
        }
      });

      // Close menu when clicking outside
      document.addEventListener('click', (e) => {
        if (!menuButton.contains(e.target) && !mobileMenu.contains(e.target)) {
          menuButton.setAttribute('aria-expanded', 'false');
          mobileMenu.classList.add('hidden');
        }
      });
    }
  };

  // Smooth Scroll for anchor links
  const initSmoothScroll = () => {
    document.querySelectorAll('a[href^="#"]').forEach(anchor => {
      anchor.addEventListener('click', (e) => {
        const targetId = anchor.getAttribute('href');
        if (targetId === '#') return;

        const target = document.querySelector(targetId);
        if (target) {
          e.preventDefault();
          target.scrollIntoView({ behavior: 'smooth', block: 'start' });
          // Update focus for accessibility
          target.setAttribute('tabindex', '-1');
          target.focus();
        }
      });
    });
  };

  // Initialize all features
  document.addEventListener('DOMContentLoaded', () => {
    initThemeToggle();
    initCopyToClipboard();
    initMobileMenu();
    initSmoothScroll();
  });
})();
