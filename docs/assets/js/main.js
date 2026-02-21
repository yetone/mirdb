/**
 * Main JavaScript for MirDB Homepage
 * Features: Theme Toggle, Copy to Clipboard, Mobile Menu, Smooth Scroll
 */

(function() {
  'use strict';

  // Theme Toggle
  const initTheme = () => {
    const themeToggle = document.getElementById('theme-toggle');
    const html = document.documentElement;

    // Check for saved theme preference or system preference
    const savedTheme = localStorage.getItem('theme');
    const systemPrefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;

    if (savedTheme === 'dark' || (!savedTheme && systemPrefersDark)) {
      html.classList.add('dark');
    } else {
      html.classList.remove('dark');
    }

    if (themeToggle) {
      themeToggle.addEventListener('click', () => {
        html.classList.toggle('dark');
        const isDark = html.classList.contains('dark');
        localStorage.setItem('theme', isDark ? 'dark' : 'light');
        updateThemeIcon(isDark);
      });

      // Set initial icon
      updateThemeIcon(html.classList.contains('dark'));
    }
  };

  const updateThemeIcon = (isDark) => {
    const sunIcon = document.getElementById('sun-icon');
    const moonIcon = document.getElementById('moon-icon');

    if (sunIcon && moonIcon) {
      if (isDark) {
        sunIcon.classList.remove('hidden');
        moonIcon.classList.add('hidden');
      } else {
        sunIcon.classList.add('hidden');
        moonIcon.classList.remove('hidden');
      }
    }
  };

  // Copy to Clipboard
  const initCopyToClipboard = () => {
    document.querySelectorAll('.copy-btn').forEach(button => {
      button.addEventListener('click', async () => {
        const codeBlock = button.closest('.code-block');
        const code = codeBlock.querySelector('code');

        if (code) {
          try {
            await navigator.clipboard.writeText(code.textContent);
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

      // Close on Escape
      document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape' && !mobileMenu.classList.contains('hidden')) {
          mobileMenu.classList.add('hidden');
          menuButton.setAttribute('aria-expanded', 'false');
          menuButton.focus();
        }
      });

      // Close when clicking outside
      document.addEventListener('click', (e) => {
        if (!menuButton.contains(e.target) && !mobileMenu.contains(e.target)) {
          mobileMenu.classList.add('hidden');
          menuButton.setAttribute('aria-expanded', 'false');
        }
      });
    }
  };

  // Smooth Scroll
  const initSmoothScroll = () => {
    document.querySelectorAll('a[href^="#"]').forEach(anchor => {
      anchor.addEventListener('click', (e) => {
        const href = anchor.getAttribute('href');
        if (href === '#') return;

        const target = document.querySelector(href);
        if (target) {
          e.preventDefault();
          target.scrollIntoView({
            behavior: 'smooth',
            block: 'start'
          });

          // Update URL without triggering scroll
          history.pushState(null, null, href);

          // Set focus for accessibility
          target.setAttribute('tabindex', '-1');
          target.focus({ preventScroll: true });
        }
      });
    });
  };

  // Initialize all features
  document.addEventListener('DOMContentLoaded', () => {
    initTheme();
    initCopyToClipboard();
    initMobileMenu();
    initSmoothScroll();
  });
})();
