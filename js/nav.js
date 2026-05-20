/**
 * Navigation Interactions
 * Owner: Scenario 6 - Navigation and Header
 *
 * Expected behavior:
 * - Mobile hamburger menu toggle (open/close)
 * - Smooth scroll to anchor sections
 * - Active nav link highlighting on scroll
 * - Theme toggle (light/dark mode) if applicable
 * - Keyboard Escape to close mobile menu
 * - ARIA attribute updates for accessibility
 */

(function () {
  'use strict';

  const HEADER_SCROLL_CLASS = 'scrolled';
  const MENU_OPEN_CLASS = 'menu-open';
  const MOBILE_NAV_OPEN_CLASS = 'open';
  const BACKDROP_ACTIVE_CLASS = 'active';
  const ACTIVE_LINK_CLASS = 'active';
  const THEME_ATTR = 'data-theme';
  const THEME_KEY = 'mirdb-theme';

  let headerEl = null;
  let hamburgerEl = null;
  let mobileNavEl = null;
  let backdropEl = null;
  let themeToggleEl = null;
  let navLinks = [];
  let sections = [];

  /**
   * Initialize navigation functionality
   */
  function init() {
    cacheElements();
    if (!headerEl) return;

    bindEvents();
    initTheme();
    updateActiveLink();
  }

  /**
   * Cache DOM element references
   */
  function cacheElements() {
    headerEl = document.querySelector('header');
    hamburgerEl = document.querySelector('.hamburger');
    mobileNavEl = document.querySelector('.mobile-nav');
    backdropEl = document.querySelector('.mobile-backdrop');
    themeToggleEl = document.querySelector('.theme-toggle');
    navLinks = document.querySelectorAll('.nav-links a[href^="#"]');
    sections = document.querySelectorAll('main section[id]');
  }

  /**
   * Bind all event listeners
   */
  function bindEvents() {
    // Scroll event for sticky header styling and active link
    window.addEventListener('scroll', throttle(onScroll, 100), { passive: true });

    // Hamburger click
    if (hamburgerEl) {
      hamburgerEl.addEventListener('click', toggleMobileMenu);
    }

    // Backdrop click closes menu
    if (backdropEl) {
      backdropEl.addEventListener('click', closeMobileMenu);
    }

    // Smooth scroll for anchor links
    navLinks.forEach(function (link) {
      link.addEventListener('click', onAnchorClick);
    });

    // Theme toggle
    if (themeToggleEl) {
      themeToggleEl.addEventListener('click', toggleTheme);
    }

    // Keyboard: Escape closes mobile menu
    document.addEventListener('keydown', onKeyDown);

    // Logo click scrolls to top
    const logoLink = document.querySelector('.header-logo');
    if (logoLink) {
      logoLink.addEventListener('click', onLogoClick);
    }
  }

  /**
   * Handle scroll events
   */
  function onScroll() {
    updateHeaderShadow();
    updateActiveLink();
  }

  /**
   * Add/remove scrolled class on header based on scroll position
   */
  function updateHeaderShadow() {
    if (!headerEl) return;
    if (window.scrollY > 10) {
      headerEl.classList.add(HEADER_SCROLL_CLASS);
    } else {
      headerEl.classList.remove(HEADER_SCROLL_CLASS);
    }
  }

  /**
   * Toggle mobile menu open/close
   */
  function toggleMobileMenu() {
    if (!mobileNavEl || !hamburgerEl) return;

    const isOpen = mobileNavEl.classList.contains(MOBILE_NAV_OPEN_CLASS);
    if (isOpen) {
      closeMobileMenu();
    } else {
      openMobileMenu();
    }
  }

  /**
   * Open mobile menu
   */
  function openMobileMenu() {
    if (!mobileNavEl || !hamburgerEl) return;

    mobileNavEl.classList.add(MOBILE_NAV_OPEN_CLASS);
    hamburgerEl.setAttribute('aria-expanded', 'true');
    document.body.classList.add(MENU_OPEN_CLASS);

    if (backdropEl) {
      backdropEl.classList.add(BACKDROP_ACTIVE_CLASS);
    }

    // Focus first link in mobile menu
    const firstLink = mobileNavEl.querySelector('.nav-links a');
    if (firstLink) {
      firstLink.focus();
    }
  }

  /**
   * Close mobile menu
   */
  function closeMobileMenu() {
    if (!mobileNavEl || !hamburgerEl) return;

    mobileNavEl.classList.remove(MOBILE_NAV_OPEN_CLASS);
    hamburgerEl.setAttribute('aria-expanded', 'false');
    document.body.classList.remove(MENU_OPEN_CLASS);

    if (backdropEl) {
      backdropEl.classList.remove(BACKDROP_ACTIVE_CLASS);
    }
  }

  /**
   * Handle anchor link clicks with smooth scroll
   */
  function onAnchorClick(event) {
    const href = this.getAttribute('href');
    if (!href || !href.startsWith('#')) return;

    event.preventDefault();
    const targetId = href.substring(1);
    const targetEl = document.getElementById(targetId);

    if (targetEl) {
      // Close mobile menu if open
      closeMobileMenu();

      // Scroll to target
      const headerHeight = headerEl ? headerEl.offsetHeight : 64;
      const targetTop = targetEl.getBoundingClientRect().top + window.scrollY - headerHeight;

      window.scrollTo({
        top: targetTop,
        behavior: 'smooth'
      });
    }
  }

  /**
   * Handle logo click - scroll to top
   */
  function onLogoClick(event) {
    const href = this.getAttribute('href');
    if (href === '#') {
      event.preventDefault();
      window.scrollTo({
        top: 0,
        behavior: 'smooth'
      });
    }
  }

  /**
   * Handle keyboard events
   */
  function onKeyDown(event) {
    if (event.key === 'Escape') {
      closeMobileMenu();
    }
  }

  /**
   * Update active nav link based on scroll position
   */
  function updateActiveLink() {
    if (!sections.length || !navLinks.length) return;

    const headerHeight = headerEl ? headerEl.offsetHeight : 64;
    const scrollPos = window.scrollY + headerHeight + 50;

    let activeId = '';
    for (let i = sections.length - 1; i >= 0; i--) {
      const section = sections[i];
      if (section.offsetTop <= scrollPos) {
        activeId = section.id;
        break;
      }
    }

    navLinks.forEach(function (link) {
      const href = link.getAttribute('href');
      if (href === '#' + activeId) {
        link.classList.add(ACTIVE_LINK_CLASS);
      } else {
        link.classList.remove(ACTIVE_LINK_CLASS);
      }
    });
  }

  /**
   * Toggle between light and dark themes
   */
  function toggleTheme() {
    const currentTheme = document.documentElement.getAttribute(THEME_ATTR);
    const newTheme = currentTheme === 'dark' ? 'light' : 'dark';

    document.documentElement.setAttribute(THEME_ATTR, newTheme);
    localStorage.setItem(THEME_KEY, newTheme);
    updateThemeIcon(newTheme);
  }

  /**
   * Initialize theme from localStorage or system preference
   */
  function initTheme() {
    const savedTheme = localStorage.getItem(THEME_KEY);
    const prefersDark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
    const theme = savedTheme || (prefersDark ? 'dark' : 'light');

    document.documentElement.setAttribute(THEME_ATTR, theme);
    updateThemeIcon(theme);
  }

  /**
   * Update theme toggle button icon
   */
  function updateThemeIcon(theme) {
    if (!themeToggleEl) return;

    const sunIcon = themeToggleEl.querySelector('.icon-sun');
    const moonIcon = themeToggleEl.querySelector('.icon-moon');

    if (sunIcon && moonIcon) {
      if (theme === 'dark') {
        sunIcon.style.display = 'block';
        moonIcon.style.display = 'none';
      } else {
        sunIcon.style.display = 'none';
        moonIcon.style.display = 'block';
      }
    }
  }

  /**
   * Throttle function
   */
  function throttle(fn, delay) {
    let lastCall = 0;
    return function () {
      const now = Date.now();
      if (now - lastCall >= delay) {
        lastCall = now;
        fn.apply(this, arguments);
      }
    };
  }

  // Initialize when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
