/**
 * Navigation Module
 * Owner: Scenario 6 - Navigation and CTAs
 *
 * Handles:
 * - Mobile menu toggle
 * - Smooth scroll to sections
 * - Active section highlighting
 * - Sticky header behavior
 */

import { $, $$ } from '../utils/helpers.js';

let isMenuOpen = false;
let activeSection = null;

/**
 * Initialize navigation functionality
 */
export const init = () => {
  setupMobileMenu();
  setupSmoothScroll();
  setupActiveSection();
  setupStickyHeader();
};

/**
 * Setup mobile menu toggle
 */
const setupMobileMenu = () => {
  const toggle = $('.nav-toggle');
  const navLinks = $('.nav-links');
  const navCta = $('.nav-cta');

  if (!toggle || !navLinks) return;

  toggle.addEventListener('click', () => {
    isMenuOpen = !isMenuOpen;
    toggle.setAttribute('aria-expanded', isMenuOpen.toString());
    toggle.classList.toggle('nav-toggle--active', isMenuOpen);
    navLinks.classList.toggle('nav-links--open', isMenuOpen);
    if (navCta) {
      navCta.classList.toggle('nav-cta--open', isMenuOpen);
    }
    document.body.classList.toggle('menu-open', isMenuOpen);
  });

  // Close menu when clicking a link
  $$('.nav-link', navLinks).forEach((link) => {
    link.addEventListener('click', () => {
      if (isMenuOpen) {
        isMenuOpen = false;
        toggle.setAttribute('aria-expanded', 'false');
        toggle.classList.remove('nav-toggle--active');
        navLinks.classList.remove('nav-links--open');
        if (navCta) {
          navCta.classList.remove('nav-cta--open');
        }
        document.body.classList.remove('menu-open');
      }
    });
  });

  // Close menu on escape key
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && isMenuOpen) {
      isMenuOpen = false;
      toggle.setAttribute('aria-expanded', 'false');
      toggle.classList.remove('nav-toggle--active');
      navLinks.classList.remove('nav-links--open');
      if (navCta) {
        navCta.classList.remove('nav-cta--open');
      }
      document.body.classList.remove('menu-open');
      toggle.focus();
    }
  });
};

/**
 * Smooth scroll to section
 * @param {string} sectionId - ID of the section to scroll to
 */
export const scrollToSection = (sectionId) => {
  const section = document.getElementById(sectionId);
  if (!section) return;

  const header = $('header');
  const headerHeight = header ? header.offsetHeight : 0;
  const targetPosition = section.offsetTop - headerHeight - 16;

  window.scrollTo({
    top: targetPosition,
    behavior: 'smooth',
  });
};

/**
 * Setup smooth scroll for navigation links
 */
const setupSmoothScroll = () => {
  $$('a[href^="#"]').forEach((link) => {
    link.addEventListener('click', (e) => {
      const href = link.getAttribute('href');
      if (!href || href === '#') return;

      const targetId = href.slice(1);
      const targetSection = document.getElementById(targetId);

      if (targetSection) {
        e.preventDefault();
        scrollToSection(targetId);

        // Update URL without triggering scroll
        history.pushState(null, '', href);
      }
    });
  });
};

/**
 * Setup active section highlighting based on scroll position
 */
const setupActiveSection = () => {
  const sections = $$('main section[id]');
  const navLinks = $$('.nav-link');

  if (!sections.length || !navLinks.length) return;

  const observerOptions = {
    root: null,
    rootMargin: '-20% 0px -70% 0px',
    threshold: 0,
  };

  const observerCallback = (entries) => {
    entries.forEach((entry) => {
      if (entry.isIntersecting) {
        const id = entry.target.id;
        activeSection = id;

        navLinks.forEach((link) => {
          const href = link.getAttribute('href');
          if (href === `#${id}`) {
            link.classList.add('nav-link--active');
          } else if (href && href.startsWith('#')) {
            link.classList.remove('nav-link--active');
          }
        });
      }
    });
  };

  const observer = new IntersectionObserver(observerCallback, observerOptions);
  sections.forEach((section) => observer.observe(section));
};

/**
 * Setup sticky header with shadow on scroll
 */
const setupStickyHeader = () => {
  const header = $('header');
  if (!header) return;

  let lastScrollY = 0;
  let ticking = false;

  const updateHeader = () => {
    const scrollY = window.scrollY;

    // Add shadow when scrolled
    header.classList.toggle('header--scrolled', scrollY > 10);

    lastScrollY = scrollY;
    ticking = false;
  };

  window.addEventListener('scroll', () => {
    if (!ticking) {
      requestAnimationFrame(updateHeader);
      ticking = true;
    }
  });
};

/**
 * Get currently active section
 * @returns {string|null} - ID of the active section
 */
export const getActiveSection = () => activeSection;
