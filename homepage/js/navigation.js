/**
 * Navigation JavaScript for MirDB Homepage
 * Owner: Scenario 1 - Header and Navigation
 *
 * Functionality:
 * - Mobile hamburger menu toggle
 * - Active section highlighting in nav
 * - Smooth scroll to anchors
 * - Close mobile menu on link click
 */

(function() {
  'use strict';

  // Mobile Menu Toggle
  const mobileMenuToggle = document.querySelector('.mobile-menu-toggle');
  const navList = document.querySelector('.nav__list');
  const navLinks = document.querySelectorAll('.nav__link');

  function toggleMobileMenu() {
    const isExpanded = mobileMenuToggle.getAttribute('aria-expanded') === 'true';
    mobileMenuToggle.setAttribute('aria-expanded', !isExpanded);
    mobileMenuToggle.classList.toggle('active');
    navList.classList.toggle('active');
  }

  function closeMobileMenu() {
    mobileMenuToggle.setAttribute('aria-expanded', 'false');
    mobileMenuToggle.classList.remove('active');
    navList.classList.remove('active');
  }

  if (mobileMenuToggle) {
    mobileMenuToggle.addEventListener('click', toggleMobileMenu);
  }

  // Smooth Scroll Navigation
  navLinks.forEach(link => {
    link.addEventListener('click', function(e) {
      const href = this.getAttribute('href');

      if (href.startsWith('#')) {
        e.preventDefault();
        const targetId = href.substring(1);
        const targetElement = document.getElementById(targetId);

        if (targetElement) {
          const headerHeight = document.querySelector('.header').offsetHeight;
          const targetPosition = targetElement.offsetTop - headerHeight;

          window.scrollTo({
            top: targetPosition,
            behavior: 'smooth'
          });

          // Update active state
          navLinks.forEach(l => l.classList.remove('active'));
          this.classList.add('active');

          // Close mobile menu after clicking
          closeMobileMenu();
        }
      }
    });
  });

  // Active Section Highlighting on Scroll
  const sections = document.querySelectorAll('section[id]');

  function highlightActiveSection() {
    const scrollPosition = window.scrollY;
    const headerHeight = document.querySelector('.header').offsetHeight;

    sections.forEach(section => {
      const sectionTop = section.offsetTop - headerHeight - 100;
      const sectionHeight = section.offsetHeight;
      const sectionId = section.getAttribute('id');

      if (scrollPosition >= sectionTop && scrollPosition < sectionTop + sectionHeight) {
        navLinks.forEach(link => {
          link.classList.remove('active');
          if (link.getAttribute('href') === `#${sectionId}`) {
            link.classList.add('active');
          }
        });
      }
    });
  }

  window.addEventListener('scroll', highlightActiveSection, { passive: true });

  // Initial highlight check
  highlightActiveSection();
})();
