/**
 * MirDB Landing Page - Main JavaScript
 */

document.addEventListener('DOMContentLoaded', () => {
  // Mobile menu toggle
  const mobileMenuToggle = document.querySelector('.mobile-menu-toggle');
  const navLinks = document.querySelector('.nav-links');

  if (mobileMenuToggle && navLinks) {
    mobileMenuToggle.addEventListener('click', () => {
      navLinks.classList.toggle('active');
      mobileMenuToggle.classList.toggle('active');
    });
  }

  // Smooth scroll for anchor links
  document.querySelectorAll('a[href^="#"]').forEach(anchor => {
    anchor.addEventListener('click', function (e) {
      const href = this.getAttribute('href');
      if (href === '#') return;

      e.preventDefault();
      const target = document.querySelector(href);
      if (target) {
        target.scrollIntoView({
          behavior: 'smooth',
          block: 'start'
        });
      }
    });
  });

  // Header scroll effect
  const header = document.querySelector('.header');
  let lastScroll = 0;

  window.addEventListener('scroll', () => {
    const currentScroll = window.pageYOffset;

    if (currentScroll > 100) {
      header.classList.add('scrolled');
    } else {
      header.classList.remove('scrolled');
    }

    lastScroll = currentScroll;
  });

  // Analytics: Track CTA button clicks
  document.querySelectorAll('[data-analytics-event]').forEach(element => {
    element.addEventListener('click', function() {
      const eventName = this.getAttribute('data-analytics-event');
      const eventCategory = this.getAttribute('data-event-category') || 'cta';
      const eventLabel = this.getAttribute('data-analytics') || this.textContent.trim();

      // Track event using gtag if available
      if (typeof gtag === 'function') {
        gtag('event', eventName, {
          'event_category': eventCategory,
          'event_label': eventLabel
        });
      }

      // Also push to dataLayer for GTM compatibility
      if (window.dataLayer) {
        window.dataLayer.push({
          'event': eventName,
          'eventCategory': eventCategory,
          'eventLabel': eventLabel
        });
      }
    });
  });
});
