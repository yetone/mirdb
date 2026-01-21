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

  // Newsletter form handling
  const newsletterForm = document.getElementById('newsletter-form');
  const newsletterEmail = document.getElementById('newsletter-email');
  const newsletterSuccess = document.querySelector('[data-testid="newsletter-success"]');
  const newsletterError = document.querySelector('[data-testid="newsletter-error"]');

  if (newsletterForm) {
    newsletterForm.addEventListener('submit', function(e) {
      e.preventDefault();

      // Reset messages
      newsletterSuccess.classList.add('hidden');
      newsletterError.classList.add('hidden');

      const email = newsletterEmail.value.trim();

      // Validate email
      if (!email) {
        newsletterError.textContent = 'Please enter your email address.';
        newsletterError.classList.remove('hidden');
        return;
      }

      // Simple email validation regex
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!emailRegex.test(email)) {
        newsletterError.textContent = 'Please enter a valid email address.';
        newsletterError.classList.remove('hidden');
        return;
      }

      // Simulate successful subscription (in production, this would be an API call)
      newsletterSuccess.textContent = 'Thank you for subscribing! We\'ll keep you updated.';
      newsletterSuccess.classList.remove('hidden');
      newsletterEmail.value = '';

      // Track newsletter signup event
      if (typeof gtag === 'function') {
        gtag('event', 'newsletter_signup', {
          'event_category': 'newsletter',
          'event_label': 'subscribe_success'
        });
      }

      if (window.dataLayer) {
        window.dataLayer.push({
          'event': 'newsletter_signup',
          'eventCategory': 'newsletter',
          'eventLabel': 'subscribe_success'
        });
      }
    });
  }
});
