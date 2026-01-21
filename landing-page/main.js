/**
 * MirDB Landing Page - Main JavaScript
 */

/**
 * Handle primary CTA button click
 */
function handleGetStarted() {
  // Scroll to get-started section or redirect to signup
  const getStartedSection = document.getElementById('get-started');
  if (getStartedSection) {
    getStartedSection.scrollIntoView({ behavior: 'smooth' });
  } else {
    // If no get-started section exists, could redirect to docs/signup
    window.location.href = 'https://github.com/yetone/mirdb#usage';
  }
}

// Make function available globally for onclick handler
window.handleGetStarted = handleGetStarted;

/**
 * Initialize FAQ accordion functionality
 */
function initFaqAccordion() {
  const faqQuestions = document.querySelectorAll('.faq-question');

  faqQuestions.forEach(question => {
    question.addEventListener('click', function() {
      const answer = this.nextElementSibling;
      const isExpanded = this.getAttribute('aria-expanded') === 'true';

      // Toggle current item
      this.setAttribute('aria-expanded', !isExpanded);
      answer.hidden = isExpanded;
    });
  });
}

// Make function available globally for testing
window.initFaqAccordion = initFaqAccordion;

/**
 * Initialize the landing page
 */
function initLandingPage() {
  // Add smooth scroll behavior for anchor links
  document.querySelectorAll('a[href^="#"]').forEach(anchor => {
    anchor.addEventListener('click', function(e) {
      const targetId = this.getAttribute('href');
      if (targetId === '#') return;

      const target = document.querySelector(targetId);
      if (target) {
        e.preventDefault();
        target.scrollIntoView({ behavior: 'smooth' });
      }
    });
  });

  // Add sticky header shadow on scroll
  const header = document.querySelector('.header');
  if (header) {
    window.addEventListener('scroll', () => {
      if (window.scrollY > 10) {
        header.style.boxShadow = '0 2px 8px rgba(0, 0, 0, 0.1)';
      } else {
        header.style.boxShadow = 'none';
      }
    });
  }

  // Initialize FAQ accordion
  initFaqAccordion();
}

// Initialize when DOM is ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initLandingPage);
} else {
  initLandingPage();
}
