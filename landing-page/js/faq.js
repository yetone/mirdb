/**
 * FAQ Accordion Module
 * Owner: Scenario 7 - FAQ Accordion
 *
 * Expected exports:
 * - initFAQ(): Initialize FAQ accordion behavior
 * - toggleFAQItem(element): Toggle individual FAQ item
 * - closeAllFAQItems(): Close all FAQ items
 *
 * Features:
 * - Click to expand/collapse answers
 * - Keyboard accessibility (Enter/Space to toggle)
 * - ARIA attributes for screen readers
 */

(function() {
  'use strict';

  /**
   * Toggle a single FAQ item open/closed
   * @param {HTMLElement} questionButton - The FAQ question button element
   */
  function toggleFAQItem(questionButton) {
    if (!questionButton) return;

    const isExpanded = questionButton.getAttribute('aria-expanded') === 'true';
    const answerId = questionButton.getAttribute('aria-controls');
    const answerElement = document.getElementById(answerId);

    if (!answerElement) return;

    // Toggle aria-expanded
    questionButton.setAttribute('aria-expanded', !isExpanded);

    // Toggle answer visibility
    if (isExpanded) {
      answerElement.classList.remove('faq-answer--open');
    } else {
      answerElement.classList.add('faq-answer--open');
    }
  }

  /**
   * Close all FAQ items
   */
  function closeAllFAQItems() {
    const allQuestions = document.querySelectorAll('.faq-question');

    allQuestions.forEach(function(question) {
      question.setAttribute('aria-expanded', 'false');
      const answerId = question.getAttribute('aria-controls');
      const answerElement = document.getElementById(answerId);
      if (answerElement) {
        answerElement.classList.remove('faq-answer--open');
      }
    });
  }

  /**
   * Handle click event on FAQ question
   * @param {Event} event - The click event
   */
  function handleQuestionClick(event) {
    const questionButton = event.currentTarget;
    toggleFAQItem(questionButton);
  }

  /**
   * Handle keyboard events on FAQ question
   * @param {KeyboardEvent} event - The keyboard event
   */
  function handleQuestionKeydown(event) {
    // Handle Enter and Space keys
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault();
      toggleFAQItem(event.currentTarget);
    }
  }

  /**
   * Initialize FAQ accordion functionality
   */
  function initFAQ() {
    const faqQuestions = document.querySelectorAll('.faq-question');

    if (faqQuestions.length === 0) {
      return;
    }

    faqQuestions.forEach(function(question) {
      // Ensure initial state is collapsed
      if (!question.hasAttribute('aria-expanded')) {
        question.setAttribute('aria-expanded', 'false');
      }

      // Add click event listener
      question.addEventListener('click', handleQuestionClick);

      // Add keyboard event listener for accessibility
      question.addEventListener('keydown', handleQuestionKeydown);
    });
  }

  // Expose functions globally for use by other scripts and tests
  window.initFAQ = initFAQ;
  window.toggleFAQItem = toggleFAQItem;
  window.closeAllFAQItems = closeAllFAQItems;

  // Initialize when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initFAQ);
  } else {
    initFAQ();
  }
})();
