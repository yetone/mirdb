/**
 * FAQ Accordion Unit Tests
 * Owner: Scenario 7 - FAQ Accordion
 *
 * Test cases:
 * - FAQ section exists with proper structure
 * - At least 5 FAQ items are present
 * - Each FAQ item has question text
 * - Each FAQ item has answer content
 * - FAQ item expands on click
 * - FAQ item collapses on second click
 * - ARIA attributes toggle correctly
 * - Keyboard navigation works
 */

const fs = require('fs');
const path = require('path');

// Read HTML file
const htmlPath = path.join(__dirname, '../../index.html');
const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

// Read FAQ JavaScript
const faqJsPath = path.join(__dirname, '../../js/faq.js');
const faqJsContent = fs.readFileSync(faqJsPath, 'utf-8');

describe('FAQ Accordion', () => {
  let container;

  beforeEach(() => {
    // Reset DOM for each test
    document.body.innerHTML = '';
    container = document.createElement('div');
    container.innerHTML = htmlContent;
    document.body.appendChild(container);

    // Execute the FAQ script
    eval(faqJsContent);

    // Initialize FAQ if needed
    if (typeof window.initFAQ === 'function') {
      window.initFAQ();
    }
  });

  afterEach(() => {
    document.body.innerHTML = '';
    // Clean up global functions
    delete window.initFAQ;
    delete window.toggleFAQItem;
    delete window.closeAllFAQItems;
  });

  // Test Case 1: FAQ section element exists
  describe('Test Case 1: FAQ Section Exists', () => {
    test('should have FAQ section element with id "faq"', () => {
      const faqSection = document.getElementById('faq');
      expect(faqSection).not.toBeNull();
      expect(faqSection.tagName.toLowerCase()).toBe('section');
    });

    test('should have FAQ section with appropriate class', () => {
      const faqSection = document.getElementById('faq');
      expect(faqSection).not.toBeNull();
      expect(faqSection.classList.contains('faq')).toBe(true);
    });
  });

  // Test Case 2: At least 5 FAQ items present
  describe('Test Case 2: FAQ Item Count', () => {
    test('should have at least 5 FAQ items', () => {
      const faqItems = document.querySelectorAll('.faq-item');
      expect(faqItems.length).toBeGreaterThanOrEqual(5);
    });
  });

  // Test Case 3: Each FAQ item has question text
  describe('Test Case 3: FAQ Questions', () => {
    test('should have question text in each FAQ item', () => {
      const faqItems = document.querySelectorAll('.faq-item');

      faqItems.forEach((item, index) => {
        const questionBtn = item.querySelector('.faq-question');
        expect(questionBtn).not.toBeNull();
        expect(questionBtn.textContent.trim().length).toBeGreaterThan(0);
      });
    });

    test('should have questions as buttons for accessibility', () => {
      const faqQuestions = document.querySelectorAll('.faq-question');

      faqQuestions.forEach((question) => {
        expect(question.tagName.toLowerCase()).toBe('button');
      });
    });
  });

  // Test Case 4: Each FAQ item has answer content
  describe('Test Case 4: FAQ Answers', () => {
    test('should have corresponding answer content for each FAQ item', () => {
      const faqItems = document.querySelectorAll('.faq-item');

      faqItems.forEach((item, index) => {
        const answer = item.querySelector('.faq-answer');
        expect(answer).not.toBeNull();
        expect(answer.textContent.trim().length).toBeGreaterThan(0);
      });
    });
  });

  // Test Case 5: Answer expands on click
  describe('Test Case 5: FAQ Expand on Click', () => {
    test('should expand answer when question is clicked', () => {
      const firstQuestion = document.querySelector('.faq-question');
      const firstAnswer = document.querySelector('.faq-answer');

      // Initially answer should be hidden
      expect(firstAnswer.classList.contains('faq-answer--open')).toBe(false);

      // Click to expand
      firstQuestion.click();

      // After click, answer should be visible
      expect(firstAnswer.classList.contains('faq-answer--open')).toBe(true);
    });

    test('should change aria-expanded to true when expanded', () => {
      const firstQuestion = document.querySelector('.faq-question');

      // Initially should be false
      expect(firstQuestion.getAttribute('aria-expanded')).toBe('false');

      // Click to expand
      firstQuestion.click();

      // After click should be true
      expect(firstQuestion.getAttribute('aria-expanded')).toBe('true');
    });
  });

  // Test Case 6: Answer collapses on second click
  describe('Test Case 6: FAQ Collapse on Click', () => {
    test('should collapse answer when same question is clicked twice', () => {
      const firstQuestion = document.querySelector('.faq-question');
      const firstAnswer = document.querySelector('.faq-answer');

      // First click - expand
      firstQuestion.click();
      expect(firstAnswer.classList.contains('faq-answer--open')).toBe(true);

      // Second click - collapse
      firstQuestion.click();
      expect(firstAnswer.classList.contains('faq-answer--open')).toBe(false);
    });

    test('should change aria-expanded back to false when collapsed', () => {
      const firstQuestion = document.querySelector('.faq-question');

      // First click - expand
      firstQuestion.click();
      expect(firstQuestion.getAttribute('aria-expanded')).toBe('true');

      // Second click - collapse
      firstQuestion.click();
      expect(firstQuestion.getAttribute('aria-expanded')).toBe('false');
    });
  });

  // Test Case 7: ARIA attributes
  describe('Test Case 7: ARIA Attributes', () => {
    test('should have aria-expanded attribute on all FAQ questions', () => {
      const faqQuestions = document.querySelectorAll('.faq-question');

      faqQuestions.forEach((question) => {
        expect(question.hasAttribute('aria-expanded')).toBe(true);
        expect(['true', 'false']).toContain(question.getAttribute('aria-expanded'));
      });
    });

    test('should have aria-controls attribute pointing to the answer', () => {
      const faqQuestions = document.querySelectorAll('.faq-question');

      faqQuestions.forEach((question) => {
        const ariaControls = question.getAttribute('aria-controls');
        expect(ariaControls).not.toBeNull();

        // The controlled element should exist
        const controlledElement = document.getElementById(ariaControls);
        expect(controlledElement).not.toBeNull();
      });
    });

    test('should toggle aria-expanded between true and false on click', () => {
      const questions = document.querySelectorAll('.faq-question');

      questions.forEach((question) => {
        const initialState = question.getAttribute('aria-expanded');
        question.click();
        const afterFirstClick = question.getAttribute('aria-expanded');
        question.click();
        const afterSecondClick = question.getAttribute('aria-expanded');

        // Should toggle
        expect(afterFirstClick).not.toBe(initialState);
        expect(afterSecondClick).toBe(initialState);
      });
    });
  });

  // Test Case 8: Keyboard navigation - Enter key
  describe('Test Case 8: Keyboard Navigation - Enter', () => {
    test('should toggle FAQ answer when Enter is pressed on focused question', () => {
      const firstQuestion = document.querySelector('.faq-question');
      const firstAnswer = document.querySelector('.faq-answer');

      // Initially collapsed
      expect(firstAnswer.classList.contains('faq-answer--open')).toBe(false);

      // Simulate Enter key press
      const enterEvent = new KeyboardEvent('keydown', {
        key: 'Enter',
        code: 'Enter',
        bubbles: true
      });
      firstQuestion.dispatchEvent(enterEvent);

      // Should be expanded
      expect(firstAnswer.classList.contains('faq-answer--open')).toBe(true);

      // Press Enter again
      firstQuestion.dispatchEvent(new KeyboardEvent('keydown', {
        key: 'Enter',
        code: 'Enter',
        bubbles: true
      }));

      // Should be collapsed again
      expect(firstAnswer.classList.contains('faq-answer--open')).toBe(false);
    });

    test('should toggle FAQ answer when Space is pressed on focused question', () => {
      const firstQuestion = document.querySelector('.faq-question');
      const firstAnswer = document.querySelector('.faq-answer');

      // Initially collapsed
      expect(firstAnswer.classList.contains('faq-answer--open')).toBe(false);

      // Simulate Space key press
      const spaceEvent = new KeyboardEvent('keydown', {
        key: ' ',
        code: 'Space',
        bubbles: true
      });
      firstQuestion.dispatchEvent(spaceEvent);

      // Should be expanded
      expect(firstAnswer.classList.contains('faq-answer--open')).toBe(true);
    });
  });

  // Test Case 9: Tab navigation - focusability
  describe('Test Case 9: Tab Navigation - Focusability', () => {
    test('should have all FAQ questions focusable', () => {
      const faqQuestions = document.querySelectorAll('.faq-question');

      faqQuestions.forEach((question) => {
        // Buttons are naturally focusable
        expect(question.tagName.toLowerCase()).toBe('button');
        // Should not have tabindex=-1 which would make it unfocusable
        const tabIndex = question.getAttribute('tabindex');
        expect(tabIndex === null || parseInt(tabIndex) >= 0).toBe(true);
      });
    });

    test('should have FAQ questions in logical order in the DOM', () => {
      const faqItems = document.querySelectorAll('.faq-item');

      // Each FAQ item should have a question that comes before its answer
      faqItems.forEach((item) => {
        const question = item.querySelector('.faq-question');
        const answer = item.querySelector('.faq-answer');

        expect(question).not.toBeNull();
        expect(answer).not.toBeNull();

        // Get parent of question (the h3 element)
        const questionHeading = question.closest('.faq-item-heading');

        // Question heading should exist and come before answer
        expect(questionHeading).not.toBeNull();

        // Get all direct children of the faq-item
        const children = Array.from(item.children);
        const headingIndex = children.indexOf(questionHeading);
        const answerIndex = children.indexOf(answer);

        expect(headingIndex).toBeLessThan(answerIndex);
      });
    });
  });
});
