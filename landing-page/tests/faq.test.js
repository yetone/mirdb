import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

/**
 * FAQ Section Tests
 * Testing REQ-8: FAQ or common questions section
 */

describe('FAQ Section', () => {
  let dom;
  let document;

  beforeEach(() => {
    // Load the HTML file
    const htmlPath = path.resolve(__dirname, '../index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');
    dom = new JSDOM(htmlContent, {
      url: 'http://localhost:3000',
      runScripts: 'dangerously',
      resources: 'usable',
    });
    document = dom.window.document;
  });

  afterEach(() => {
    if (dom) {
      dom.window.close();
    }
  });

  /**
   * Test Case 1: Check FAQ section exists
   * Input: Check FAQ section exists
   * Expected: FAQ section with questions and answers is present
   */
  describe('Test Case 1: FAQ Section Exists', () => {
    it('should have a FAQ section element', () => {
      const faqSection = document.querySelector('.faq-section');
      expect(faqSection).not.toBeNull();
      expect(faqSection).toBeInTheDocument();
    });

    it('should have FAQ section with correct ID for navigation', () => {
      const faqSection = document.querySelector('#faq');
      expect(faqSection).not.toBeNull();
    });

    it('should have FAQ section as a semantic section element', () => {
      const faqSection = document.querySelector('section.faq-section');
      expect(faqSection).not.toBeNull();
      expect(faqSection.tagName.toLowerCase()).toBe('section');
    });

    it('should have a heading for the FAQ section', () => {
      const faqSection = document.querySelector('.faq-section');
      const heading = faqSection.querySelector('h2');

      expect(heading).not.toBeNull();
      expect(heading.textContent.trim().length).toBeGreaterThan(0);
      // Heading should contain FAQ-related text
      const headingText = heading.textContent.toLowerCase();
      expect(headingText.includes('faq') || headingText.includes('frequently asked') || headingText.includes('question')).toBe(true);
    });

    it('should have FAQ items container', () => {
      const faqContainer = document.querySelector('.faq-container');
      expect(faqContainer).not.toBeNull();
    });

    it('should have FAQ section positioned appropriately in page layout', () => {
      const main = document.querySelector('main');
      const sections = main.querySelectorAll('section');
      const sectionsArray = Array.from(sections);

      const faqIndex = sectionsArray.findIndex(s => s.classList.contains('faq-section'));
      expect(faqIndex).toBeGreaterThan(-1);
    });
  });

  /**
   * Test Case 2: Count FAQ items
   * Input: Count FAQ items
   * Expected: At least 3 FAQ items are displayed
   */
  describe('Test Case 2: FAQ Item Count', () => {
    it('should have at least 3 FAQ items', () => {
      const faqItems = document.querySelectorAll('.faq-item');
      expect(faqItems.length).toBeGreaterThanOrEqual(3);
    });

    it('should have FAQ items contained within FAQ container', () => {
      const faqContainer = document.querySelector('.faq-container');
      expect(faqContainer).not.toBeNull();

      const faqItems = faqContainer.querySelectorAll('.faq-item');
      expect(faqItems.length).toBeGreaterThanOrEqual(3);
    });

    it('should have each FAQ item contain a question and answer', () => {
      const faqItems = document.querySelectorAll('.faq-item');

      faqItems.forEach(item => {
        const question = item.querySelector('.faq-question');
        const answer = item.querySelector('.faq-answer');

        expect(question).not.toBeNull();
        expect(answer).not.toBeNull();
      });
    });
  });

  /**
   * FAQ Structure and Accessibility Tests
   */
  describe('FAQ Structure and Accessibility', () => {
    it('should have each FAQ question as a button for accessibility', () => {
      const faqQuestions = document.querySelectorAll('.faq-question');
      expect(faqQuestions.length).toBeGreaterThanOrEqual(3);

      faqQuestions.forEach(question => {
        expect(question.tagName.toLowerCase()).toBe('button');
      });
    });

    it('should have each FAQ question with aria-expanded attribute', () => {
      const faqQuestions = document.querySelectorAll('.faq-question');

      faqQuestions.forEach(question => {
        const ariaExpanded = question.getAttribute('aria-expanded');
        expect(ariaExpanded).not.toBeNull();
        expect(['true', 'false']).toContain(ariaExpanded);
      });
    });

    it('should have each FAQ question with aria-controls attribute', () => {
      const faqQuestions = document.querySelectorAll('.faq-question');

      faqQuestions.forEach(question => {
        const ariaControls = question.getAttribute('aria-controls');
        expect(ariaControls).not.toBeNull();
        expect(ariaControls.trim().length).toBeGreaterThan(0);
      });
    });

    it('should have each FAQ answer with corresponding ID', () => {
      const faqQuestions = document.querySelectorAll('.faq-question');

      faqQuestions.forEach(question => {
        const ariaControls = question.getAttribute('aria-controls');
        const answer = document.getElementById(ariaControls);
        expect(answer).not.toBeNull();
      });
    });

    it('should have meaningful question text', () => {
      const faqQuestions = document.querySelectorAll('.faq-question');

      faqQuestions.forEach(question => {
        const text = question.textContent.trim();
        expect(text.length).toBeGreaterThan(10);
      });
    });

    it('should have meaningful answer text', () => {
      const faqAnswers = document.querySelectorAll('.faq-answer');

      faqAnswers.forEach(answer => {
        const text = answer.textContent.trim();
        expect(text.length).toBeGreaterThan(20);
      });
    });

    it('should have FAQ answers initially hidden', () => {
      const faqAnswers = document.querySelectorAll('.faq-answer');

      faqAnswers.forEach(answer => {
        expect(answer.hidden).toBe(true);
      });
    });

    it('should have FAQ questions initially collapsed (aria-expanded="false")', () => {
      const faqQuestions = document.querySelectorAll('.faq-question');

      faqQuestions.forEach(question => {
        expect(question.getAttribute('aria-expanded')).toBe('false');
      });
    });
  });

  /**
   * CSS styling tests for FAQ section
   */
  describe('FAQ Section Styling', () => {
    it('should have CSS styles defined for FAQ section', () => {
      const cssPath = path.resolve(__dirname, '../styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf-8');

      expect(cssContent).toContain('.faq-section');
      expect(cssContent).toContain('.faq-item');
      expect(cssContent).toContain('.faq-question');
      expect(cssContent).toContain('.faq-answer');
    });

    it('should have flex or grid layout for FAQ container', () => {
      const cssPath = path.resolve(__dirname, '../styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf-8');

      expect(cssContent).toMatch(/\.faq-container\s*\{[^}]*(display:\s*(flex|grid))/);
    });

    it('should have focus styles for FAQ questions', () => {
      const cssPath = path.resolve(__dirname, '../styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf-8');

      expect(cssContent).toContain('.faq-question:focus');
    });
  });
});
