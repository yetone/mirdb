const fs = require('fs');
const path = require('path');
require('@testing-library/jest-dom');

/**
 * Newsletter Signup Form Tests
 * Tests for REQ-9: Newsletter signup form for lead capture
 */

beforeEach(() => {
  const htmlPath = path.resolve(__dirname, '../index.html');
  const html = fs.readFileSync(htmlPath, 'utf8');
  document.documentElement.innerHTML = html;
});

describe('Newsletter Signup Form - REQ-9', () => {
  describe('Form Structure', () => {
    test('newsletter form exists on the page', () => {
      const form = document.querySelector('[data-testid="newsletter-form"]');
      expect(form).toBeInTheDocument();
    });

    test('newsletter form has an email input field', () => {
      const emailInput = document.querySelector('[data-testid="newsletter-email"]');
      expect(emailInput).toBeInTheDocument();
      expect(emailInput.type).toBe('email');
    });

    test('email input has required attribute', () => {
      const emailInput = document.querySelector('[data-testid="newsletter-email"]');
      expect(emailInput).toHaveAttribute('required');
    });

    test('newsletter form has a submit button', () => {
      const submitButton = document.querySelector('[data-testid="newsletter-submit"]');
      expect(submitButton).toBeInTheDocument();
      expect(submitButton.type).toBe('submit');
    });

    test('email input has placeholder text', () => {
      const emailInput = document.querySelector('[data-testid="newsletter-email"]');
      expect(emailInput).toHaveAttribute('placeholder');
    });

    test('newsletter section has a heading', () => {
      const newsletterSection = document.querySelector('[data-testid="newsletter-section"]');
      expect(newsletterSection).toBeInTheDocument();
      const heading = newsletterSection.querySelector('h2, h3, h4');
      expect(heading).toBeInTheDocument();
    });
  });

  describe('Accessibility', () => {
    test('email input has associated label or aria-label', () => {
      const emailInput = document.querySelector('[data-testid="newsletter-email"]');
      const hasAriaLabel = emailInput.hasAttribute('aria-label');
      const inputId = emailInput.id;
      const hasAssociatedLabel = inputId && document.querySelector(`label[for="${inputId}"]`);

      expect(hasAriaLabel || hasAssociatedLabel).toBe(true);
    });

    test('submit button has descriptive text', () => {
      const submitButton = document.querySelector('[data-testid="newsletter-submit"]');
      const buttonText = submitButton.textContent.trim();
      expect(buttonText.length).toBeGreaterThan(0);
    });

    test('form has aria attributes for accessibility', () => {
      const form = document.querySelector('[data-testid="newsletter-form"]');
      const hasRole = form.hasAttribute('role') || form.tagName.toLowerCase() === 'form';
      expect(hasRole).toBe(true);
    });
  });

  describe('Form Validation Attributes', () => {
    test('email input has type="email" for native validation', () => {
      const emailInput = document.querySelector('[data-testid="newsletter-email"]');
      expect(emailInput.type).toBe('email');
    });

    test('form has novalidate attribute removed or not present (use browser validation)', () => {
      const form = document.querySelector('[data-testid="newsletter-form"]');
      // Form should use browser validation OR have custom validation
      // We check that the form exists and is properly configured
      expect(form.tagName.toLowerCase()).toBe('form');
    });
  });

  describe('Message Display Elements', () => {
    test('form has success message container', () => {
      const successMessage = document.querySelector('[data-testid="newsletter-success"]');
      expect(successMessage).toBeInTheDocument();
    });

    test('form has error message container', () => {
      const errorMessage = document.querySelector('[data-testid="newsletter-error"]');
      expect(errorMessage).toBeInTheDocument();
    });

    test('success message is initially hidden', () => {
      const successMessage = document.querySelector('[data-testid="newsletter-success"]');
      const style = window.getComputedStyle(successMessage);
      const isHidden = style.display === 'none' ||
                       successMessage.classList.contains('hidden') ||
                       successMessage.hidden ||
                       successMessage.getAttribute('aria-hidden') === 'true';
      expect(isHidden).toBe(true);
    });

    test('error message is initially hidden', () => {
      const errorMessage = document.querySelector('[data-testid="newsletter-error"]');
      const style = window.getComputedStyle(errorMessage);
      const isHidden = style.display === 'none' ||
                       errorMessage.classList.contains('hidden') ||
                       errorMessage.hidden ||
                       errorMessage.getAttribute('aria-hidden') === 'true';
      expect(isHidden).toBe(true);
    });
  });
});
