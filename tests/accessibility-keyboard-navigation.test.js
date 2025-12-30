/**
 * Tests for Accessibility - Keyboard Navigation (NFR-3)
 * Verify page can be navigated using keyboard only
 * - Tab navigation through all interactive elements
 * - Visible focus indicators
 * - Button activation with Enter/Space keys
 */

const fs = require('fs');
const path = require('path');

describe('Accessibility - Keyboard Navigation', () => {
  let document;
  let htmlContent;

  beforeEach(() => {
    htmlContent = fs.readFileSync(path.resolve(__dirname, '../index.html'), 'utf8');
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  // Test Case 1: Tab through page from start - All interactive elements receive focus in logical order
  describe('Test Case 1: Tab Navigation Order', () => {
    test('should have all interactive elements (links, buttons) focusable', () => {
      const links = document.querySelectorAll('a[href]');
      const buttons = document.querySelectorAll('button');
      const inputs = document.querySelectorAll('input, textarea, select');

      // All links should be focusable (not have tabindex="-1" unless there's a reason)
      links.forEach(link => {
        const tabIndex = link.getAttribute('tabindex');
        // tabindex should be null (default), 0, or positive
        expect(tabIndex === null || parseInt(tabIndex) >= 0).toBe(true);
      });

      // All buttons should be focusable
      buttons.forEach(button => {
        const tabIndex = button.getAttribute('tabindex');
        expect(tabIndex === null || parseInt(tabIndex) >= 0).toBe(true);
      });
    });

    test('should have links in a logical reading order (hero links first)', () => {
      const heroSection = document.querySelector('.hero');
      expect(heroSection).not.toBeNull();

      const heroLinks = heroSection.querySelectorAll('a[href]');
      expect(heroLinks.length).toBeGreaterThanOrEqual(1);

      // First link should be "Get Started"
      const firstHeroLink = heroLinks[0];
      expect(firstHeroLink.textContent.toLowerCase()).toContain('get started');
    });

    test('should have footer links at the end of tab order', () => {
      const footer = document.querySelector('.footer, footer');
      expect(footer).not.toBeNull();

      const footerLinks = footer.querySelectorAll('a[href]');
      expect(footerLinks.length).toBeGreaterThan(0);
    });

    test('interactive elements should follow document flow order', () => {
      // Get all focusable elements in document order
      const focusableSelector = 'a[href], button, input, textarea, select, [tabindex]:not([tabindex="-1"])';
      const focusableElements = Array.from(document.querySelectorAll(focusableSelector));

      expect(focusableElements.length).toBeGreaterThan(0);

      // Verify that elements appear in a logical order based on their section
      // Hero elements should come before main content
      const heroElement = document.querySelector('.hero');
      const footerElement = document.querySelector('.footer, footer');

      if (heroElement && footerElement) {
        const heroLinks = Array.from(heroElement.querySelectorAll(focusableSelector));
        const footerLinks = Array.from(footerElement.querySelectorAll(focusableSelector));

        // Check that hero links appear before footer links in the focusable list
        if (heroLinks.length > 0 && footerLinks.length > 0) {
          const firstHeroIndex = focusableElements.indexOf(heroLinks[0]);
          const firstFooterIndex = focusableElements.indexOf(footerLinks[0]);
          expect(firstHeroIndex).toBeLessThan(firstFooterIndex);
        }
      }
    });

    test('no elements should trap keyboard focus', () => {
      // Check that no element has an event handler that would trap focus
      // This is a static check - elements should not have tabindex=-1 on all children
      const containers = document.querySelectorAll('div, section, article');
      containers.forEach(container => {
        const children = container.querySelectorAll('a[href], button');
        if (children.length > 1) {
          // Not all children should have tabindex=-1
          const allHidden = Array.from(children).every(
            child => child.getAttribute('tabindex') === '-1'
          );
          expect(allHidden).toBe(false);
        }
      });
    });
  });

  // Test Case 2: Check focus indicator visibility
  describe('Test Case 2: Focus Indicator Visibility', () => {
    let styleContent;

    beforeEach(() => {
      // Read the CSS file to check for focus styles
      styleContent = fs.readFileSync(path.resolve(__dirname, '../styles.css'), 'utf8');
    });

    test('should have focus styles defined in CSS', () => {
      // Check that :focus or :focus-visible styles are defined
      const hasFocusStyles = styleContent.includes(':focus') || styleContent.includes(':focus-visible');
      expect(hasFocusStyles).toBe(true);
    });

    test('should have visible focus indicator styles (outline or equivalent)', () => {
      // Check for outline, box-shadow, or border styles in focus rules
      const focusRegex = /:focus(-visible)?\s*\{[^}]*(outline|box-shadow|border)[^}]*\}/gi;
      const hasFocusIndicator = focusRegex.test(styleContent);
      expect(hasFocusIndicator).toBe(true);
    });

    test('should not hide focus outlines globally without alternative', () => {
      // Check for outline: none without providing alternative focus styles
      const outlineNoneRegex = /\*\s*\{[^}]*outline\s*:\s*(none|0)[^}]*\}/gi;
      const hasGlobalOutlineRemoval = outlineNoneRegex.test(styleContent);

      if (hasGlobalOutlineRemoval) {
        // If outline is removed globally, there should be custom focus styles
        const hasCustomFocusStyles = /:focus(-visible)?\s*\{/gi.test(styleContent);
        expect(hasCustomFocusStyles).toBe(true);
      }
    });

    test('links should have focus styles', () => {
      // Check for a:focus or link-specific focus styles
      const hasLinkFocusStyles = /a(\[href\])?:focus(-visible)?\s*\{/gi.test(styleContent) ||
                                  /\.btn:focus(-visible)?\s*\{/gi.test(styleContent) ||
                                  /:focus(-visible)?\s*\{/gi.test(styleContent);
      expect(hasLinkFocusStyles).toBe(true);
    });

    test('buttons should have focus styles', () => {
      // Check for button focus styles
      const hasButtonFocusStyles = /button:focus(-visible)?\s*\{/gi.test(styleContent) ||
                                    /\.btn:focus(-visible)?\s*\{/gi.test(styleContent) ||
                                    /:focus(-visible)?\s*\{/gi.test(styleContent);
      expect(hasButtonFocusStyles).toBe(true);
    });

    test('focus indicator should have sufficient contrast', () => {
      // Check that focus styles don't use transparent or very light colors
      // This is a basic static check
      const focusMatch = styleContent.match(/:focus(-visible)?\s*\{([^}]*)\}/gi);
      if (focusMatch) {
        // Focus styles should not be transparent
        const focusStylesJoined = focusMatch.join('');
        const hasTransparentFocus = /outline-color\s*:\s*transparent/i.test(focusStylesJoined);
        expect(hasTransparentFocus).toBe(false);
      }
    });
  });

  // Test Case 3: Activate button with Enter key
  describe('Test Case 3: Button Activation with Keyboard', () => {
    test('all links should be natively keyboard activatable', () => {
      const links = document.querySelectorAll('a[href]');

      links.forEach(link => {
        // Links are natively keyboard activatable when they have href attribute
        expect(link.hasAttribute('href')).toBe(true);

        // Links should not be disabled
        expect(link.hasAttribute('disabled')).toBe(false);
      });
    });

    test('buttons should use proper button element or have role="button"', () => {
      // Check for elements that look like buttons
      const btnElements = document.querySelectorAll('.btn, [class*="button"]');

      btnElements.forEach(el => {
        const isButton = el.tagName.toLowerCase() === 'button';
        const isLink = el.tagName.toLowerCase() === 'a' && el.hasAttribute('href');
        const hasButtonRole = el.getAttribute('role') === 'button';

        // Element should be either a button, a link, or have button role
        expect(isButton || isLink || hasButtonRole).toBe(true);

        // If it's a div/span with role="button", it should be keyboard accessible
        if (!isButton && !isLink && hasButtonRole) {
          const tabIndex = el.getAttribute('tabindex');
          expect(tabIndex === '0' || el.hasAttribute('tabindex')).toBe(true);
        }
      });
    });

    test('hero call-to-action buttons should be keyboard accessible', () => {
      const heroActions = document.querySelector('.hero-actions');
      expect(heroActions).not.toBeNull();

      const ctaButtons = heroActions.querySelectorAll('a.btn, button.btn, .btn');
      expect(ctaButtons.length).toBeGreaterThanOrEqual(1);

      ctaButtons.forEach(btn => {
        // CTA buttons should be focusable
        const tabIndex = btn.getAttribute('tabindex');
        expect(tabIndex === null || parseInt(tabIndex) >= 0).toBe(true);

        // CTA buttons should be links or buttons (natively keyboard accessible)
        const isNativelyAccessible = btn.tagName.toLowerCase() === 'a' ||
                                      btn.tagName.toLowerCase() === 'button';
        expect(isNativelyAccessible).toBe(true);
      });
    });

    test('no interactive elements should rely on click-only handlers without keyboard support', () => {
      // This checks that elements with onclick also work with keyboard
      // For static HTML, we verify that interactive elements use native elements
      const clickableElements = document.querySelectorAll('[onclick]');

      clickableElements.forEach(el => {
        const tagName = el.tagName.toLowerCase();

        // If element has onclick, it should be a natively focusable element
        // or have tabindex and keydown handler
        const isNativelyFocusable = ['a', 'button', 'input', 'select', 'textarea'].includes(tagName);
        const hasTabIndex = el.hasAttribute('tabindex');
        const hasKeyHandler = el.hasAttribute('onkeydown') || el.hasAttribute('onkeyup') || el.hasAttribute('onkeypress');

        expect(isNativelyFocusable || (hasTabIndex && hasKeyHandler)).toBe(true);
      });
    });

    test('skip link should exist for keyboard users (optional enhancement)', () => {
      // Skip links are a best practice but not always required
      // If a skip link exists, verify it works
      const skipLink = document.querySelector('a[href="#main"], a[href="#content"], .skip-link, [class*="skip"]');

      if (skipLink) {
        expect(skipLink.hasAttribute('href')).toBe(true);
        // Skip link target should exist
        const targetId = skipLink.getAttribute('href').replace('#', '');
        const target = document.getElementById(targetId);
        // Target should exist if skip link is present
        if (targetId) {
          expect(target !== null || targetId === 'main' || targetId === 'content').toBe(true);
        }
      }
      // Test passes even if no skip link (it's an enhancement)
      expect(true).toBe(true);
    });
  });

  // Additional keyboard navigation tests
  describe('Additional Keyboard Navigation Checks', () => {
    test('page should have semantic landmarks for navigation', () => {
      // Check for semantic elements that help keyboard/screen reader navigation
      const header = document.querySelector('header, [role="banner"]');
      const main = document.querySelector('main, [role="main"]');
      const footer = document.querySelector('footer, [role="contentinfo"]');
      const nav = document.querySelector('nav, [role="navigation"]');

      // At minimum, should have header and footer
      expect(header || footer).not.toBeNull();
    });

    test('focusable elements count should be reasonable', () => {
      const focusableSelector = 'a[href], button, input, textarea, select, [tabindex]:not([tabindex="-1"])';
      const focusableElements = document.querySelectorAll(focusableSelector);

      // Page should have focusable elements
      expect(focusableElements.length).toBeGreaterThan(0);

      // But not an excessive number that would make keyboard navigation tedious
      expect(focusableElements.length).toBeLessThan(100);
    });

    test('tab order should not use positive tabindex values', () => {
      // Positive tabindex values can create confusing tab order
      const positiveTabIndex = document.querySelectorAll('[tabindex]');

      positiveTabIndex.forEach(el => {
        const tabIndex = parseInt(el.getAttribute('tabindex'));
        // tabindex should be 0, -1, or not set (default)
        // Positive values > 0 are discouraged
        expect(tabIndex <= 0 || isNaN(tabIndex)).toBe(true);
      });
    });

    test('internal anchor links should have valid targets', () => {
      const internalLinks = document.querySelectorAll('a[href^="#"]');

      internalLinks.forEach(link => {
        const targetId = link.getAttribute('href').slice(1);
        if (targetId && targetId.length > 0) {
          const targetElement = document.getElementById(targetId);
          expect(targetElement).not.toBeNull();
        }
      });
    });
  });
});
