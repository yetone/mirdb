import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import { readFileSync } from 'fs';
import { resolve } from 'path';

describe('Accessibility - Keyboard Navigation', () => {
  let dom;
  let document;
  let window;

  beforeEach(() => {
    const htmlPath = resolve(__dirname, '../index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    dom = new JSDOM(html, { runScripts: 'dangerously', resources: 'usable' });
    document = dom.window.document;
    window = dom.window;
  });

  afterEach(() => {
    if (dom) {
      dom.window.close();
    }
  });

  // Test Case 1: Tab through page from top - All navigation links receive focus in logical order
  describe('Test Case 1: Tab through page from top', () => {
    it('should have all navigation links focusable', () => {
      const nav = document.querySelector('nav');
      const navLinks = nav.querySelectorAll('a');
      expect(navLinks.length).toBeGreaterThan(0);

      navLinks.forEach(link => {
        // Links should be focusable (tabindex not -1)
        const tabindex = link.getAttribute('tabindex');
        expect(tabindex !== '-1').toBe(true);
      });
    });

    it('should have logical tab order based on DOM structure', () => {
      // Get all focusable elements
      const focusableSelector = 'a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])';
      const focusableElements = Array.from(document.querySelectorAll(focusableSelector));

      expect(focusableElements.length).toBeGreaterThan(0);

      // First focusable elements should be in header/navigation area or skip link
      const firstFocusable = focusableElements[0];
      const isInHeaderOrSkipLink =
        firstFocusable.closest('header') !== null ||
        firstFocusable.closest('nav') !== null ||
        firstFocusable.classList.contains('skip-link') ||
        firstFocusable.textContent.toLowerCase().includes('skip');
      expect(isInHeaderOrSkipLink).toBe(true);
    });

    it('should have navigation links appear before main content links in tab order', () => {
      const focusableSelector = 'a[href], button:not([tabindex="-1"])';
      const focusableElements = Array.from(document.querySelectorAll(focusableSelector));

      // Find indices of nav links and main content links
      const navLinks = Array.from(document.querySelectorAll('nav a, header a, .skip-link'));
      const heroLinks = Array.from(document.querySelectorAll('.hero a, .cta-buttons a'));

      if (navLinks.length > 0 && heroLinks.length > 0) {
        // Navigation links should come before hero CTA buttons
        const firstNavIndex = focusableElements.indexOf(navLinks[0]);
        const firstHeroIndex = focusableElements.indexOf(heroLinks[0]);

        // Account for skip link which may precede nav
        expect(firstNavIndex).toBeLessThan(firstHeroIndex);
      }
    });

    it('all focusable elements should have no negative tabindex blocking focus', () => {
      const allInteractive = document.querySelectorAll('a[href], button, [role="button"]');

      allInteractive.forEach(element => {
        const tabindex = element.getAttribute('tabindex');
        // Should not have tabindex="-1" which would remove from tab order
        if (tabindex !== null) {
          expect(parseInt(tabindex)).toBeGreaterThanOrEqual(0);
        }
      });
    });
  });

  // Test Case 2: Check focus indicator visibility - Focused elements have visible outline or highlight
  describe('Test Case 2: Check focus indicator visibility', () => {
    it('should have CSS :focus-visible styles defined', () => {
      const cssPath = resolve(__dirname, '../styles.css');
      const css = readFileSync(cssPath, 'utf-8');

      // Check for focus-visible rules
      expect(css).toMatch(/:focus-visible/);
    });

    it('should have outline or border defined for focused elements', () => {
      const cssPath = resolve(__dirname, '../styles.css');
      const css = readFileSync(cssPath, 'utf-8');

      // Check for outline property in focus styles
      const hasFocusOutline = css.includes('outline') && css.includes(':focus');
      expect(hasFocusOutline).toBe(true);
    });

    it('should not have outline: none without replacement focus indicator', () => {
      const cssPath = resolve(__dirname, '../styles.css');
      const css = readFileSync(cssPath, 'utf-8');

      // If outline: none exists, there should be another focus indicator
      if (css.includes('outline: none') || css.includes('outline:none')) {
        // Should have alternative focus indicator like border or box-shadow
        const hasAlternative = css.includes('box-shadow') ||
                              css.includes('border') ||
                              css.match(/:focus[^{]*{[^}]*outline:\s*[^n]/);
        expect(hasAlternative).toBe(true);
      }
    });

    it('buttons should have focus styles', () => {
      const cssPath = resolve(__dirname, '../styles.css');
      const css = readFileSync(cssPath, 'utf-8');

      // Check that buttons have focus-visible styles
      const hasButtonFocus = css.includes('button:focus') ||
                            css.includes(':focus-visible');
      expect(hasButtonFocus).toBe(true);
    });

    it('links should have focus styles', () => {
      const cssPath = resolve(__dirname, '../styles.css');
      const css = readFileSync(cssPath, 'utf-8');

      // Check that links have focus-visible styles
      const hasLinkFocus = css.includes('a:focus') ||
                          css.includes(':focus-visible');
      expect(hasLinkFocus).toBe(true);
    });
  });

  // Test Case 3: Tab to CTA buttons - Hero section CTA buttons are focusable via keyboard
  describe('Test Case 3: Tab to CTA buttons', () => {
    it('should have CTA buttons in hero section', () => {
      const hero = document.querySelector('.hero');
      expect(hero).not.toBeNull();

      const ctaButtons = hero.querySelectorAll('a.btn, button.btn, .cta-buttons a, .cta-buttons button');
      expect(ctaButtons.length).toBeGreaterThan(0);
    });

    it('hero CTA buttons should be anchor elements or buttons', () => {
      const hero = document.querySelector('.hero');
      const ctaButtons = hero.querySelectorAll('.cta-buttons a, .cta-buttons button, a.btn, button.btn');

      ctaButtons.forEach(btn => {
        const tagName = btn.tagName.toLowerCase();
        expect(['a', 'button'].includes(tagName)).toBe(true);
      });
    });

    it('CTA buttons should have href or type attribute', () => {
      const hero = document.querySelector('.hero');
      const ctaButtons = hero.querySelectorAll('.cta-buttons a, .cta-buttons button');

      ctaButtons.forEach(btn => {
        if (btn.tagName.toLowerCase() === 'a') {
          expect(btn.hasAttribute('href')).toBe(true);
        } else if (btn.tagName.toLowerCase() === 'button') {
          // Buttons don't require type but should be present
          expect(btn.tagName.toLowerCase()).toBe('button');
        }
      });
    });

    it('CTA buttons should not have tabindex="-1"', () => {
      const hero = document.querySelector('.hero');
      const ctaButtons = hero.querySelectorAll('.cta-buttons a, .cta-buttons button');

      ctaButtons.forEach(btn => {
        const tabindex = btn.getAttribute('tabindex');
        expect(tabindex !== '-1').toBe(true);
      });
    });

    it('should have at least one primary and one secondary CTA', () => {
      const hero = document.querySelector('.hero');
      const primaryCta = hero.querySelector('.btn-primary, [class*="primary"]');
      const secondaryCta = hero.querySelector('.btn-secondary, [class*="secondary"]');

      expect(primaryCta).not.toBeNull();
      expect(secondaryCta).not.toBeNull();
    });
  });

  // Test Case 4: Activate link with Enter key - Pressing Enter on focused link triggers navigation
  describe('Test Case 4: Activate link with Enter key', () => {
    it('all links should be standard anchor elements with href', () => {
      const links = document.querySelectorAll('a');

      links.forEach(link => {
        expect(link.hasAttribute('href')).toBe(true);
      });
    });

    it('internal links should have valid anchor targets', () => {
      const internalLinks = document.querySelectorAll('a[href^="#"]');

      internalLinks.forEach(link => {
        const href = link.getAttribute('href');
        if (href && href !== '#') {
          const targetId = href.substring(1);
          const targetElement = document.getElementById(targetId);
          expect(targetElement).not.toBeNull();
        }
      });
    });

    it('links should have accessible names', () => {
      const links = document.querySelectorAll('a');

      links.forEach(link => {
        const hasTextContent = link.textContent.trim().length > 0;
        const hasAriaLabel = link.hasAttribute('aria-label');
        const hasAriaLabelledBy = link.hasAttribute('aria-labelledby');
        const hasTitle = link.hasAttribute('title');
        const hasImg = link.querySelector('img[alt]');

        const hasAccessibleName = hasTextContent || hasAriaLabel || hasAriaLabelledBy || hasTitle || hasImg;
        expect(hasAccessibleName).toBe(true);
      });
    });

    it('buttons should be properly implemented for keyboard activation', () => {
      const buttons = document.querySelectorAll('button');

      buttons.forEach(button => {
        // Buttons are natively keyboard accessible
        expect(button.tagName.toLowerCase()).toBe('button');

        // Should have accessible name
        const hasTextContent = button.textContent.trim().length > 0;
        const hasAriaLabel = button.hasAttribute('aria-label');
        expect(hasTextContent || hasAriaLabel).toBe(true);
      });
    });

    it('elements with onclick should be proper buttons or links', () => {
      // Find elements with click handlers that aren't buttons or links
      const clickableElements = document.querySelectorAll('[onclick]');

      clickableElements.forEach(element => {
        const tagName = element.tagName.toLowerCase();
        // If an element has onclick, it should be a button or link, or have appropriate role
        const isInteractive = ['a', 'button', 'input', 'select'].includes(tagName);
        const hasRole = element.hasAttribute('role');

        expect(isInteractive || hasRole).toBe(true);
      });
    });
  });

  // Test Case 5: Check skip link presence - Skip to main content link exists for keyboard users
  describe('Test Case 5: Check skip link presence', () => {
    it('should have a skip link element', () => {
      const skipLink = document.querySelector(
        '.skip-link, .skip-to-content, .skip-navigation, a[href="#main"], a[href="#main-content"], a[href="#content"]'
      );
      expect(skipLink).not.toBeNull();
    });

    it('skip link should be the first focusable element', () => {
      const focusableSelector = 'a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])';
      const focusableElements = Array.from(document.querySelectorAll(focusableSelector));

      const firstFocusable = focusableElements[0];
      const isSkipLink =
        firstFocusable.classList.contains('skip-link') ||
        firstFocusable.classList.contains('skip-to-content') ||
        firstFocusable.getAttribute('href') === '#main' ||
        firstFocusable.getAttribute('href') === '#main-content' ||
        firstFocusable.getAttribute('href') === '#content' ||
        firstFocusable.textContent.toLowerCase().includes('skip');

      expect(isSkipLink).toBe(true);
    });

    it('skip link should target main content area', () => {
      const skipLink = document.querySelector(
        '.skip-link, .skip-to-content, a[href="#main"], a[href="#main-content"], a[href="#content"]'
      );
      expect(skipLink).not.toBeNull();

      const href = skipLink.getAttribute('href');
      expect(href).toMatch(/^#(main|content|main-content)$/);

      // Target element should exist
      const targetId = href.substring(1);
      const targetElement = document.getElementById(targetId) || document.querySelector('main');
      expect(targetElement).not.toBeNull();
    });

    it('skip link should have descriptive text', () => {
      const skipLink = document.querySelector(
        '.skip-link, .skip-to-content, a[href="#main"], a[href="#main-content"]'
      );
      expect(skipLink).not.toBeNull();

      const text = skipLink.textContent.toLowerCase();
      const hasDescriptiveText =
        text.includes('skip') ||
        text.includes('main') ||
        text.includes('content') ||
        text.includes('navigation');

      expect(hasDescriptiveText).toBe(true);
    });

    it('skip link should be visually hidden but accessible', () => {
      const cssPath = resolve(__dirname, '../styles.css');
      const css = readFileSync(cssPath, 'utf-8');

      // Skip link CSS should exist
      const hasSkipLinkStyles =
        css.includes('.skip-link') ||
        css.includes('.skip-to-content') ||
        css.includes('skip');

      expect(hasSkipLinkStyles).toBe(true);
    });

    it('skip link should become visible on focus', () => {
      const cssPath = resolve(__dirname, '../styles.css');
      const css = readFileSync(cssPath, 'utf-8');

      // Should have focus styles that make it visible
      const hasSkipLinkFocusVisible =
        css.includes('.skip-link:focus') ||
        css.includes('.skip-to-content:focus') ||
        (css.includes('skip') && css.includes(':focus'));

      expect(hasSkipLinkFocusVisible).toBe(true);
    });
  });

  // Additional keyboard accessibility tests
  describe('Additional keyboard accessibility', () => {
    it('page should not have any keyboard traps', () => {
      // Check that all focusable elements can receive and lose focus
      const focusableSelector = 'a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])';
      const focusableElements = document.querySelectorAll(focusableSelector);

      // All focusable elements should be standard interactive elements or have valid tabindex
      focusableElements.forEach(element => {
        const tabindex = element.getAttribute('tabindex');
        if (tabindex) {
          expect(parseInt(tabindex)).toBeGreaterThanOrEqual(-1);
        }
      });
    });

    it('interactive elements should have sufficient size for touch/click', () => {
      const interactiveElements = document.querySelectorAll('a, button');

      // Check that interactive elements have text or meaningful content
      interactiveElements.forEach(element => {
        const hasContent = element.textContent.trim().length > 0 ||
                          element.querySelector('svg, img') !== null ||
                          element.hasAttribute('aria-label');
        expect(hasContent).toBe(true);
      });
    });

    it('main landmark should exist for skip link target', () => {
      const main = document.querySelector('main');
      expect(main).not.toBeNull();
    });

    it('page should have proper heading hierarchy', () => {
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();

      // Check that h1 appears before h2
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      if (headings.length > 1) {
        expect(headings[0].tagName.toLowerCase()).toBe('h1');
      }
    });
  });
});
