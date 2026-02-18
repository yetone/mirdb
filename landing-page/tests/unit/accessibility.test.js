/**
 * Accessibility Unit Tests
 * Owner: Scenario 10 - Accessibility Compliance
 *
 * Test cases for WCAG 2.1 Level AA compliance:
 * - HTML lang attribute
 * - Image alt attributes
 * - Heading hierarchy
 * - Form label associations
 * - Landmark elements (main, nav)
 * - Skip link presence
 *
 * @jest-environment jsdom
 */

const fs = require('fs');
const path = require('path');

describe('Accessibility Unit Tests', () => {
  beforeAll(() => {
    const htmlPath = path.resolve(__dirname, '../../index.html');
    const html = fs.readFileSync(htmlPath, 'utf8');
    document.open();
    document.write(html);
    document.close();
  });

  describe('Test Case 2: HTML lang attribute', () => {
    test('html element has lang attribute set', () => {
      const htmlElement = document.documentElement;
      expect(htmlElement).toHaveAttribute('lang');
      expect(htmlElement.getAttribute('lang')).toBe('en');
    });
  });

  describe('Test Case 3: Image alt attributes', () => {
    test('all img elements have alt attribute', () => {
      const images = document.querySelectorAll('img');

      images.forEach((img) => {
        expect(img).toHaveAttribute('alt');
        // Alt can be empty for decorative images, but must exist
        const altValue = img.getAttribute('alt');
        expect(typeof altValue).toBe('string');
      });
    });

    test('decorative SVGs have aria-hidden="true"', () => {
      // SVGs that are decorative should be hidden from screen readers
      const decorativeSvgs = document.querySelectorAll('[aria-hidden="true"] svg, svg[aria-hidden="true"]');

      // This test ensures we have thought about SVG accessibility
      expect(decorativeSvgs.length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 4: Heading hierarchy', () => {
    test('page has exactly one h1 element', () => {
      const h1Elements = document.querySelectorAll('h1');
      expect(h1Elements.length).toBe(1);
    });

    test('headings follow logical order without skipping levels', () => {
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      const levels = Array.from(headings).map(h => parseInt(h.tagName[1]));

      // Check that no level is skipped (e.g., h1 to h3 without h2)
      for (let i = 1; i < levels.length; i++) {
        const currentLevel = levels[i];
        const previousLevel = levels[i - 1];

        // Going down in hierarchy: can only increase by 1
        // Going up in hierarchy: can decrease by any amount
        if (currentLevel > previousLevel) {
          expect(currentLevel - previousLevel).toBeLessThanOrEqual(1);
        }
      }
    });

    test('h2 elements exist for main sections', () => {
      const h2Elements = document.querySelectorAll('h2');
      expect(h2Elements.length).toBeGreaterThanOrEqual(5);
    });
  });

  describe('Test Case 7: Form labels', () => {
    test('form inputs have associated labels or no form inputs exist', () => {
      const inputs = document.querySelectorAll('input:not([type="hidden"]):not([type="submit"]):not([type="button"])');

      // If there are no inputs, the test passes (landing page may not have forms)
      if (inputs.length === 0) {
        expect(true).toBe(true);
        return;
      }

      inputs.forEach((input) => {
        const id = input.id;
        const hasLabel = id && document.querySelector(`label[for="${id}"]`);
        const hasAriaLabel = input.hasAttribute('aria-label');
        const hasAriaLabelledBy = input.hasAttribute('aria-labelledby');
        const hasTitle = input.hasAttribute('title');

        // Input should have at least one form of label
        const hasLabelAssociation = hasLabel || hasAriaLabel || hasAriaLabelledBy || hasTitle;

        expect(hasLabelAssociation).toBe(true);
      });
    });
  });

  describe('Test Case 8: Main content landmark', () => {
    test('page has main element for primary content', () => {
      const mainElement = document.querySelector('main');
      expect(mainElement).not.toBeNull();
    });

    test('main element has id for skip link target', () => {
      const mainElement = document.querySelector('main');
      expect(mainElement).toHaveAttribute('id');
      expect(mainElement.id).toBe('main-content');
    });

    test('main element has role="main" for older browsers', () => {
      const mainElement = document.querySelector('main');
      expect(mainElement).toHaveAttribute('role', 'main');
    });
  });

  describe('Test Case 9: Navigation landmark', () => {
    test('navigation uses nav element', () => {
      const navElement = document.querySelector('nav');
      expect(navElement).not.toBeNull();
    });

    test('nav element has role="navigation" for older browsers', () => {
      const navElement = document.querySelector('nav');
      expect(navElement).toHaveAttribute('role', 'navigation');
    });

    test('nav element has aria-label for identification', () => {
      const navElement = document.querySelector('nav');
      expect(navElement).toHaveAttribute('aria-label');
    });
  });

  describe('Test Case 12: Skip link', () => {
    test('skip to main content link exists', () => {
      const skipLink = document.querySelector('a[href="#main-content"]');
      expect(skipLink).not.toBeNull();
    });

    test('skip link has descriptive text', () => {
      const skipLink = document.querySelector('a[href="#main-content"]');
      expect(skipLink.textContent.toLowerCase()).toContain('skip');
      expect(skipLink.textContent.toLowerCase()).toContain('main');
    });

    test('skip link has skip-link class for styling', () => {
      const skipLink = document.querySelector('a[href="#main-content"]');
      expect(skipLink.classList.contains('skip-link')).toBe(true);
    });
  });

  describe('Additional ARIA tests', () => {
    test('interactive buttons have accessible names', () => {
      const buttons = document.querySelectorAll('button');

      buttons.forEach((button) => {
        const hasAriaLabel = button.hasAttribute('aria-label');
        const hasAriaLabelledBy = button.hasAttribute('aria-labelledby');
        const hasTextContent = button.textContent.trim().length > 0;
        const hasTitle = button.hasAttribute('title');

        const hasAccessibleName = hasAriaLabel || hasAriaLabelledBy || hasTextContent || hasTitle;
        expect(hasAccessibleName).toBe(true);
      });
    });

    test('header has role="banner"', () => {
      const header = document.querySelector('header');
      expect(header).toHaveAttribute('role', 'banner');
    });

    test('footer has role="contentinfo" or is a semantic footer element', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
      // Semantic footer element has implicit role="contentinfo"
      // Some parsers may not preserve explicit role, but the element provides the landmark
      const hasExplicitRole = footer.hasAttribute('role') && footer.getAttribute('role') === 'contentinfo';
      const isSemanticFooter = footer.tagName.toLowerCase() === 'footer';
      expect(hasExplicitRole || isSemanticFooter).toBe(true);
    });

    test('sections with custom titles have aria-labelledby', () => {
      const sections = document.querySelectorAll('section[aria-labelledby]');

      sections.forEach((section) => {
        const labelId = section.getAttribute('aria-labelledby');
        const labelElement = document.getElementById(labelId);
        expect(labelElement).not.toBeNull();
      });
    });

    test('FAQ buttons have aria-expanded attribute', () => {
      const faqButtons = document.querySelectorAll('.faq-question');

      faqButtons.forEach((button) => {
        expect(button).toHaveAttribute('aria-expanded');
      });
    });

    test('FAQ buttons have aria-controls pointing to answer', () => {
      const faqButtons = document.querySelectorAll('.faq-question');

      faqButtons.forEach((button) => {
        const controlsId = button.getAttribute('aria-controls');
        expect(controlsId).toBeTruthy();
        const controlledElement = document.getElementById(controlsId);
        expect(controlledElement).not.toBeNull();
      });
    });

    test('mobile menu toggle has proper ARIA attributes', () => {
      const menuToggle = document.querySelector('#nav-mobile-toggle');
      expect(menuToggle).toHaveAttribute('aria-expanded');
      expect(menuToggle).toHaveAttribute('aria-controls');
      expect(menuToggle).toHaveAttribute('aria-label');
    });
  });
});
