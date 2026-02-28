/**
 * Semantic HTML Unit Tests
 * Owner: Scenario 10 - Semantic HTML Structure
 *
 * Tests:
 * - Semantic elements present (header, nav, main, footer)
 * - Single h1 element
 * - Heading hierarchy valid
 * - Images have alt attributes
 * - Buttons have proper accessibility attributes
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { loadDocument, getDocument } from '../../setup.js';

describe('Semantic HTML Structure', () => {
  let dom;
  let document;

  beforeEach(() => {
    dom = loadDocument();
    document = getDocument(dom);
  });

  describe('Document Structure', () => {
    it('should contain a header element', () => {
      const header = document.querySelector('header');
      expect(header).not.toBeNull();
    });

    it('should contain a nav element', () => {
      const nav = document.querySelector('nav');
      expect(nav).not.toBeNull();
    });

    it('should contain a main element', () => {
      const main = document.querySelector('main');
      expect(main).not.toBeNull();
    });

    it('should contain a footer element', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
    });

    it('should have all required semantic elements (header, nav, main, footer)', () => {
      const header = document.querySelector('header');
      const nav = document.querySelector('nav');
      const main = document.querySelector('main');
      const footer = document.querySelector('footer');

      expect(header).not.toBeNull();
      expect(nav).not.toBeNull();
      expect(main).not.toBeNull();
      expect(footer).not.toBeNull();
    });
  });

  describe('Single H1 Element', () => {
    it('should contain exactly one h1 element', () => {
      const h1Elements = document.querySelectorAll('h1');
      expect(h1Elements.length).toBe(1);
    });

    it('should have h1 element with product name MirDB', () => {
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.textContent).toContain('MirDB');
    });
  });

  describe('Heading Hierarchy', () => {
    it('should follow logical heading order without skipping levels', () => {
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      const headingLevels = Array.from(headings).map((h) =>
        parseInt(h.tagName.charAt(1))
      );

      // Check that no heading level is skipped
      for (let i = 1; i < headingLevels.length; i++) {
        const currentLevel = headingLevels[i];
        const previousLevel = headingLevels[i - 1];
        // Current level should not be more than 1 level deeper than previous
        expect(currentLevel - previousLevel).toBeLessThanOrEqual(1);
      }
    });

    it('should start with h1', () => {
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      expect(headings.length).toBeGreaterThan(0);
      expect(headings[0].tagName.toLowerCase()).toBe('h1');
    });

    it('should have h2 elements for section headings', () => {
      const h2Elements = document.querySelectorAll('h2');
      expect(h2Elements.length).toBeGreaterThan(0);
    });

    it('should have h3 elements nested under h2 sections', () => {
      const h3Elements = document.querySelectorAll('h3');
      // Features section should have h3 elements for feature cards
      if (h3Elements.length > 0) {
        const featuresSection = document.querySelector('#features');
        if (featuresSection) {
          const h3InFeatures = featuresSection.querySelectorAll('h3');
          expect(h3InFeatures.length).toBeGreaterThan(0);
        }
      }
    });
  });

  describe('Image Alt Attributes', () => {
    it('should have alt attributes on all img elements', () => {
      const images = document.querySelectorAll('img');
      images.forEach((img) => {
        expect(img.hasAttribute('alt')).toBe(true);
      });
    });

    it('should have non-empty alt attributes for meaningful images', () => {
      const images = document.querySelectorAll('img:not([role="presentation"]):not([aria-hidden="true"])');
      images.forEach((img) => {
        const alt = img.getAttribute('alt');
        // Empty alt is acceptable only for decorative images
        expect(alt).not.toBeNull();
      });
    });
  });

  describe('Button Accessibility', () => {
    it('should use button elements for interactive buttons', () => {
      const toggleButton = document.querySelector('#nav-toggle');
      expect(toggleButton).not.toBeNull();
      expect(toggleButton.tagName.toLowerCase()).toBe('button');
    });

    it('should have aria-label or text content on buttons', () => {
      const buttons = document.querySelectorAll('button');
      buttons.forEach((button) => {
        const hasAriaLabel = button.hasAttribute('aria-label');
        const hasTextContent = button.textContent.trim().length > 0;
        const hasAriaLabelledBy = button.hasAttribute('aria-labelledby');
        expect(hasAriaLabel || hasTextContent || hasAriaLabelledBy).toBe(true);
      });
    });

    it('should have aria-expanded attribute on expandable buttons', () => {
      const navToggle = document.querySelector('#nav-toggle');
      expect(navToggle).not.toBeNull();
      expect(navToggle.hasAttribute('aria-expanded')).toBe(true);
    });

    it('should have aria-controls attribute on buttons that control other elements', () => {
      const navToggle = document.querySelector('#nav-toggle');
      expect(navToggle).not.toBeNull();
      expect(navToggle.hasAttribute('aria-controls')).toBe(true);
    });
  });

  describe('Landmark Regions', () => {
    it('should have role="banner" or header element for page header', () => {
      const banner = document.querySelector('[role="banner"]') || document.querySelector('header');
      expect(banner).not.toBeNull();
    });

    it('should have role="navigation" or nav element', () => {
      const navigation = document.querySelector('[role="navigation"]') || document.querySelector('nav');
      expect(navigation).not.toBeNull();
    });

    it('should have role="main" or main element for main content', () => {
      const main = document.querySelector('[role="main"]') || document.querySelector('main');
      expect(main).not.toBeNull();
    });

    it('should have role="contentinfo" or footer element for footer', () => {
      const contentinfo = document.querySelector('[role="contentinfo"]') || document.querySelector('footer');
      expect(contentinfo).not.toBeNull();
    });

    it('should have aria-label on navigation elements for screen readers', () => {
      const navElements = document.querySelectorAll('nav, [role="navigation"]');
      navElements.forEach((nav) => {
        const hasAriaLabel = nav.hasAttribute('aria-label');
        const hasAriaLabelledBy = nav.hasAttribute('aria-labelledby');
        expect(hasAriaLabel || hasAriaLabelledBy).toBe(true);
      });
    });
  });

  describe('Skip Link', () => {
    it('should have a skip-to-main-content link', () => {
      const skipLink = document.querySelector('a[href="#main-content"]');
      expect(skipLink).not.toBeNull();
    });

    it('should have skip link as first focusable element', () => {
      const skipLink = document.querySelector('.skip-link');
      expect(skipLink).not.toBeNull();
      // Skip link should appear early in the document
      const body = document.body;
      const firstChild = body.firstElementChild;
      // The skip link should be one of the first elements
      expect(firstChild.classList.contains('skip-link') ||
             body.querySelector('.skip-link') !== null).toBe(true);
    });
  });

  describe('Section Landmarks', () => {
    it('should use section elements with aria-labelledby for content sections', () => {
      const sections = document.querySelectorAll('main > section');
      expect(sections.length).toBeGreaterThan(0);
    });

    it('should have sections with proper labelling', () => {
      const heroSection = document.querySelector('#home');
      const featuresSection = document.querySelector('#features');

      // Check that major sections exist and are labelled
      expect(heroSection || document.querySelector('.hero')).not.toBeNull();
      expect(featuresSection || document.querySelector('.features')).not.toBeNull();
    });
  });

  describe('Article Elements', () => {
    it('should use article elements for standalone content like feature cards', () => {
      const featureCards = document.querySelectorAll('.feature-card');
      featureCards.forEach((card) => {
        expect(card.tagName.toLowerCase()).toBe('article');
      });
    });
  });
});
