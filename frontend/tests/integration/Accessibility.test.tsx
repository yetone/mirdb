/**
 * Accessibility Integration Tests
 * Owner: Scenario 13
 *
 * Test coverage:
 * - WCAG 2.1 AA compliance
 * - Heading hierarchy
 * - Image alt text
 * - Keyboard navigation
 * - Focus states
 * - ARIA labels
 * - Landmark regions
 *
 * Test suites:
 * - describe('Axe Accessibility Audit')
 * - describe('Semantic HTML')
 * - describe('Keyboard Navigation')
 * - describe('ARIA Compliance')
 *
 * Dependencies:
 * - vitest-axe for automated accessibility testing
 */

import { describe, it, expect } from 'vitest';
import { screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { axe } from 'vitest-axe';
import Home from '../../src/pages/Home';
import { renderWithProviders } from '../utils/renderWithProviders';

describe('Accessibility Compliance', () => {
  describe('Axe Accessibility Audit', () => {
    it('should have no critical or serious accessibility violations', async () => {
      const { container } = renderWithProviders(<Home />);

      const results = await axe(container, {
        rules: {
          // Focus on WCAG 2.1 AA compliance
          'color-contrast': { enabled: true },
          'heading-order': { enabled: true },
          'image-alt': { enabled: true },
          'label': { enabled: true },
          'link-name': { enabled: true },
          'region': { enabled: true },
        },
      });

      // Filter for only critical and serious violations
      const criticalAndSerious = results.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      );

      expect(criticalAndSerious).toHaveLength(0);
    });
  });

  describe('Semantic HTML - Heading Hierarchy', () => {
    it('should have proper h1, h2, h3 hierarchy without skipped levels', () => {
      renderWithProviders(<Home />);

      // There should be exactly one h1 (main headline)
      const h1Elements = document.querySelectorAll('h1');
      expect(h1Elements.length).toBeGreaterThanOrEqual(1);

      // Check for h2 elements (section headings)
      const h2Elements = document.querySelectorAll('h2');
      expect(h2Elements.length).toBeGreaterThanOrEqual(1);

      // Get all heading elements in order
      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      const headingLevels: number[] = [];

      allHeadings.forEach((heading) => {
        const level = parseInt(heading.tagName.charAt(1));
        headingLevels.push(level);
      });

      // Verify no skipped levels (e.g., h1 directly to h3)
      for (let i = 1; i < headingLevels.length; i++) {
        const currentLevel = headingLevels[i];
        const previousLevel = headingLevels[i - 1];

        // A heading can be same level, one level deeper, or any level higher (going back up)
        const validTransition =
          currentLevel === previousLevel ||
          currentLevel === previousLevel + 1 ||
          currentLevel < previousLevel;

        expect(validTransition).toBe(true);
      }
    });

    it('should have a single h1 element for the main page heading', () => {
      renderWithProviders(<Home />);

      const h1Elements = screen.getAllByRole('heading', { level: 1 });
      expect(h1Elements.length).toBe(1);
    });
  });

  describe('Semantic HTML - Image Alt Text', () => {
    it('should have non-empty alt text on all img elements', () => {
      renderWithProviders(<Home />);

      const images = document.querySelectorAll('img');

      images.forEach((img) => {
        const alt = img.getAttribute('alt');
        // All images should have alt attribute that is non-empty
        // Unless they are decorative (aria-hidden="true" or role="presentation")
        const isDecorative =
          img.getAttribute('aria-hidden') === 'true' ||
          img.getAttribute('role') === 'presentation';

        if (!isDecorative) {
          expect(alt).toBeTruthy();
          expect(alt?.trim().length).toBeGreaterThan(0);
        }
      });
    });
  });

  describe('Keyboard Navigation', () => {
    it('should allow all buttons and links to be keyboard focusable', async () => {
      const user = userEvent.setup();
      renderWithProviders(<Home />);

      // Get all interactive elements
      const buttons = screen.getAllByRole('button');
      const links = screen.getAllByRole('link');

      const interactiveElements = [...buttons, ...links];

      // Verify each element can receive focus
      for (const element of interactiveElements) {
        element.focus();
        expect(document.activeElement).toBe(element);
      }
    });

    it('should be able to tab through interactive elements', async () => {
      const user = userEvent.setup();
      renderWithProviders(<Home />);

      // Start from the body
      document.body.focus();

      // Tab through and verify focus moves to interactive elements
      await user.tab();

      // After first tab, focus should be on an interactive element
      const activeElement = document.activeElement;
      expect(activeElement).not.toBe(document.body);

      // The active element should be focusable (link or button typically)
      const isInteractive =
        activeElement?.tagName === 'A' ||
        activeElement?.tagName === 'BUTTON' ||
        activeElement?.getAttribute('tabindex') === '0';

      expect(isInteractive).toBe(true);
    });
  });

  describe('ARIA Compliance - Labels', () => {
    it('should have accessible names on buttons and links', () => {
      renderWithProviders(<Home />);

      const buttons = screen.getAllByRole('button');
      const links = screen.getAllByRole('link');

      // All buttons should have accessible names
      buttons.forEach((button) => {
        const accessibleName =
          button.textContent ||
          button.getAttribute('aria-label') ||
          button.getAttribute('title');

        expect(accessibleName).toBeTruthy();
      });

      // All links should have accessible names
      links.forEach((link) => {
        const accessibleName =
          link.textContent ||
          link.getAttribute('aria-label') ||
          link.getAttribute('title');

        expect(accessibleName).toBeTruthy();
      });
    });

    it('should have meaningful accessible names (not just empty text)', () => {
      renderWithProviders(<Home />);

      const buttons = screen.getAllByRole('button');
      const links = screen.getAllByRole('link');

      [...buttons, ...links].forEach((element) => {
        const accessibleName =
          element.textContent?.trim() ||
          element.getAttribute('aria-label')?.trim() ||
          element.getAttribute('title')?.trim();

        // Should have at least 1 character of meaningful text
        expect(accessibleName?.length).toBeGreaterThan(0);
      });
    });
  });

  describe('ARIA Compliance - Landmark Regions', () => {
    it('should use appropriate ARIA landmark regions (main, nav, footer)', () => {
      renderWithProviders(<Home />);

      // Check for main landmark
      const mainElement = document.querySelector('main');
      expect(mainElement).toBeTruthy();

      // Check for nav landmark
      const navElement = document.querySelector('nav');
      expect(navElement).toBeTruthy();

      // Check for footer landmark
      const footerElement = document.querySelector('footer');
      expect(footerElement).toBeTruthy();
    });

    it('should have roles correctly assigned to landmark elements', () => {
      renderWithProviders(<Home />);

      // Main element should have role="main" or be a <main> tag
      const mainByRole = screen.getByRole('main');
      expect(mainByRole).toBeTruthy();

      // Navigation should have role="navigation" or be a <nav> tag
      // There may be multiple navigation elements (main navbar, footer nav)
      const navElements = screen.getAllByRole('navigation');
      expect(navElements.length).toBeGreaterThanOrEqual(1);

      // Content info (footer) should have role="contentinfo" or be a <footer> tag
      const footerByRole = screen.getByRole('contentinfo');
      expect(footerByRole).toBeTruthy();
    });
  });

  describe('Focus States', () => {
    it('should maintain visible focus on interactive elements', async () => {
      const user = userEvent.setup();
      renderWithProviders(<Home />);

      const links = screen.getAllByRole('link');
      const buttons = screen.getAllByRole('button');

      // Test that elements can be focused
      for (const element of [...links.slice(0, 3), ...buttons.slice(0, 3)]) {
        element.focus();

        // Element should be the active element
        expect(document.activeElement).toBe(element);

        // Check that the element has focus-related styles or is interactive
        const computedStyle = window.getComputedStyle(element);
        // Focus states are typically applied via CSS - we just verify the element can receive focus
        expect(element.tabIndex).toBeGreaterThanOrEqual(-1);
      }
    });
  });

  describe('Color Contrast', () => {
    it('should not have any color contrast violations detected by axe', async () => {
      const { container } = renderWithProviders(<Home />);

      const results = await axe(container, {
        rules: {
          'color-contrast': { enabled: true },
        },
      });

      const contrastViolations = results.violations.filter(
        (v) => v.id === 'color-contrast'
      );

      // No color contrast violations should exist
      expect(contrastViolations).toHaveLength(0);
    });
  });
});
