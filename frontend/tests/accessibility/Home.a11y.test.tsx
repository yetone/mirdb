/**
 * Homepage Accessibility Tests
 * Owner: Scenario 6 - Accessibility Compliance
 *
 * Test coverage using axe-core:
 * - WCAG 2.1 AA compliance
 * - Heading hierarchy (h1 -> h2, no skipped levels)
 * - ARIA landmarks present
 * - Focus management
 * - Color contrast ratios
 * - Keyboard navigation
 * - Skip to content functionality
 */

import { describe, it, expect } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import { axe } from 'vitest-axe';
import * as matchers from 'vitest-axe/matchers';
import { Home } from '../../src/pages/Home';

// Extend expect with axe matchers
expect.extend(matchers);

// Helper function to render Home with router context
const renderHome = () => {
  return render(
    <MemoryRouter initialEntries={['/']}>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/login" element={<div>Login Page</div>} />
        <Route path="/register" element={<div>Register Page</div>} />
      </Routes>
    </MemoryRouter>
  );
};

describe('Accessibility Compliance - WCAG 2.1 AA', () => {
  /**
   * Test Case 1: Run axe-core on rendered homepage
   * Input: Run axe-core on rendered homepage
   * Expected: No critical or serious accessibility violations
   * Type: e2e
   */
  it('passes axe-core accessibility audit with no critical or serious violations', async () => {
    const { container } = renderHome();

    const results = await axe(container, {
      rules: {
        // Enable WCAG 2.1 AA rules
        'color-contrast': { enabled: true },
        'heading-order': { enabled: true },
        'landmark-one-main': { enabled: true },
        'region': { enabled: true },
      },
    });

    // Filter for critical and serious violations only
    const criticalOrSerious = results.violations.filter(
      (violation) => violation.impact === 'critical' || violation.impact === 'serious'
    );

    expect(criticalOrSerious).toHaveLength(0);
  });

  /**
   * Test Case 2: Tab through all interactive elements
   * Input: Tab through all interactive elements
   * Expected: All buttons and links receive focus in logical order
   * Type: e2e
   */
  it('allows tabbing through all interactive elements in logical order', () => {
    renderHome();

    // Get all focusable elements
    const focusableElements = document.querySelectorAll(
      'a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])'
    );

    // Filter to visible and enabled elements
    const visibleFocusable = Array.from(focusableElements).filter((el) => {
      const style = window.getComputedStyle(el);
      return (
        style.display !== 'none' &&
        style.visibility !== 'hidden' &&
        !el.hasAttribute('disabled')
      );
    });

    // Verify there are focusable elements
    expect(visibleFocusable.length).toBeGreaterThan(0);

    // Tab through elements and verify focus moves
    visibleFocusable.forEach((element, index) => {
      (element as HTMLElement).focus();
      expect(document.activeElement).toBe(element);
    });

    // Verify skip link is first focusable
    const skipLink = screen.getByTestId('skip-to-content');
    expect(visibleFocusable[0]).toBe(skipLink);
  });

  /**
   * Test Case 3: Check focus indicators visibility
   * Input: Check focus indicators visibility
   * Expected: Focus ring/outline visible on focused elements
   * Type: e2e
   */
  it('has visible focus indicators on interactive elements', () => {
    renderHome();

    // Get interactive elements
    const buttons = screen.getAllByRole('button');
    const links = screen.getAllByRole('link');

    // Check buttons have focus styles (DaisyUI provides these via btn class)
    buttons.forEach((button) => {
      expect(button).toHaveClass('btn');
    });

    // Check skip link has focus styles
    const skipLink = screen.getByTestId('skip-to-content');
    expect(skipLink).toHaveClass('focus:ring-2');
  });

  /**
   * Test Case 4: Query for ARIA landmarks
   * Input: Query for ARIA landmarks
   * Expected: main and contentinfo landmarks present
   * Type: integration
   */
  it('has main and contentinfo ARIA landmarks', () => {
    renderHome();

    // Check for main landmark
    const mainLandmark = screen.getByRole('main');
    expect(mainLandmark).toBeInTheDocument();
    expect(mainLandmark).toHaveAttribute('aria-label', 'Homepage main content');

    // Check for contentinfo landmark (footer)
    const contentInfoLandmark = screen.getByRole('contentinfo');
    expect(contentInfoLandmark).toBeInTheDocument();
  });

  /**
   * Test Case 5: Check decorative icons have aria-hidden
   * Input: Check decorative icons have aria-hidden
   * Expected: Decorative icons marked with aria-hidden='true'
   * Type: unit
   */
  it('marks decorative icons with aria-hidden="true"', () => {
    renderHome();

    // Check feature icons
    const featureIcons = screen.getAllByTestId('feature-icon');
    featureIcons.forEach((icon) => {
      const svg = icon.querySelector('svg');
      expect(svg).toHaveAttribute('aria-hidden', 'true');
    });

    // Check step icons
    const stepIcons = screen.getAllByTestId('step-icon');
    stepIcons.forEach((icon) => {
      const svg = icon.querySelector('svg');
      expect(svg).toHaveAttribute('aria-hidden', 'true');
    });
  });

  /**
   * Test Case 6: Check buttons have accessible names
   * Input: Check buttons have accessible names
   * Expected: All buttons have text content or aria-label
   * Type: integration
   */
  it('ensures all buttons have accessible names', () => {
    renderHome();

    const buttons = screen.getAllByRole('button');

    buttons.forEach((button) => {
      // Button should have accessible name (text content or aria-label)
      const accessibleName =
        button.textContent?.trim() ||
        button.getAttribute('aria-label') ||
        button.getAttribute('aria-labelledby');
      expect(accessibleName).toBeTruthy();
    });
  });

  /**
   * Test Case 7: Check links have accessible names
   * Input: Check links have accessible names
   * Expected: All links have descriptive text content
   * Type: integration
   */
  it('ensures all links have accessible names', () => {
    renderHome();

    const links = screen.getAllByRole('link');

    links.forEach((link) => {
      // Link should have accessible name (text content or aria-label)
      const accessibleName =
        link.textContent?.trim() ||
        link.getAttribute('aria-label') ||
        link.getAttribute('aria-labelledby');
      expect(accessibleName).toBeTruthy();
      expect(accessibleName!.length).toBeGreaterThan(0);
    });
  });

  /**
   * Test Case 8: Check color contrast ratios
   * Input: Check color contrast ratios
   * Expected: Text meets WCAG AA contrast requirements (4.5:1 normal, 3:1 large)
   * Type: e2e
   */
  it('passes color contrast checks via axe-core', async () => {
    const { container } = renderHome();

    const results = await axe(container, {
      rules: {
        'color-contrast': { enabled: true },
      },
      runOnly: ['color-contrast'],
    });

    // Check no color contrast violations
    const contrastViolations = results.violations.filter(
      (v) => v.id === 'color-contrast'
    );
    expect(contrastViolations).toHaveLength(0);
  });

  /**
   * Test Case 9: Test skip-to-content with keyboard only
   * Input: Test skip-to-content with keyboard only
   * Expected: First Tab focuses skip link, Enter activates it, focus moves to main
   * Type: e2e
   */
  it('allows skip-to-content link to be activated with keyboard', () => {
    renderHome();

    // Get skip link
    const skipLink = screen.getByTestId('skip-to-content');
    const mainContent = screen.getByRole('main');

    // Focus skip link (simulating first Tab)
    skipLink.focus();
    expect(document.activeElement).toBe(skipLink);

    // Activate skip link (simulating Enter key)
    fireEvent.click(skipLink);

    // Main content should now have focus
    expect(document.activeElement).toBe(mainContent);
    expect(mainContent).toHaveAttribute('id', 'main-content');
    expect(mainContent).toHaveAttribute('tabindex', '-1');
  });

  /**
   * Test Case 10: Check heading levels sequence
   * Input: Check heading levels sequence
   * Expected: Headings follow logical order (h1 -> h2 -> h3), no skipped levels
   * Type: integration
   */
  it('has headings in logical order without skipped levels', () => {
    renderHome();

    // Get all headings
    const headings = screen.getAllByRole('heading');

    // Verify there's exactly one h1
    const h1Elements = headings.filter(
      (h) => h.tagName.toLowerCase() === 'h1'
    );
    expect(h1Elements).toHaveLength(1);

    // Get heading levels in order of appearance
    const headingLevels = headings.map((h) => parseInt(h.tagName.substring(1)));

    // Verify no skipped levels
    for (let i = 0; i < headingLevels.length; i++) {
      const currentLevel = headingLevels[i];

      // First heading should be h1
      if (i === 0) {
        expect(currentLevel).toBe(1);
        continue;
      }

      const previousLevel = headingLevels[i - 1];

      // Current level should not skip more than one level from any previous heading
      // (e.g., h1 -> h3 is not allowed, but h2 -> h3 or h3 -> h2 is fine)
      if (currentLevel > previousLevel) {
        expect(currentLevel - previousLevel).toBeLessThanOrEqual(1);
      }
    }

    // Verify h2 sections exist
    const h2Elements = headings.filter(
      (h) => h.tagName.toLowerCase() === 'h2'
    );
    expect(h2Elements.length).toBeGreaterThanOrEqual(2);

    // Verify specific h2 headings
    expect(screen.getByRole('heading', { level: 2, name: /powerful features/i })).toBeInTheDocument();
    expect(screen.getByRole('heading', { level: 2, name: /how it works/i })).toBeInTheDocument();
  });
});

describe('Accessibility - Additional Compliance Checks', () => {
  it('has sections with aria-labelledby attributes', () => {
    renderHome();

    // Hero section
    const heroSection = document.querySelector('[aria-labelledby="hero-headline"]');
    expect(heroSection).toBeInTheDocument();

    // Features section
    const featuresSection = document.querySelector('[aria-labelledby="features-heading"]');
    expect(featuresSection).toBeInTheDocument();

    // How It Works section
    const howItWorksSection = document.querySelector('[aria-labelledby="how-it-works-heading"]');
    expect(howItWorksSection).toBeInTheDocument();
  });

  it('has footer navigation with aria-label', () => {
    renderHome();

    const footerNav = screen.getByRole('navigation', { name: /footer navigation/i });
    expect(footerNav).toBeInTheDocument();
  });

  it('has ordered list for How It Works steps', () => {
    renderHome();

    const orderedList = screen.getByTestId('how-it-works-steps');
    expect(orderedList.tagName.toLowerCase()).toBe('ol');
  });

  it('renders without any axe violations', async () => {
    const { container } = renderHome();

    const results = await axe(container);
    expect(results).toHaveNoViolations();
  });
});
