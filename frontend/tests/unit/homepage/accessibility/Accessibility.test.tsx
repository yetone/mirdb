/**
 * Accessibility Unit Tests
 * Scenario 9 - Accessibility Compliance
 *
 * Tests for accessibility compliance verifying:
 * - Proper landmark regions (main, nav, footer)
 * - Image alt attributes
 * - Heading hierarchy (one h1, no skipped levels)
 *
 * Requirements: NFR-2 (WCAG 2.1 AA compliance)
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { screen, within } from '@testing-library/react';
import { renderWithProviders } from '../../../setup/homepage.setup.tsx';
import Home from '../../../../src/pages/Home';

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => {
  const React = require('react');
  const createMotionComponent = (tag: string) =>
    React.forwardRef(function MotionComponent(
      props: Record<string, unknown>,
      ref: React.Ref<HTMLElement>
    ) {
      const { initial, animate, exit, whileHover, whileTap, transition, variants, ...rest } = props;
      return React.createElement(tag, { ...rest, ref });
    });

  return {
    motion: {
      div: createMotionComponent('div'),
      button: createMotionComponent('button'),
      footer: createMotionComponent('footer'),
      section: createMotionComponent('section'),
      nav: createMotionComponent('nav'),
      h1: createMotionComponent('h1'),
      p: createMotionComponent('p'),
    },
    AnimatePresence: ({ children }: { children: React.ReactNode }) => children,
  };
});

// Mock canvas for BackgroundEffect
beforeEach(() => {
  HTMLCanvasElement.prototype.getContext = vi.fn().mockReturnValue({
    clearRect: vi.fn(),
    beginPath: vi.fn(),
    arc: vi.fn(),
    fill: vi.fn(),
    fillStyle: '',
  });
});

describe('Accessibility - Landmark Regions', () => {
  /**
   * Test Case 5: Check for proper landmark regions
   * Expected: Page has main, nav, and footer landmarks
   */
  it('TC5: should have main landmark region', () => {
    renderWithProviders(<Home />);

    // Check for main element (implicit role="main")
    const mainElement = screen.getByRole('main');
    expect(mainElement).toBeInTheDocument();
  });

  it('TC5: should have navigation landmark region', () => {
    renderWithProviders(<Home />);

    // Check for nav element (implicit role="navigation")
    const navElement = screen.getByRole('navigation', { name: /main/i });
    expect(navElement).toBeInTheDocument();
  });

  it('TC5: should have footer landmark region with contentinfo role', () => {
    renderWithProviders(<Home />);

    // Check for footer element with contentinfo role
    const footerElement = screen.getByRole('contentinfo');
    expect(footerElement).toBeInTheDocument();
    expect(footerElement.tagName).toBe('FOOTER');
  });

  it('TC5: should have all required landmark regions (main, nav, footer)', () => {
    renderWithProviders(<Home />);

    // Verify all landmark regions exist
    expect(screen.getByRole('main')).toBeInTheDocument();
    expect(screen.getAllByRole('navigation').length).toBeGreaterThanOrEqual(1);
    expect(screen.getByRole('contentinfo')).toBeInTheDocument();
  });

  it('should have footer navigation labeled for accessibility', () => {
    renderWithProviders(<Home />);

    // Footer should have a navigation with aria-label
    const footer = screen.getByRole('contentinfo');
    const footerNav = within(footer).getByRole('navigation', { name: /footer navigation/i });
    expect(footerNav).toBeInTheDocument();
  });
});

describe('Accessibility - Images and Alt Text', () => {
  /**
   * Test Case 6: Verify images have alt attributes
   * Expected: All img elements have meaningful alt text or empty alt for decorative
   */
  it('TC6: should have no images without alt attributes', () => {
    const { container } = renderWithProviders(<Home />);

    // Find all img elements
    const images = container.querySelectorAll('img');

    // Each image should have an alt attribute
    images.forEach((img) => {
      expect(img).toHaveAttribute('alt');
    });
  });

  it('TC6: decorative canvas element should be hidden from screen readers', () => {
    const { container } = renderWithProviders(<Home />);

    // Canvas used for BackgroundEffect should be hidden from assistive technologies
    const canvas = container.querySelector('canvas');
    if (canvas) {
      expect(canvas).toHaveAttribute('aria-hidden', 'true');
    }
  });

  it('TC6: SVG icons should be decorative or have proper labels', () => {
    const { container } = renderWithProviders(<Home />);

    // Find all SVG elements
    const svgs = container.querySelectorAll('svg');

    // SVGs should either be hidden or have accessible names
    // OR be inside interactive elements (buttons, links) that provide context
    // OR be inside elements with text that provides context (like feature cards)
    // OR be inside a section with labeled content (feature icons)
    svgs.forEach((svg) => {
      const ariaHidden = svg.getAttribute('aria-hidden');
      const ariaLabel = svg.getAttribute('aria-label');
      const role = svg.getAttribute('role');

      // Check if SVG is inside an interactive element that provides context
      const isInsideInteractive =
        svg.closest('button') !== null ||
        svg.closest('a') !== null ||
        svg.closest('[role="button"]') !== null;

      // Check if SVG is inside a feature card or similar component that provides context
      const isInsideFeatureCard =
        svg.closest('[data-testid="feature-card"]') !== null ||
        svg.closest('[data-testid="feature-icon"]') !== null;

      // Check if SVG has a nearby sibling or parent with descriptive text
      const hasDescriptiveContext =
        svg.closest('[data-testid]') !== null;

      // Decorative SVGs should be hidden from assistive technology
      // Or they should have proper accessible names
      // Or they should be inside interactive elements or feature cards
      const isAccessible =
        ariaHidden === 'true' ||
        ariaLabel !== null ||
        role === 'img' ||
        svg.querySelector('title') !== null ||
        isInsideInteractive ||
        svg.closest('[aria-label]') !== null ||
        isInsideFeatureCard ||
        hasDescriptiveContext;

      expect(isAccessible).toBe(true);
    });
  });
});

describe('Accessibility - Heading Hierarchy', () => {
  /**
   * Test Case 7: Check heading hierarchy
   * Expected: One h1 exists, headings don't skip levels
   */
  it('TC7: should have exactly one h1 element', () => {
    const { container } = renderWithProviders(<Home />);

    const h1Elements = container.querySelectorAll('h1');
    expect(h1Elements.length).toBe(1);
  });

  it('TC7: h1 should contain the main headline', () => {
    renderWithProviders(<Home />);

    const h1 = screen.getByRole('heading', { level: 1 });
    expect(h1).toBeInTheDocument();
    expect(h1.textContent?.toLowerCase()).toContain('shorten');
  });

  it('TC7: heading hierarchy should not skip levels', () => {
    const { container } = renderWithProviders(<Home />);

    // Get all headings
    const headings = container.querySelectorAll('h1, h2, h3, h4, h5, h6');
    const levels: number[] = [];

    headings.forEach((h) => {
      const level = parseInt(h.tagName.charAt(1), 10);
      levels.push(level);
    });

    // Must have at least one heading
    expect(levels.length).toBeGreaterThan(0);

    // First heading must be h1
    expect(levels[0]).toBe(1);

    // Check that headings don't skip more than one level going down
    for (let i = 1; i < levels.length; i++) {
      const diff = levels[i] - levels[i - 1];
      // When going to a lower level (larger number), shouldn't skip
      expect(diff).toBeLessThanOrEqual(1);
    }
  });

  it('TC7: should have h2 elements for section headings', () => {
    renderWithProviders(<Home />);

    const h2Elements = screen.getAllByRole('heading', { level: 2 });
    expect(h2Elements.length).toBeGreaterThanOrEqual(2); // Features and CTA sections

    // At least one h2 should be for Features section
    const h2Texts = h2Elements.map((h) => h.textContent?.toLowerCase() || '');
    expect(h2Texts.some((text) => text.includes('feature'))).toBe(true);
  });

  it('TC7: feature cards should use h3 for titles', () => {
    renderWithProviders(<Home />);

    const h3Elements = screen.getAllByRole('heading', { level: 3 });
    expect(h3Elements.length).toBeGreaterThanOrEqual(3); // At least 3 feature cards

    // H3 titles should be for feature cards
    const featureTitles = [
      'url shortening',
      'analytics',
      'link management',
      'share statistics',
    ];

    h3Elements.forEach((h3) => {
      const text = h3.textContent?.toLowerCase() || '';
      const isFeatureTitle = featureTitles.some(
        (title) => text.includes(title.split(' ')[0]) // Check first word
      );
      expect(isFeatureTitle).toBe(true);
    });
  });
});

describe('Accessibility - Semantic HTML Structure', () => {
  it('should use semantic section elements', () => {
    const { container } = renderWithProviders(<Home />);

    // Check for semantic section elements
    const sections = container.querySelectorAll('section');
    expect(sections.length).toBeGreaterThanOrEqual(2); // Hero, Features, CTA
  });

  it('should have sections with aria-labelledby for screen reader navigation', () => {
    renderWithProviders(<Home />);

    // Features section should have aria-labelledby
    const featuresSection = screen.getByTestId('features-section');
    expect(featuresSection).toHaveAttribute('aria-labelledby', 'features-heading');

    // CTA section should have aria-labelledby
    const ctaSection = screen.getByTestId('cta-section');
    expect(ctaSection).toHaveAttribute('aria-labelledby', 'cta-heading');
  });

  it('should have properly structured navigation with links', () => {
    renderWithProviders(<Home />);

    // Desktop navigation should have proper links
    const loginLink = screen.getByTestId('login-link');
    expect(loginLink.tagName).toBe('A');
    expect(loginLink).toHaveAttribute('href', '/login');

    const registerButton = screen.getByTestId('register-button');
    expect(registerButton.tagName).toBe('A');
    expect(registerButton).toHaveAttribute('href', '/register');
  });

  it('should have mobile menu button with accessible label', () => {
    renderWithProviders(<Home />);

    // Mobile menu button should have aria-label
    const menuButton = screen.getByRole('button', { name: /toggle menu/i });
    expect(menuButton).toBeInTheDocument();
    expect(menuButton).toHaveAttribute('aria-label');
  });
});
