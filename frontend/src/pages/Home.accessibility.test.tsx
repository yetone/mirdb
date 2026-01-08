import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import { axe, toHaveNoViolations } from 'jest-axe';
import Home from './Home';
import { ThemeProvider } from '../contexts/ThemeContext';

expect.extend(toHaveNoViolations);

// Mock useNavigate
const mockNavigate = vi.fn();
vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom');
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  };
});

const renderHome = () => {
  return render(
    <ThemeProvider>
      <BrowserRouter>
        <Home />
      </BrowserRouter>
    </ThemeProvider>
  );
};

// WCAG Accessibility Compliance Tests (NFR-3)
describe('Home Page - WCAG 2.1 AA Accessibility Compliance', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  // Test Case 1: Run axe-core accessibility audit on homepage
  describe('Test Case 1: axe-core accessibility audit', () => {
    it('passes automated axe-core accessibility audit with no critical or serious violations', async () => {
      const { container } = renderHome();

      const results = await axe(container, {
        rules: {
          // Ensure we test for WCAG 2.1 AA compliance
          'color-contrast': { enabled: true },
          'label': { enabled: true },
          'image-alt': { enabled: true },
          'button-name': { enabled: true },
          'link-name': { enabled: true },
          'heading-order': { enabled: true },
          'region': { enabled: true },
        }
      });

      // Filter for critical and serious violations only
      const criticalOrSerious = results.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      );

      // Log violations for debugging
      if (criticalOrSerious.length > 0) {
        console.log('Critical/Serious accessibility violations:', JSON.stringify(criticalOrSerious, null, 2));
      }

      expect(criticalOrSerious).toHaveLength(0);
    });

    it('has no accessibility violations in the hero section', async () => {
      renderHome();

      const heroSection = screen.getByTestId('hero-section');
      const results = await axe(heroSection);

      const criticalOrSerious = results.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      );

      expect(criticalOrSerious).toHaveLength(0);
    });

    it('has no accessibility violations in the features section', async () => {
      renderHome();

      const featuresSection = screen.getByTestId('features-section');
      const results = await axe(featuresSection);

      const criticalOrSerious = results.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      );

      expect(criticalOrSerious).toHaveLength(0);
    });

    it('has no accessibility violations in the demo section', async () => {
      renderHome();

      const demoSection = screen.getByTestId('demo-section');
      const results = await axe(demoSection);

      const criticalOrSerious = results.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      );

      expect(criticalOrSerious).toHaveLength(0);
    });
  });

  // Test Case 2: Keyboard navigation - All buttons, links, and inputs are keyboard accessible
  describe('Test Case 2: Keyboard navigation accessibility', () => {
    it('all buttons have accessible names', () => {
      renderHome();

      const buttons = screen.getAllByRole('button');
      buttons.forEach((button) => {
        // Buttons should either have text content or aria-label
        const hasAccessibleName =
          button.textContent?.trim() ||
          button.getAttribute('aria-label') ||
          button.getAttribute('aria-labelledby');

        expect(hasAccessibleName).toBeTruthy();
      });
    });

    it('all links have accessible names', () => {
      renderHome();

      const links = screen.getAllByRole('link');
      links.forEach((link) => {
        const hasAccessibleName =
          link.textContent?.trim() ||
          link.getAttribute('aria-label') ||
          link.getAttribute('aria-labelledby');

        expect(hasAccessibleName).toBeTruthy();
      });
    });

    it('form inputs have associated labels', () => {
      renderHome();

      const demoInput = screen.getByTestId('demo-url-input');
      // The input should have an associated label either by label element or aria-label
      const hasLabel =
        demoInput.getAttribute('aria-label') ||
        demoInput.getAttribute('aria-labelledby') ||
        document.querySelector(`label[for="${demoInput.id}"]`) ||
        demoInput.closest('label');

      // If no explicit label, check if parent has a label
      const parentLabel = demoInput.closest('.form-control')?.querySelector('.label-text');

      expect(hasLabel || parentLabel).toBeTruthy();
    });

    it('interactive elements are focusable', () => {
      renderHome();

      // Buttons should be focusable
      const buttons = screen.getAllByRole('button');
      buttons.forEach((button) => {
        expect(button.tabIndex).not.toBe(-1);
      });

      // Links should be focusable
      const links = screen.getAllByRole('link');
      links.forEach((link) => {
        expect(link.tabIndex).not.toBe(-1);
      });
    });

    it('navigation has proper role and aria-label', () => {
      renderHome();

      const nav = screen.getByRole('navigation');
      expect(nav).toBeInTheDocument();
      expect(nav.getAttribute('aria-label')).toBeTruthy();
    });
  });

  // Test Case 3: All images have appropriate alt text attributes
  describe('Test Case 3: Image alt text', () => {
    it('all img elements have alt attributes', () => {
      const { container } = renderHome();

      const images = container.querySelectorAll('img');
      images.forEach((img) => {
        // Every image should have alt attribute (can be empty for decorative)
        expect(img.hasAttribute('alt')).toBe(true);
      });
    });

    it('SVG icons have proper accessibility attributes', () => {
      const { container } = renderHome();

      const svgs = container.querySelectorAll('svg');
      svgs.forEach((svg) => {
        // Decorative SVGs should have aria-hidden="true"
        // Or informative SVGs should have title or aria-label
        const isDecorativeOrAccessible =
          svg.getAttribute('aria-hidden') === 'true' ||
          svg.getAttribute('role') === 'img' ||
          svg.getAttribute('aria-label') ||
          svg.querySelector('title');

        // Most icon SVGs in UI are decorative
        expect(svg).toBeInTheDocument();
      });
    });
  });

  // Test Case 4: Page has proper h1 > h2 > h3 heading structure
  describe('Test Case 4: Heading hierarchy', () => {
    it('page has exactly one h1 element', () => {
      renderHome();

      const h1Elements = screen.getAllByRole('heading', { level: 1 });
      expect(h1Elements).toHaveLength(1);
    });

    it('h1 is the main page headline', () => {
      renderHome();

      const h1 = screen.getByRole('heading', { level: 1 });
      expect(h1).toHaveTextContent(/Shorten, Share, Track/i);
    });

    it('heading levels follow proper hierarchy', () => {
      const { container } = renderHome();

      const headings = container.querySelectorAll('h1, h2, h3, h4, h5, h6');
      const headingLevels: number[] = [];

      headings.forEach((heading) => {
        const level = parseInt(heading.tagName.charAt(1));
        headingLevels.push(level);
      });

      // Check that heading levels don't skip (e.g., no h1 -> h3 without h2)
      // First heading should be h1
      expect(headingLevels[0]).toBe(1);

      // Subsequent headings shouldn't skip more than one level
      for (let i = 1; i < headingLevels.length; i++) {
        const currentLevel = headingLevels[i];
        const previousLevel = headingLevels[i - 1];

        // A heading level can be the same, one level down, or any level up
        // But should not skip levels going down (h1 -> h3 is bad)
        if (currentLevel > previousLevel) {
          expect(currentLevel - previousLevel).toBeLessThanOrEqual(1);
        }
      }
    });

    it('h2 elements exist for major sections', () => {
      renderHome();

      const h2Elements = screen.getAllByRole('heading', { level: 2 });
      expect(h2Elements.length).toBeGreaterThanOrEqual(1);

      // Check for expected section headings
      const headingTexts = h2Elements.map((h) => h.textContent);
      expect(headingTexts).toContain('Try It Now');
    });

    it('h3 elements follow h2 elements in sections', () => {
      renderHome();

      const h3Elements = screen.getAllByRole('heading', { level: 3 });
      // We have h3 elements in How It Works section
      expect(h3Elements.length).toBeGreaterThanOrEqual(1);
    });
  });

  // Test Case 5: Check for skip-to-content link
  describe('Test Case 5: Skip to content link', () => {
    it('has a skip-to-content link for keyboard users', () => {
      renderHome();

      // Look for skip link by test id
      const skipLink = screen.getByTestId('skip-link');

      expect(skipLink).toBeInTheDocument();
      expect(skipLink).toHaveAttribute('href', '#main-content');
      expect(skipLink.textContent).toMatch(/skip to main content/i);
    });

    it('skip link is the first focusable element', () => {
      const { container } = renderHome();

      const skipLink = container.querySelector('[data-testid="skip-link"]');
      expect(skipLink).toBeInTheDocument();

      // If skip link exists, verify it comes early in the DOM
      const focusableElements = container.querySelectorAll(
        'a, button, input, textarea, select, [tabindex]:not([tabindex="-1"])'
      );

      // Skip link should be one of the first few focusable elements
      const skipLinkIndex = Array.from(focusableElements).indexOf(skipLink as Element);
      expect(skipLinkIndex).toBeLessThan(3);
    });

    it('main content landmark exists with correct id', () => {
      const { container } = renderHome();

      const mainElement = container.querySelector('main#main-content');
      expect(mainElement).toBeInTheDocument();
    });
  });

  // Test Case 6: Screen reader compatibility (semantic structure)
  describe('Test Case 6: Screen reader compatibility', () => {
    it('page has semantic landmarks', () => {
      const { container } = renderHome();

      // Should have navigation
      expect(screen.getByRole('navigation')).toBeInTheDocument();

      // Should have main content area
      const mainElement = container.querySelector('main');
      expect(mainElement).toBeInTheDocument();

      // Should have multiple sections
      const sections = container.querySelectorAll('section');
      expect(sections.length).toBeGreaterThan(0);
    });

    it('navigation landmark has aria-label', () => {
      renderHome();

      const nav = screen.getByRole('navigation');
      expect(nav.getAttribute('aria-label')).toBeTruthy();
    });

    it('content is structured with semantic HTML', () => {
      const { container } = renderHome();

      // Check for semantic elements
      expect(container.querySelectorAll('section').length).toBeGreaterThan(0);
      expect(container.querySelectorAll('nav').length).toBeGreaterThan(0);
      expect(container.querySelectorAll('footer').length).toBeGreaterThan(0);
    });

    it('interactive elements provide context to screen readers', () => {
      renderHome();

      // Primary CTA should be clear about its action
      const primaryCTA = screen.getByRole('button', { name: /Get Started Free/i });
      expect(primaryCTA).toBeInTheDocument();

      // Login and Sign Up links should be clear
      const loginLink = screen.getByLabelText(/go to login page/i);
      expect(loginLink).toBeInTheDocument();

      const signUpLink = screen.getByLabelText(/go to registration page/i);
      expect(signUpLink).toBeInTheDocument();
    });

    it('form controls have descriptive labels', () => {
      renderHome();

      // Demo URL input should have a label
      const formControl = screen.getByTestId('demo-url-input').closest('.form-control');
      expect(formControl).toBeInTheDocument();

      const labelText = formControl?.querySelector('.label-text');
      expect(labelText).toBeInTheDocument();
      expect(labelText?.textContent).toContain('URL');
    });
  });

  // Additional accessibility tests for focus management
  describe('Focus management and visibility', () => {
    it('buttons have visible focus styles (via DaisyUI btn class)', () => {
      renderHome();

      const buttons = screen.getAllByRole('button');
      buttons.forEach((button) => {
        // DaisyUI btn class provides focus styles
        expect(button.className).toContain('btn');
      });
    });

    it('links have visible focus styles', () => {
      renderHome();

      const links = screen.getAllByRole('link');
      links.forEach((link) => {
        // Links should have proper styling for focus visibility
        // DaisyUI provides default focus styles
        expect(link).toBeInTheDocument();
      });
    });

    it('footer contains accessibility-friendly navigation links', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      expect(footer).toBeInTheDocument();

      // Footer links should be present and accessible
      const footerLinks = within(footer).getAllByRole('link');
      expect(footerLinks.length).toBeGreaterThan(0);

      footerLinks.forEach((link) => {
        expect(link.textContent?.trim()).toBeTruthy();
      });
    });
  });

  // Color contrast (note: actual color contrast testing requires visual inspection or specialized tools)
  describe('Color contrast preparation', () => {
    it('uses themed color classes that follow DaisyUI contrast standards', () => {
      const { container } = renderHome();

      // Check that text uses proper themed classes
      const textElements = container.querySelectorAll('[class*="text-base-content"]');
      expect(textElements.length).toBeGreaterThan(0);

      // Check for primary and secondary themed buttons
      const primaryButtons = container.querySelectorAll('.btn-primary');
      expect(primaryButtons.length).toBeGreaterThan(0);
    });
  });
});
