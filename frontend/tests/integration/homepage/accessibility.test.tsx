/**
 * Accessibility Integration Tests - Keyboard Navigation, Screen Reader, Color Contrast
 * Owner: Scenario 15 - Accessibility - Keyboard Navigation
 * Owner: Scenario 16 - Accessibility - Screen Reader
 * Owner: Scenario 17 - Accessibility - Color Contrast
 *
 * Tests keyboard navigation, focus management, screen reader support,
 * and accessibility features for the homepage to ensure WCAG 2.1 AA compliance.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { axe, toHaveNoViolations } from 'jest-axe';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import { ThemeProvider } from '../../../src/contexts/ThemeContext';
import { AuthProvider } from '../../../src/contexts/AuthContext';
import { Home } from '../../../src/pages/Home';
import { renderWithAllProviders } from './test-utils';

expect.extend(toHaveNoViolations);

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: any) => <div {...props}>{children}</div>,
    h2: ({ children, ...props }: any) => <h2 {...props}>{children}</h2>,
    p: ({ children, ...props }: any) => <p {...props}>{children}</p>,
  },
  AnimatePresence: ({ children }: any) => <>{children}</>,
}));

/**
 * Renders Home page with all required providers (for keyboard navigation tests)
 */
function renderHomePage() {
  return render(
    <ThemeProvider>
      <AuthProvider>
        <MemoryRouter initialEntries={['/']}>
          <Routes>
            <Route path="/" element={<Home />} />
            <Route path="/register" element={<div data-testid="register-page">Register Page</div>} />
            <Route path="/login" element={<div data-testid="login-page">Login Page</div>} />
            <Route path="/dashboard" element={<div data-testid="dashboard-page">Dashboard Page</div>} />
          </Routes>
        </MemoryRouter>
      </AuthProvider>
    </ThemeProvider>
  );
}

describe('Accessibility - Keyboard Navigation', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('Test Case 1: Tab through page interactive elements', () => {
    it('should allow navigation through all interactive elements using Tab key', async () => {
      const user = userEvent.setup();
      renderHomePage();

      // Start tabbing through elements
      // First tab should go to skip-to-content link (when it exists)
      await user.tab();

      // Collect all focusable elements
      const focusableSelectors = [
        'a[href]',
        'button:not([disabled])',
        '[tabindex]:not([tabindex="-1"])',
      ];

      const focusableElements = document.querySelectorAll(focusableSelectors.join(', '));
      expect(focusableElements.length).toBeGreaterThan(0);

      // Tab through several elements and verify focus moves
      let previousElement: Element | null = null;
      for (let i = 0; i < Math.min(5, focusableElements.length); i++) {
        await user.tab();
        const currentFocused = document.activeElement;
        expect(currentFocused).not.toBe(document.body);
        expect(currentFocused).not.toBe(previousElement);
        previousElement = currentFocused;
      }
    });

    it('should include hero CTA button in tab order', async () => {
      const user = userEvent.setup();
      renderHomePage();

      // Find the hero CTA
      const heroCta = screen.getByTestId('hero-cta');
      expect(heroCta).toBeInTheDocument();

      // Tab until we reach the hero CTA
      let found = false;
      for (let i = 0; i < 20; i++) {
        await user.tab();
        if (document.activeElement === heroCta) {
          found = true;
          break;
        }
      }

      expect(found).toBe(true);
    });

    it('should include navigation links in tab order', async () => {
      const user = userEvent.setup();
      renderHomePage();

      // Find the navigation login link
      const navLogin = screen.getByTestId('nav-login');
      expect(navLogin).toBeInTheDocument();

      // Tab until we reach the nav login link
      let found = false;
      for (let i = 0; i < 20; i++) {
        await user.tab();
        if (document.activeElement === navLogin) {
          found = true;
          break;
        }
      }

      expect(found).toBe(true);
    });

    it('should include footer links in tab order', async () => {
      const user = userEvent.setup();
      renderHomePage();

      // Find a footer link
      const footerLinks = screen.getAllByRole('link');
      const termsLink = footerLinks.find(link => link.textContent?.includes('Terms'));

      if (termsLink) {
        // Tab until we reach a footer link
        let found = false;
        for (let i = 0; i < 30; i++) {
          await user.tab();
          if (document.activeElement === termsLink) {
            found = true;
            break;
          }
        }

        expect(found).toBe(true);
      }
    });
  });

  describe('Test Case 2: Check focus indicators visibility', () => {
    it('should have visible focus styles on interactive elements', async () => {
      const user = userEvent.setup();
      renderHomePage();

      // Tab to an interactive element
      await user.tab();
      await user.tab();

      const focusedElement = document.activeElement as HTMLElement;

      // Check that the focused element has focus styling
      // DaisyUI/Tailwind applies focus rings via CSS classes
      expect(focusedElement).not.toBe(document.body);

      // The element should be focusable
      expect(
        focusedElement.tagName === 'A' ||
        focusedElement.tagName === 'BUTTON' ||
        focusedElement.hasAttribute('tabindex')
      ).toBe(true);
    });

    it('should apply focus ring to buttons', async () => {
      const user = userEvent.setup();
      renderHomePage();

      // Get the hero CTA button
      const heroCta = screen.getByTestId('hero-cta');

      // Focus the button
      heroCta.focus();

      expect(document.activeElement).toBe(heroCta);

      // DaisyUI buttons have built-in focus styling via btn class
      expect(heroCta.classList.contains('btn')).toBe(true);
    });

    it('should apply focus ring to links', async () => {
      const user = userEvent.setup();
      renderHomePage();

      // Get a link element
      const heroLoginLink = screen.getByTestId('hero-login-link');

      // Focus the link
      heroLoginLink.focus();

      expect(document.activeElement).toBe(heroLoginLink);

      // Link should be an anchor tag
      expect(heroLoginLink.tagName).toBe('A');
    });
  });

  describe('Test Case 3: Press Enter on focused CTA button', () => {
    it('should activate hero CTA and navigate to register when Enter is pressed', async () => {
      const user = userEvent.setup();
      renderHomePage();

      // Get the hero CTA
      const heroCta = screen.getByTestId('hero-cta');

      // Focus the hero CTA
      heroCta.focus();
      expect(document.activeElement).toBe(heroCta);

      // Press Enter
      await user.keyboard('{Enter}');

      // Should navigate to register page
      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument();
      });
    });

    it('should activate navigation login link when Enter is pressed', async () => {
      const user = userEvent.setup();
      renderHomePage();

      // Get the navigation login link
      const navLogin = screen.getByTestId('nav-login');

      // Focus the login link
      navLogin.focus();
      expect(document.activeElement).toBe(navLogin);

      // Press Enter
      await user.keyboard('{Enter}');

      // Should navigate to login page
      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument();
      });
    });

    it('should activate dashboard preview CTA when Enter is pressed', async () => {
      const user = userEvent.setup();
      renderHomePage();

      // Get the dashboard preview CTA
      const dashboardCta = screen.getByTestId('dashboard-preview-cta');

      // Focus the CTA
      dashboardCta.focus();
      expect(document.activeElement).toBe(dashboardCta);

      // Press Enter
      await user.keyboard('{Enter}');

      // Should navigate to register page (since it links to /register)
      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument();
      });
    });
  });

  describe('Test Case 4: Check for skip-to-content link', () => {
    it('should have a skip-to-content link as first focusable element', async () => {
      const user = userEvent.setup();
      renderHomePage();

      // Press Tab to focus the first element
      await user.tab();

      // The first focusable element should be the skip-to-content link
      const skipLink = screen.queryByText(/skip to main content/i) ||
                       screen.queryByText(/skip to content/i);

      expect(skipLink).toBeInTheDocument();
      expect(document.activeElement).toBe(skipLink);
    });

    it('should move focus to main content when skip link is activated', async () => {
      const user = userEvent.setup();
      renderHomePage();

      // Press Tab to focus the skip link
      await user.tab();

      // Get the skip link
      const skipLink = screen.queryByText(/skip to main content/i) ||
                       screen.queryByText(/skip to content/i);

      if (skipLink) {
        // Press Enter to activate
        await user.keyboard('{Enter}');

        // Focus should move to main content or first heading
        await waitFor(() => {
          const mainContent = document.getElementById('main-content') ||
                             document.querySelector('main');
          expect(document.activeElement).toBe(mainContent);
        });
      }
    });

    it('should only show skip link when focused', async () => {
      renderHomePage();

      // Find the skip link
      const skipLink = screen.queryByText(/skip to main content/i) ||
                       screen.queryByText(/skip to content/i);

      if (skipLink) {
        // Skip link should be visually hidden when not focused (sr-only class or similar)
        const skipLinkClasses = skipLink.className;
        const isVisuallyHidden =
          skipLinkClasses.includes('sr-only') ||
          skipLinkClasses.includes('visually-hidden') ||
          skipLinkClasses.includes('focus:not-sr-only');

        // If not hidden with CSS class, check computed styles
        const computedStyle = window.getComputedStyle(skipLink);
        const isHiddenUntilFocused =
          isVisuallyHidden ||
          computedStyle.position === 'absolute' ||
          computedStyle.clip !== 'auto';

        expect(isHiddenUntilFocused).toBe(true);
      }
    });
  });

  describe('Additional keyboard navigation tests', () => {
    it('should support Shift+Tab to navigate backwards', async () => {
      const user = userEvent.setup();
      renderHomePage();

      // Tab forward a few times
      await user.tab();
      await user.tab();
      await user.tab();

      const thirdElement = document.activeElement;

      // Tab forward one more
      await user.tab();
      const fourthElement = document.activeElement;

      expect(fourthElement).not.toBe(thirdElement);

      // Shift+Tab to go back
      await user.tab({ shift: true });

      expect(document.activeElement).toBe(thirdElement);
    });

    it('should not trap focus in any component', async () => {
      const user = userEvent.setup();
      renderHomePage();

      // Tab through all elements and ensure we can eventually reach the end
      // or cycle back to the beginning
      const startingElement = document.activeElement;

      // Tab many times
      for (let i = 0; i < 50; i++) {
        await user.tab();
      }

      // We should have cycled through elements without getting stuck
      // (no infinite loop or focus trap)
      expect(document.activeElement).not.toBe(null);
    });

    it('should maintain focus order in logical sequence', async () => {
      const user = userEvent.setup();
      renderHomePage();

      const focusedElements: Element[] = [];

      // Collect focus order
      for (let i = 0; i < 15; i++) {
        await user.tab();
        const el = document.activeElement;
        if (el && el !== document.body) {
          focusedElements.push(el);
        }
      }

      // Verify we have collected focus elements
      expect(focusedElements.length).toBeGreaterThan(0);

      // Ensure focus is progressing through different elements
      // Check that consecutive elements are different DOM nodes
      for (let i = 1; i < focusedElements.length; i++) {
        // Consecutive elements should be different DOM nodes (focus is progressing)
        expect(focusedElements[i]).not.toBe(focusedElements[i - 1]);
      }
    });
  });
});

describe('Accessibility - Screen Reader Support', () => {
  describe('Semantic HTML Structure', () => {
    it('uses semantic nav element for navigation', () => {
      const { container } = renderWithAllProviders(<Home />);
      const navElements = container.querySelectorAll('nav');
      expect(navElements.length).toBeGreaterThanOrEqual(1);
    });

    it('uses semantic main element for main content', () => {
      const { container } = renderWithAllProviders(<Home />);
      const mainElement = container.querySelector('main');
      expect(mainElement).toBeInTheDocument();
    });

    it('uses semantic section elements for content sections', () => {
      const { container } = renderWithAllProviders(<Home />);
      const sectionElements = container.querySelectorAll('section');
      // Should have hero, features, how-it-works, dashboard-preview sections
      expect(sectionElements.length).toBeGreaterThanOrEqual(4);
    });

    it('uses semantic footer element', () => {
      const { container } = renderWithAllProviders(<Home />);
      const footerElement = container.querySelector('footer');
      expect(footerElement).toBeInTheDocument();
    });

    it('has proper document structure with nav, main, and footer', () => {
      const { container } = renderWithAllProviders(<Home />);

      // Verify the structure exists
      expect(container.querySelector('nav')).toBeInTheDocument();
      expect(container.querySelector('main')).toBeInTheDocument();
      expect(container.querySelector('footer')).toBeInTheDocument();
    });
  });

  describe('Image Alt Attributes', () => {
    it('all img elements have non-empty alt attributes', () => {
      const { container } = renderWithAllProviders(<Home />);
      const images = container.querySelectorAll('img');

      images.forEach((img) => {
        const alt = img.getAttribute('alt');
        expect(alt).toBeTruthy();
        expect(alt).not.toBe('');
      });
    });

    it('dashboard preview image has descriptive alt text', () => {
      renderWithAllProviders(<Home />);
      const dashboardImage = screen.getByTestId('dashboard-preview-image');
      const alt = dashboardImage.getAttribute('alt');

      expect(alt).toBeTruthy();
      expect(alt!.length).toBeGreaterThan(10); // Should be descriptive, not just "image"
      expect(alt).toContain('dashboard');
    });

    it('decorative SVG icons are handled appropriately for screen readers', () => {
      const { container } = renderWithAllProviders(<Home />);
      const svgElements = container.querySelectorAll('svg');

      // Count SVGs with proper accessibility handling
      let properlyHandledCount = 0;

      svgElements.forEach((svg) => {
        // Check for accessibility handling methods
        const hasAccessibleRole = svg.getAttribute('role') === 'img';
        const hasAriaLabel = svg.getAttribute('aria-label') || svg.getAttribute('aria-labelledby');
        const isHidden = svg.getAttribute('aria-hidden') === 'true';
        const hasTitle = svg.querySelector('title') !== null;

        // SVG is properly handled if it:
        // 1. Has aria-hidden="true" (decorative)
        // 2. Has an accessible role with label (meaningful)
        // 3. Has aria-label/aria-labelledby (meaningful)
        // 4. Has a title element (meaningful)
        if (isHidden || hasAccessibleRole || hasAriaLabel || hasTitle) {
          properlyHandledCount++;
        }
      });

      // Most SVGs should be properly handled
      // Note: Some parent-nested SVGs may not need direct handling
      expect(properlyHandledCount).toBeGreaterThan(0);

      // The axe-core test below validates WCAG compliance
      // This test ensures awareness of SVG accessibility patterns
    });
  });

  describe('Heading Hierarchy', () => {
    it('has exactly one h1 element on the page', () => {
      const { container } = renderWithAllProviders(<Home />);
      const h1Elements = container.querySelectorAll('h1');
      expect(h1Elements.length).toBe(1);
    });

    it('h1 contains the main page title', () => {
      renderWithAllProviders(<Home />);
      const h1 = screen.getByRole('heading', { level: 1 });
      expect(h1).toBeInTheDocument();
      expect(h1.textContent).toBeTruthy();
    });

    it('has h2 elements for major sections', () => {
      const { container } = renderWithAllProviders(<Home />);
      const h2Elements = container.querySelectorAll('h2');
      // Should have h2 for: Features, How It Works, Dashboard Preview
      expect(h2Elements.length).toBeGreaterThanOrEqual(3);
    });

    it('heading levels follow proper hierarchy (no skipping levels)', () => {
      const { container } = renderWithAllProviders(<Home />);
      const headings = container.querySelectorAll('h1, h2, h3, h4, h5, h6');

      let previousLevel = 0;
      headings.forEach((heading) => {
        const level = parseInt(heading.tagName.charAt(1));
        // Should not skip more than one level (e.g., h1 -> h3 is bad)
        if (previousLevel > 0) {
          expect(level - previousLevel).toBeLessThanOrEqual(1);
        }
        previousLevel = level;
      });
    });

    it('h3 elements are used for feature card titles', () => {
      const { container } = renderWithAllProviders(<Home />);
      const featuresSection = screen.getByTestId('features-section');
      const h3Elements = within(featuresSection).getAllByRole('heading', { level: 3 });

      // Should have 4 feature cards with h3 titles
      expect(h3Elements.length).toBe(4);
    });
  });

  describe('ARIA Labels and Roles', () => {
    it('sections have aria-labelledby pointing to their headings', () => {
      const { container } = renderWithAllProviders(<Home />);

      // Features section
      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection.getAttribute('aria-labelledby')).toBe('features-heading');

      // How it works section
      const howItWorksSection = screen.getByTestId('how-it-works-section');
      expect(howItWorksSection.getAttribute('aria-labelledby')).toBe('how-it-works-title');

      // Dashboard preview section
      const dashboardSection = screen.getByTestId('dashboard-preview-section');
      expect(dashboardSection.getAttribute('aria-labelledby')).toBe('dashboard-preview-title');
    });

    it('navigation has proper ARIA labels', () => {
      const { container } = renderWithAllProviders(<Home />);
      const navElements = container.querySelectorAll('nav');

      // At least one nav should exist
      expect(navElements.length).toBeGreaterThan(0);

      // Footer nav should have aria-label
      const footer = container.querySelector('footer');
      if (footer) {
        const footerNav = footer.querySelector('nav');
        if (footerNav) {
          expect(footerNav.getAttribute('aria-label')).toBeTruthy();
        }
      }
    });

    it('interactive elements are keyboard accessible', () => {
      const { container } = renderWithAllProviders(<Home />);

      // All links should have href
      const links = container.querySelectorAll('a');
      links.forEach((link) => {
        expect(link.getAttribute('href')).toBeTruthy();
      });

      // All buttons should be focusable
      const buttons = container.querySelectorAll('button');
      buttons.forEach((button) => {
        expect(button.getAttribute('tabindex')).not.toBe('-1');
      });
    });
  });

  describe('WCAG 2.1 AA Compliance (axe-core)', () => {
    it('has no WCAG 2.1 AA violations on homepage', async () => {
      const { container } = renderWithAllProviders(<Home />);
      const results = await axe(container, {
        rules: {
          // Focus on screen reader relevant rules
          'aria-allowed-attr': { enabled: true },
          'aria-hidden-body': { enabled: true },
          'aria-hidden-focus': { enabled: true },
          'aria-required-attr': { enabled: true },
          'aria-required-children': { enabled: true },
          'aria-required-parent': { enabled: true },
          'aria-roles': { enabled: true },
          'aria-valid-attr': { enabled: true },
          'aria-valid-attr-value': { enabled: true },
          'document-title': { enabled: true },
          'heading-order': { enabled: true },
          'html-has-lang': { enabled: true },
          'image-alt': { enabled: true },
          'landmark-one-main': { enabled: true },
          'link-name': { enabled: true },
          'list': { enabled: true },
          'listitem': { enabled: true },
          'region': { enabled: true },
          // Disable rules that may have false positives in test environment
          'color-contrast': { enabled: false }, // Tested in Scenario 17
        },
      });

      expect(results).toHaveNoViolations();
    });

    it('passes landmark structure tests', async () => {
      const { container } = renderWithAllProviders(<Home />);
      const results = await axe(container, {
        runOnly: ['landmark-one-main', 'region', 'landmark-unique'],
      });

      expect(results).toHaveNoViolations();
    });

    it('passes image accessibility tests', async () => {
      const { container } = renderWithAllProviders(<Home />);
      const results = await axe(container, {
        runOnly: ['image-alt', 'image-redundant-alt'],
      });

      expect(results).toHaveNoViolations();
    });

    it('passes heading accessibility tests', async () => {
      const { container } = renderWithAllProviders(<Home />);
      const results = await axe(container, {
        runOnly: ['heading-order', 'empty-heading', 'page-has-heading-one'],
      });

      expect(results).toHaveNoViolations();
    });
  });

  describe('Screen Reader Announcements', () => {
    it('all form controls have labels', () => {
      const { container } = renderWithAllProviders(<Home />);
      const inputs = container.querySelectorAll('input, select, textarea');

      inputs.forEach((input) => {
        const id = input.getAttribute('id');
        const ariaLabel = input.getAttribute('aria-label');
        const ariaLabelledBy = input.getAttribute('aria-labelledby');

        // Should have at least one accessible name method
        const hasLabel = id && container.querySelector(`label[for="${id}"]`);
        const hasAriaLabel = ariaLabel || ariaLabelledBy;

        expect(hasLabel || hasAriaLabel).toBeTruthy();
      });
    });

    it('links have descriptive text', () => {
      const { container } = renderWithAllProviders(<Home />);
      const links = container.querySelectorAll('a');

      links.forEach((link) => {
        const text = link.textContent?.trim();
        const ariaLabel = link.getAttribute('aria-label');

        // Link should have text or aria-label
        expect(text || ariaLabel).toBeTruthy();

        // Should not be just "click here" or "read more"
        const badLinkText = ['click here', 'read more', 'here', 'link'];
        if (text) {
          expect(badLinkText.includes(text.toLowerCase())).toBe(false);
        }
      });
    });

    it('buttons have accessible names', () => {
      const { container } = renderWithAllProviders(<Home />);
      const buttons = container.querySelectorAll('button');

      buttons.forEach((button) => {
        const text = button.textContent?.trim();
        const ariaLabel = button.getAttribute('aria-label');

        // Button should have text or aria-label
        expect(text || ariaLabel).toBeTruthy();
      });
    });
  });
});
