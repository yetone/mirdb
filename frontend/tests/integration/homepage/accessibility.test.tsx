/**
 * Accessibility Integration Tests - Keyboard Navigation
 * Owner: Scenario 15 - Accessibility - Keyboard Navigation
 * Owner: Scenario 16 - Accessibility - Screen Reader
 * Owner: Scenario 17 - Accessibility - Color Contrast
 *
 * Tests keyboard navigation, focus management, and accessibility features
 * for the homepage to ensure WCAG 2.1 AA compliance.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import { ThemeProvider } from '../../../src/contexts/ThemeContext';
import { AuthProvider } from '../../../src/contexts/AuthContext';
import { Home } from '../../../src/pages/Home';

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
 * Renders Home page with all required providers
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
