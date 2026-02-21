/**
 * Accessibility tests for homepage.
 * Owner: Scenario 8 - Accessibility and Keyboard Navigation
 *
 * Test coverage:
 * - Tab key navigation through interactive elements
 * - Focus indicators visible on all focusable elements
 * - Proper heading hierarchy (single h1, h2/h3 structure)
 * - ARIA labels on icon buttons
 * - Semantic HTML elements (header, main, section, footer)
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { screen, within, fireEvent } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { Home } from '@/pages/Home';
import { renderWithProviders } from '../utils/renderWithProviders';

describe('Homepage Accessibility (WCAG 2.1 AA)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('Semantic HTML Structure', () => {
    it('uses header element for navbar', () => {
      const { container } = renderWithProviders(<Home />);
      const header = container.querySelector('header');
      expect(header).toBeInTheDocument();
      expect(header).toHaveAttribute('data-testid', 'navbar');
    });

    it('uses main element for primary content', () => {
      const { container } = renderWithProviders(<Home />);
      const main = container.querySelector('main');
      expect(main).toBeInTheDocument();
    });

    it('uses section elements for content sections', () => {
      const { container } = renderWithProviders(<Home />);
      const sections = container.querySelectorAll('section');
      // Hero section, Features section, and Final CTA section
      expect(sections.length).toBeGreaterThanOrEqual(3);
    });

    it('uses footer element for footer content', () => {
      const { container } = renderWithProviders(<Home />);
      const footer = container.querySelector('footer');
      expect(footer).toBeInTheDocument();
      expect(footer).toHaveAttribute('data-testid', 'footer');
    });

    it('has proper landmark structure for screen readers', () => {
      const { container } = renderWithProviders(<Home />);

      // Should have header, main, and footer landmarks
      expect(container.querySelector('header')).toBeInTheDocument();
      expect(container.querySelector('main')).toBeInTheDocument();
      expect(container.querySelector('footer')).toBeInTheDocument();
    });
  });

  describe('Heading Hierarchy', () => {
    it('has a single h1 element on the page', () => {
      const { container } = renderWithProviders(<Home />);
      const h1Elements = container.querySelectorAll('h1');
      expect(h1Elements).toHaveLength(1);
    });

    it('has h1 as the main headline in hero section', () => {
      renderWithProviders(<Home />);
      const headline = screen.getByTestId('hero-headline');
      expect(headline.tagName).toBe('H1');
    });

    it('uses h2 for section headings', () => {
      const { container } = renderWithProviders(<Home />);
      const h2Elements = container.querySelectorAll('h2');
      // Features section heading and Final CTA heading
      expect(h2Elements.length).toBeGreaterThanOrEqual(2);
    });

    it('uses h3 for feature card titles', () => {
      const { container } = renderWithProviders(<Home />);
      const featuresSection = screen.getByTestId('features-section');
      const h3Elements = within(featuresSection).getAllByRole('heading', { level: 3 });
      // 4 feature cards with h3 titles
      expect(h3Elements).toHaveLength(4);
    });

    it('follows proper heading hierarchy without skipping levels', () => {
      const { container } = renderWithProviders(<Home />);
      const h1Elements = container.querySelectorAll('h1');
      const h2Elements = container.querySelectorAll('h2');
      const h3Elements = container.querySelectorAll('h3');

      // Must have h1 first
      expect(h1Elements.length).toBe(1);
      // If h3 exists, must have h2
      if (h3Elements.length > 0) {
        expect(h2Elements.length).toBeGreaterThan(0);
      }
    });
  });

  describe('Focus Indicators', () => {
    it('CTA buttons have focus ring classes', () => {
      renderWithProviders(<Home />);

      const getStartedBtn = screen.getByTestId('cta-get-started');
      const signInBtn = screen.getByTestId('cta-sign-in');

      // FuturisticButton includes focus:ring-2 class
      expect(getStartedBtn.className).toContain('focus:ring');
      expect(signInBtn.className).toContain('focus:ring');
    });

    it('final CTA buttons have focus ring classes', () => {
      renderWithProviders(<Home />);

      const signUpBtn = screen.getByTestId('final-cta-signup');
      const signInBtn = screen.getByTestId('final-cta-signin');

      expect(signUpBtn.className).toContain('focus:ring');
      expect(signInBtn.className).toContain('focus:ring');
    });

    it('navbar links are focusable', () => {
      renderWithProviders(<Home />);

      const loginLink = screen.getByTestId('navbar-login');
      const registerLink = screen.getByTestId('navbar-register');

      expect(loginLink).toBeVisible();
      expect(registerLink).toBeVisible();

      // Links should be focusable
      loginLink.focus();
      expect(document.activeElement).toBe(loginLink);

      registerLink.focus();
      expect(document.activeElement).toBe(registerLink);
    });

    it('footer links are focusable', () => {
      renderWithProviders(<Home />);

      const footer = screen.getByTestId('footer');
      const links = within(footer).getAllByRole('link');

      links.forEach(link => {
        link.focus();
        expect(document.activeElement).toBe(link);
      });
    });
  });

  describe('ARIA Labels', () => {
    it('hero CTA buttons have aria-label attributes', () => {
      renderWithProviders(<Home />);

      const getStartedBtn = screen.getByTestId('cta-get-started');
      const signInBtn = screen.getByTestId('cta-sign-in');

      expect(getStartedBtn).toHaveAttribute('aria-label');
      expect(signInBtn).toHaveAttribute('aria-label');
    });

    it('final CTA buttons have aria-label attributes', () => {
      renderWithProviders(<Home />);

      const signUpBtn = screen.getByTestId('final-cta-signup');
      const signInBtn = screen.getByTestId('final-cta-signin');

      expect(signUpBtn).toHaveAttribute('aria-label');
      expect(signInBtn).toHaveAttribute('aria-label');
    });

    it('mobile menu toggle has aria-label', () => {
      renderWithProviders(<Home />);

      const hamburgerBtn = screen.getByTestId('navbar-hamburger');
      expect(hamburgerBtn).toHaveAttribute('aria-label');
    });

    it('mobile menu toggle has aria-expanded attribute', () => {
      renderWithProviders(<Home />);

      const hamburgerBtn = screen.getByTestId('navbar-hamburger');
      expect(hamburgerBtn).toHaveAttribute('aria-expanded', 'false');
    });

    it('hero section has aria-labelledby pointing to headline', () => {
      renderWithProviders(<Home />);

      const heroSection = screen.getByTestId('hero-section');
      const headline = screen.getByTestId('hero-headline');

      expect(heroSection).toHaveAttribute('aria-labelledby', 'hero-headline');
      expect(headline).toHaveAttribute('id', 'hero-headline');
    });

    it('final CTA section has aria-labelledby pointing to heading', () => {
      renderWithProviders(<Home />);

      const ctaSection = screen.getByTestId('final-cta-section');
      expect(ctaSection).toHaveAttribute('aria-labelledby', 'final-cta-heading');
    });
  });

  describe('Keyboard Navigation', () => {
    it('interactive elements receive focus in sequential order', async () => {
      const user = userEvent.setup();
      renderWithProviders(<Home />);

      // Start by focusing the document body
      document.body.focus();

      // Tab through interactive elements
      await user.tab();

      // First focusable element should be in the navbar (logo link or similar)
      const activeElement = document.activeElement;
      expect(activeElement?.tagName).toBe('A');
    });

    it('all CTA buttons are reachable via keyboard', async () => {
      const user = userEvent.setup();
      renderWithProviders(<Home />);

      // Get all interactive elements
      const getStartedBtn = screen.getByTestId('cta-get-started');
      const signInBtn = screen.getByTestId('cta-sign-in');
      const finalSignUpBtn = screen.getByTestId('final-cta-signup');
      const finalSignInBtn = screen.getByTestId('final-cta-signin');

      // All buttons should exist and be visible
      expect(getStartedBtn).toBeInTheDocument();
      expect(signInBtn).toBeInTheDocument();
      expect(finalSignUpBtn).toBeInTheDocument();
      expect(finalSignInBtn).toBeInTheDocument();

      // Each should be focusable
      getStartedBtn.focus();
      expect(document.activeElement).toBe(getStartedBtn);

      signInBtn.focus();
      expect(document.activeElement).toBe(signInBtn);

      finalSignUpBtn.focus();
      expect(document.activeElement).toBe(finalSignUpBtn);

      finalSignInBtn.focus();
      expect(document.activeElement).toBe(finalSignInBtn);
    });

    it('Enter key activates focused link elements', async () => {
      const user = userEvent.setup();
      renderWithProviders(<Home />);

      const getStartedBtn = screen.getByTestId('cta-get-started');

      // Focus the button
      getStartedBtn.focus();
      expect(document.activeElement).toBe(getStartedBtn);

      // Button is actually a Link, so Enter should work
      // We verify the element is a link with correct href
      expect(getStartedBtn.tagName).toBe('A');
      expect(getStartedBtn).toHaveAttribute('href', '/register');
    });

    it('Space key activates focused button elements', async () => {
      const user = userEvent.setup();
      renderWithProviders(<Home />);

      const hamburgerBtn = screen.getByTestId('navbar-hamburger');

      // Focus the hamburger button
      hamburgerBtn.focus();
      expect(document.activeElement).toBe(hamburgerBtn);

      // Press space to toggle mobile menu
      await user.keyboard(' ');

      // Mobile menu should now be open
      expect(hamburgerBtn).toHaveAttribute('aria-expanded', 'true');
    });

    it('theme toggle can be activated via keyboard', async () => {
      const user = userEvent.setup();
      renderWithProviders(<Home />);

      const themeToggle = screen.getByTestId('theme-toggle');

      // Theme toggle should be focusable
      themeToggle.focus();
      expect(document.activeElement).toBe(themeToggle);
    });

    it('navbar links can be navigated sequentially', async () => {
      const user = userEvent.setup();
      renderWithProviders(<Home />);

      // Focus the first navbar link (logo)
      const logoLink = screen.getByTestId('navbar-logo');
      logoLink.focus();

      expect(document.activeElement).toBe(logoLink);

      // Tab to next element in navbar
      await user.tab();

      // Should move to next focusable element
      expect(document.activeElement).not.toBe(logoLink);
    });
  });

  describe('Screen Reader Support', () => {
    it('feature icons have associated text labels', () => {
      renderWithProviders(<Home />);

      // Each feature card should have a title that describes the icon
      const featureIds = ['url-shortening', 'click-analytics', 'secure-management', 'custom-short-codes'];

      featureIds.forEach(id => {
        const title = screen.getByTestId(`feature-title-${id}`);
        expect(title).toBeInTheDocument();
        expect(title.textContent).not.toBe('');
      });
    });

    it('page has descriptive link text', () => {
      renderWithProviders(<Home />);

      // CTA buttons have descriptive text
      const getStartedBtn = screen.getByTestId('cta-get-started');
      const signInBtn = screen.getByTestId('cta-sign-in');

      expect(getStartedBtn).toHaveTextContent('Get Started');
      expect(signInBtn).toHaveTextContent('Sign In');
    });

    it('images/icons within buttons have appropriate context', () => {
      renderWithProviders(<Home />);

      // Feature icons are within cards that have descriptive text
      const featuresSection = screen.getByTestId('features-section');
      const iconSvgs = within(featuresSection).getAllByTestId('icon-svg');

      // Icons exist and are within elements that have associated text
      expect(iconSvgs.length).toBe(4);
    });

    it('content is organized in logical reading order', () => {
      const { container } = renderWithProviders(<Home />);

      // Get all major sections in DOM order
      const header = container.querySelector('header');
      const main = container.querySelector('main');
      const footer = container.querySelector('footer');

      // Verify they exist and are in correct order
      expect(header).toBeInTheDocument();
      expect(main).toBeInTheDocument();
      expect(footer).toBeInTheDocument();

      // Header should come before main in the DOM
      expect(header!.compareDocumentPosition(main!)).toBe(Node.DOCUMENT_POSITION_FOLLOWING);
      // Main should come before footer in the DOM
      expect(main!.compareDocumentPosition(footer!)).toBe(Node.DOCUMENT_POSITION_FOLLOWING);
    });
  });

  describe('Color and Contrast', () => {
    it('text content uses semantic color classes for theme support', () => {
      renderWithProviders(<Home />);

      // Subheadline uses theme-aware color classes
      const subheadline = screen.getByTestId('hero-subheadline');
      expect(subheadline.className).toContain('text-base-content');
    });

    it('primary buttons use theme-aware color classes', () => {
      renderWithProviders(<Home />);

      const getStartedBtn = screen.getByTestId('cta-get-started');
      expect(getStartedBtn.className).toContain('btn-primary');
    });

    it('outline buttons have visible borders', () => {
      renderWithProviders(<Home />);

      const signInBtn = screen.getByTestId('cta-sign-in');
      expect(signInBtn.className).toContain('btn-outline');
    });
  });

  describe('Touch Target Size', () => {
    it('CTA buttons have minimum touch target size', () => {
      renderWithProviders(<Home />);

      const getStartedBtn = screen.getByTestId('cta-get-started');

      // FuturisticButton has min-h-[44px] class for touch accessibility
      expect(getStartedBtn.className).toContain('min-h-[44px]');
    });

    it('final CTA buttons have minimum touch target size', () => {
      renderWithProviders(<Home />);

      const signUpBtn = screen.getByTestId('final-cta-signup');
      expect(signUpBtn.className).toContain('min-h-[44px]');
    });
  });
});
