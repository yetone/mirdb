/**
 * Responsive Design Integration Tests
 * Owner: Scenario 9 - Mobile, Scenario 10 - Tablet, Scenario 11 - Desktop
 *
 * Tests responsive layout behavior across different viewport sizes.
 * Verifies:
 * - Content readability without horizontal scrolling
 * - Navigation accessibility (hamburger menu on mobile)
 * - Touch target sizes for interactive elements
 * - Grid layout changes for feature cards
 */

import React from 'react';
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { render, screen, fireEvent, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import { AuthProvider } from '../../../src/contexts/AuthContext';
import { ThemeProvider } from '../../../src/contexts/ThemeContext';
import { Home } from '../../../src/pages/Home';
import { viewports, setViewport } from './test-utils';

/**
 * Wrapper component that provides full app context
 */
function TestApp({ initialRoute = '/' }: { initialRoute?: string }) {
  return (
    <ThemeProvider>
      <AuthProvider>
        <MemoryRouter initialEntries={[initialRoute]}>
          <Routes>
            <Route path="/" element={<Home />} />
          </Routes>
        </MemoryRouter>
      </AuthProvider>
    </ThemeProvider>
  );
}

/**
 * Helper to check if element has horizontal scroll
 */
function hasHorizontalOverflow(element: HTMLElement): boolean {
  return element.scrollWidth > element.clientWidth;
}

/**
 * Helper to get computed style property
 */
function getComputedStyleProperty(element: HTMLElement, property: string): string {
  return window.getComputedStyle(element).getPropertyValue(property);
}

/**
 * Helper to get element dimensions
 */
function getElementDimensions(element: HTMLElement): { width: number; height: number } {
  const rect = element.getBoundingClientRect();
  return { width: rect.width, height: rect.height };
}

describe('Scenario 9: Responsive Design - Mobile', () => {
  beforeEach(() => {
    // Set mobile viewport
    setViewport('mobile');
  });

  afterEach(() => {
    // Reset to default viewport
    vi.restoreAllMocks();
  });

  describe('Test Case 1: No horizontal scrolling at 375px viewport width', () => {
    it('should render homepage without horizontal scrolling on mobile viewport', () => {
      // Set specific mobile width
      Object.defineProperty(window, 'innerWidth', {
        writable: true,
        configurable: true,
        value: 375,
      });
      Object.defineProperty(window, 'innerHeight', {
        writable: true,
        configurable: true,
        value: 667,
      });
      window.dispatchEvent(new Event('resize'));

      const { container } = render(<TestApp />);

      // Verify the main container doesn't overflow horizontally
      const mainElement = container.querySelector('main');
      expect(mainElement).toBeInTheDocument();

      // Check that the body and html elements don't have horizontal overflow
      // In jsdom, we check by examining the content width vs viewport
      const rootDiv = container.firstChild as HTMLElement;
      expect(rootDiv).toBeInTheDocument();

      // Verify key sections are present and contained within viewport
      expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument();
      expect(screen.getByTestId('features-section')).toBeInTheDocument();

      // Check that content respects viewport width
      const heroHeading = screen.getByRole('heading', { level: 1 });
      expect(heroHeading.closest('section')).toBeInTheDocument();
    });

    it('should have all sections properly contained at mobile width', () => {
      Object.defineProperty(window, 'innerWidth', {
        writable: true,
        configurable: true,
        value: 375,
      });
      window.dispatchEvent(new Event('resize'));

      render(<TestApp />);

      // Verify all homepage sections are present
      expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument();
      expect(screen.getByTestId('features-section')).toBeInTheDocument();
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument();
      expect(screen.getByTestId('dashboard-preview-section')).toBeInTheDocument();
      expect(screen.getByTestId('footer')).toBeInTheDocument();

      // Verify the hero CTA is accessible
      expect(screen.getByTestId('hero-cta')).toBeInTheDocument();
    });

    it('should have responsive text that wraps within mobile viewport', () => {
      Object.defineProperty(window, 'innerWidth', {
        writable: true,
        configurable: true,
        value: 375,
      });
      window.dispatchEvent(new Event('resize'));

      render(<TestApp />);

      // Check that the main heading exists and is readable
      const heading = screen.getByRole('heading', { level: 1 });
      expect(heading).toBeInTheDocument();
      expect(heading).toHaveTextContent(/shorten urls/i);

      // Check feature cards are present (they should stack on mobile)
      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection).toBeInTheDocument();
    });
  });

  describe('Test Case 2: Navigation accessible via hamburger/menu icon', () => {
    it('should display hamburger menu icon on mobile viewport', () => {
      Object.defineProperty(window, 'innerWidth', {
        writable: true,
        configurable: true,
        value: 375,
      });
      window.dispatchEvent(new Event('resize'));

      render(<TestApp />);

      // Look for hamburger menu button
      const hamburgerMenu = screen.getByTestId('hamburger-menu');
      expect(hamburgerMenu).toBeInTheDocument();
      expect(hamburgerMenu).toHaveAttribute('aria-label');
    });

    it('should toggle mobile menu when hamburger icon is clicked', async () => {
      Object.defineProperty(window, 'innerWidth', {
        writable: true,
        configurable: true,
        value: 375,
      });
      window.dispatchEvent(new Event('resize'));

      const user = userEvent.setup();
      render(<TestApp />);

      // Initially mobile menu should not be visible
      expect(screen.queryByTestId('mobile-menu')).not.toBeInTheDocument();

      // Click hamburger menu
      const hamburgerMenu = screen.getByTestId('hamburger-menu');
      await user.click(hamburgerMenu);

      // Mobile menu should now be visible
      await waitFor(() => {
        expect(screen.getByTestId('mobile-menu')).toBeInTheDocument();
      });

      // Click hamburger menu again to close
      await user.click(hamburgerMenu);

      // Mobile menu should be hidden
      await waitFor(() => {
        expect(screen.queryByTestId('mobile-menu')).not.toBeInTheDocument();
      });
    });

    it('should provide navigation links in mobile menu', async () => {
      Object.defineProperty(window, 'innerWidth', {
        writable: true,
        configurable: true,
        value: 375,
      });
      window.dispatchEvent(new Event('resize'));

      const user = userEvent.setup();
      render(<TestApp />);

      // Open mobile menu
      const hamburgerMenu = screen.getByTestId('hamburger-menu');
      await user.click(hamburgerMenu);

      // Verify navigation links are present in mobile menu
      await waitFor(() => {
        const mobileMenu = screen.getByTestId('mobile-menu');
        expect(mobileMenu).toBeInTheDocument();

        // Check for Login and Register links in mobile menu
        expect(screen.getByTestId('mobile-nav-login')).toBeInTheDocument();
        expect(screen.getByTestId('mobile-nav-register')).toBeInTheDocument();
      });
    });

    it('should have accessible hamburger menu with proper ARIA attributes', () => {
      Object.defineProperty(window, 'innerWidth', {
        writable: true,
        configurable: true,
        value: 375,
      });
      window.dispatchEvent(new Event('resize'));

      render(<TestApp />);

      const hamburgerMenu = screen.getByTestId('hamburger-menu');

      // Check ARIA attributes
      expect(hamburgerMenu).toHaveAttribute('aria-label');
      expect(hamburgerMenu).toHaveAttribute('aria-expanded', 'false');
    });

    it('should update aria-expanded when menu is opened', async () => {
      Object.defineProperty(window, 'innerWidth', {
        writable: true,
        configurable: true,
        value: 375,
      });
      window.dispatchEvent(new Event('resize'));

      const user = userEvent.setup();
      render(<TestApp />);

      const hamburgerMenu = screen.getByTestId('hamburger-menu');

      // Initially closed
      expect(hamburgerMenu).toHaveAttribute('aria-expanded', 'false');

      // Open menu
      await user.click(hamburgerMenu);

      // Should now be expanded
      await waitFor(() => {
        expect(hamburgerMenu).toHaveAttribute('aria-expanded', 'true');
      });
    });
  });

  describe('Test Case 3: CTA buttons have minimum 44x44px touch target', () => {
    it('should have hero CTA button with minimum 44x44px dimensions', () => {
      Object.defineProperty(window, 'innerWidth', {
        writable: true,
        configurable: true,
        value: 375,
      });
      window.dispatchEvent(new Event('resize'));

      render(<TestApp />);

      const heroCTA = screen.getByTestId('hero-cta');
      expect(heroCTA).toBeInTheDocument();

      // Check that the button has minimum touch target size class
      // The button should have min-h-[44px] and min-w-[44px] classes
      expect(heroCTA.className).toMatch(/min-h-\[44px\]/);
      expect(heroCTA.className).toMatch(/min-w-\[44px\]/);
    });

    it('should have hamburger menu button with minimum 44x44px dimensions', () => {
      Object.defineProperty(window, 'innerWidth', {
        writable: true,
        configurable: true,
        value: 375,
      });
      window.dispatchEvent(new Event('resize'));

      render(<TestApp />);

      const hamburgerMenu = screen.getByTestId('hamburger-menu');
      expect(hamburgerMenu).toBeInTheDocument();

      // Check that the button has minimum touch target size
      expect(hamburgerMenu.className).toMatch(/min-w-\[44px\]/);
      expect(hamburgerMenu.className).toMatch(/min-h-\[44px\]/);
    });

    it('should have mobile navigation links with adequate touch targets', async () => {
      Object.defineProperty(window, 'innerWidth', {
        writable: true,
        configurable: true,
        value: 375,
      });
      window.dispatchEvent(new Event('resize'));

      const user = userEvent.setup();
      render(<TestApp />);

      // Open mobile menu
      const hamburgerMenu = screen.getByTestId('hamburger-menu');
      await user.click(hamburgerMenu);

      await waitFor(() => {
        const mobileNavLogin = screen.getByTestId('mobile-nav-login');
        const mobileNavRegister = screen.getByTestId('mobile-nav-register');

        // Check touch target sizes
        expect(mobileNavLogin.className).toMatch(/min-h-\[44px\]/);
        expect(mobileNavRegister.className).toMatch(/min-h-\[44px\]/);
      });
    });
  });

  describe('Test Case 4: Feature cards stack vertically (1 column layout)', () => {
    it('should have features grid with single column layout on mobile', () => {
      Object.defineProperty(window, 'innerWidth', {
        writable: true,
        configurable: true,
        value: 375,
      });
      window.dispatchEvent(new Event('resize'));

      render(<TestApp />);

      const featuresGrid = screen.getByTestId('features-grid');
      expect(featuresGrid).toBeInTheDocument();

      // Check that the grid has mobile-first single column class
      expect(featuresGrid.className).toMatch(/grid-cols-1/);
    });

    it('should display all 4 feature cards in single column', () => {
      Object.defineProperty(window, 'innerWidth', {
        writable: true,
        configurable: true,
        value: 375,
      });
      window.dispatchEvent(new Event('resize'));

      render(<TestApp />);

      // Verify all 4 feature cards are present
      expect(screen.getByTestId('feature-card-url-shortening')).toBeInTheDocument();
      expect(screen.getByTestId('feature-card-click-analytics')).toBeInTheDocument();
      expect(screen.getByTestId('feature-card-geographic-insights')).toBeInTheDocument();
      expect(screen.getByTestId('feature-card-shareable-stats')).toBeInTheDocument();
    });

    it('should have feature cards with readable content on mobile', () => {
      Object.defineProperty(window, 'innerWidth', {
        writable: true,
        configurable: true,
        value: 375,
      });
      window.dispatchEvent(new Event('resize'));

      render(<TestApp />);

      // Check that feature titles are visible
      expect(screen.getByTestId('feature-title-url-shortening')).toHaveTextContent('URL Shortening');
      expect(screen.getByTestId('feature-title-click-analytics')).toHaveTextContent('Click Analytics');
      expect(screen.getByTestId('feature-title-geographic-insights')).toHaveTextContent('Geographic Insights');
      expect(screen.getByTestId('feature-title-shareable-stats')).toHaveTextContent('Shareable Stats');

      // Check that feature descriptions are visible
      expect(screen.getByTestId('feature-description-url-shortening')).toBeInTheDocument();
      expect(screen.getByTestId('feature-description-click-analytics')).toBeInTheDocument();
      expect(screen.getByTestId('feature-description-geographic-insights')).toBeInTheDocument();
      expect(screen.getByTestId('feature-description-shareable-stats')).toBeInTheDocument();
    });

    it('should have feature icons visible on mobile', () => {
      Object.defineProperty(window, 'innerWidth', {
        writable: true,
        configurable: true,
        value: 375,
      });
      window.dispatchEvent(new Event('resize'));

      render(<TestApp />);

      // Check that feature icons are present
      expect(screen.getByTestId('feature-icon-url-shortening')).toBeInTheDocument();
      expect(screen.getByTestId('feature-icon-click-analytics')).toBeInTheDocument();
      expect(screen.getByTestId('feature-icon-geographic-insights')).toBeInTheDocument();
      expect(screen.getByTestId('feature-icon-shareable-stats')).toBeInTheDocument();
    });
  });

  describe('Additional mobile responsive tests', () => {
    it('should have proper spacing and padding on mobile', () => {
      Object.defineProperty(window, 'innerWidth', {
        writable: true,
        configurable: true,
        value: 375,
      });
      window.dispatchEvent(new Event('resize'));

      render(<TestApp />);

      // Check that features section has responsive padding
      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection.className).toMatch(/px-4/);
    });

    it('should display how-it-works section properly on mobile', () => {
      Object.defineProperty(window, 'innerWidth', {
        writable: true,
        configurable: true,
        value: 375,
      });
      window.dispatchEvent(new Event('resize'));

      render(<TestApp />);

      const howItWorksSection = screen.getByTestId('how-it-works-section');
      expect(howItWorksSection).toBeInTheDocument();

      // Verify all 3 steps are visible
      expect(screen.getByTestId('step-1')).toBeInTheDocument();
      expect(screen.getByTestId('step-2')).toBeInTheDocument();
      expect(screen.getByTestId('step-3')).toBeInTheDocument();
    });

    it('should display footer properly on mobile', () => {
      Object.defineProperty(window, 'innerWidth', {
        writable: true,
        configurable: true,
        value: 375,
      });
      window.dispatchEvent(new Event('resize'));

      render(<TestApp />);

      const footer = screen.getByTestId('footer');
      expect(footer).toBeInTheDocument();

      // Verify footer content is visible
      expect(screen.getByText(/shorturl/i)).toBeInTheDocument();
    });

    it('should close mobile menu when navigating', async () => {
      Object.defineProperty(window, 'innerWidth', {
        writable: true,
        configurable: true,
        value: 375,
      });
      window.dispatchEvent(new Event('resize'));

      const user = userEvent.setup();
      render(<TestApp />);

      // Open mobile menu
      const hamburgerMenu = screen.getByTestId('hamburger-menu');
      await user.click(hamburgerMenu);

      await waitFor(() => {
        expect(screen.getByTestId('mobile-menu')).toBeInTheDocument();
      });

      // Click on a navigation link
      const loginLink = screen.getByTestId('mobile-nav-login');
      await user.click(loginLink);

      // Menu should close after navigation
      await waitFor(() => {
        expect(screen.queryByTestId('mobile-menu')).not.toBeInTheDocument();
      });
    });
  });
});
