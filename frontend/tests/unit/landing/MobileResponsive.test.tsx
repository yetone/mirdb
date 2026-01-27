/**
 * Mobile Responsive Design Tests
 * Owner: Scenario 6 - Responsive Design - Mobile
 *
 * Tests for mobile viewport (320px-767px) responsiveness:
 * 1. No horizontal overflow on the page at 375px
 * 2. Hero content is vertically stacked and readable
 * 3. Feature cards display in single column layout
 * 4. Navigation is accessible and usable on mobile
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { screen, within } from '@testing-library/react';
import { renderWithProviders, setViewport } from './setup';
import Home from '../../../src/pages/Home';
import HeroSection from '../../../src/components/landing/HeroSection';
import FeaturesSection from '../../../src/components/landing/FeaturesSection';

// Mobile viewport width
const MOBILE_WIDTH = 375;
const MOBILE_HEIGHT = 667;

describe('Responsive Design - Mobile (375px)', () => {
  beforeEach(() => {
    // Set mobile viewport before each test
    setViewport(MOBILE_WIDTH, MOBILE_HEIGHT);
  });

  afterEach(() => {
    // Reset viewport after each test
    setViewport(1024, 768);
  });

  /**
   * Test Case 1: No horizontal overflow on the page at 375px viewport width
   * Input: Render LandingPage at 375px viewport width
   * Expected: No horizontal overflow on the page
   */
  describe('Test Case 1: No horizontal overflow', () => {
    it('renders page without horizontal overflow at 375px width', () => {
      const { container } = renderWithProviders(<Home />);

      // Check that the main container has proper overflow handling
      const mainContainer = container.firstChild as HTMLElement;
      expect(mainContainer).toBeInTheDocument();

      // Verify that max-width and overflow styles are appropriate
      const computedStyle = window.getComputedStyle(mainContainer);

      // The page should not have horizontal scrolling
      // Check that overflow-x is not 'scroll' or 'auto' at page level
      expect(computedStyle.overflowX).not.toBe('scroll');
    });

    it('all content sections fit within 375px viewport', () => {
      const { container } = renderWithProviders(<Home />);

      // Check that all sections are present and contained
      const header = screen.getByRole('banner');
      expect(header).toBeInTheDocument();

      const main = screen.getByRole('main');
      expect(main).toBeInTheDocument();

      // Verify elements have responsive classes
      expect(container.querySelector('.min-h-screen')).toBeInTheDocument();
    });

    it('hero section respects max-width constraints', () => {
      renderWithProviders(<HeroSection />);

      // Check that hero section has proper responsive classes
      const section = document.querySelector('section');
      expect(section).toBeInTheDocument();
      expect(section).toHaveClass('px-4');
    });

    it('features section respects max-width constraints', () => {
      renderWithProviders(<FeaturesSection />);

      // Check that features section has proper padding for mobile
      const section = screen.getByTestId('features-section');
      expect(section).toBeInTheDocument();
      expect(section).toHaveClass('px-4');
    });
  });

  /**
   * Test Case 2: Hero content is vertically stacked and readable
   * Input: Check hero section at mobile viewport
   * Expected: Hero content is vertically stacked and readable
   */
  describe('Test Case 2: Hero section vertical stacking', () => {
    it('hero section renders with vertical layout at mobile viewport', () => {
      renderWithProviders(<HeroSection />);

      // Hero should have flex-col for mobile stacking
      const ctaContainer = document.querySelector('.flex.flex-col');
      expect(ctaContainer).toBeInTheDocument();
    });

    it('hero headline is visible and readable', () => {
      renderWithProviders(<HeroSection />);

      const headline = screen.getByRole('heading', { level: 1 });
      expect(headline).toBeInTheDocument();
      expect(headline).toHaveTextContent(/shorten links/i);

      // Check that headline has responsive text sizing
      expect(headline).toHaveClass('text-4xl');
    });

    it('hero subheadline is visible and readable', () => {
      renderWithProviders(<HeroSection />);

      const subheadline = screen.getByText(/create short, powerful links/i);
      expect(subheadline).toBeInTheDocument();

      // Check that subheadline has appropriate text size for mobile
      expect(subheadline).toHaveClass('text-lg');
    });

    it('CTA buttons stack vertically on mobile', () => {
      renderWithProviders(<HeroSection />);

      // Find the CTA container - should have flex-col for mobile
      const ctaContainer = document.querySelector('.flex.flex-col.sm\\:flex-row');
      expect(ctaContainer).toBeInTheDocument();

      // Verify both CTAs are present
      const getStartedButton = screen.getByRole('button', { name: /get started/i });
      const learnMoreButton = screen.getByRole('button', { name: /learn more/i });

      expect(getStartedButton).toBeInTheDocument();
      expect(learnMoreButton).toBeInTheDocument();
    });

    it('hero content is centered and has proper spacing', () => {
      renderWithProviders(<HeroSection />);

      const contentContainer = document.querySelector('.text-center');
      expect(contentContainer).toBeInTheDocument();

      // Check for proper max-width constraint
      expect(document.querySelector('.max-w-4xl')).toBeInTheDocument();
    });
  });

  /**
   * Test Case 3: Feature cards display in single column layout
   * Input: Check feature cards at mobile viewport
   * Expected: Feature cards display in single column layout
   */
  describe('Test Case 3: Feature cards single column', () => {
    it('features grid uses single column on mobile', () => {
      renderWithProviders(<FeaturesSection />);

      const grid = screen.getByTestId('features-grid');
      expect(grid).toBeInTheDocument();

      // Should have grid-cols-1 for mobile (single column)
      expect(grid).toHaveClass('grid-cols-1');
    });

    it('all feature cards are visible on mobile', () => {
      renderWithProviders(<FeaturesSection />);

      const featureCards = screen.getAllByTestId('feature-card');
      expect(featureCards).toHaveLength(4);

      // Each card should be in the document
      featureCards.forEach((card) => {
        expect(card).toBeInTheDocument();
      });
    });

    it('feature cards have full width on mobile', () => {
      renderWithProviders(<FeaturesSection />);

      const grid = screen.getByTestId('features-grid');

      // grid-cols-1 means each card takes full width
      expect(grid).toHaveClass('grid-cols-1');

      // Should use md:grid-cols-2 for tablet breakpoint
      expect(grid).toHaveClass('md:grid-cols-2');
    });

    it('feature card content is readable on mobile', () => {
      renderWithProviders(<FeaturesSection />);

      // Check that all feature titles are visible
      expect(screen.getByText('URL Shortening')).toBeInTheDocument();
      expect(screen.getByText('Click Analytics')).toBeInTheDocument();
      expect(screen.getByText('Dashboard Management')).toBeInTheDocument();
      expect(screen.getByText('Share Stats')).toBeInTheDocument();

      // Check that descriptions are visible
      expect(screen.getByText(/create short, memorable links/i)).toBeInTheDocument();
    });

    it('feature icons are properly sized on mobile', () => {
      renderWithProviders(<FeaturesSection />);

      const featureCards = screen.getAllByTestId('feature-card');

      featureCards.forEach((card) => {
        const iconContainer = within(card).getByTestId('feature-icon');
        expect(iconContainer).toBeInTheDocument();

        // Icon should have proper size classes
        const svg = iconContainer.querySelector('svg');
        expect(svg).toBeInTheDocument();
        expect(svg).toHaveClass('h-10', 'w-10');
      });
    });
  });

  /**
   * Test Case 4: Navigation is accessible and usable on mobile
   * Input: Check navigation at mobile viewport
   * Expected: Navigation is accessible and usable on mobile
   */
  describe('Test Case 4: Navigation accessibility on mobile', () => {
    it('navigation header is visible on mobile', () => {
      renderWithProviders(<Home />);

      const header = screen.getByRole('banner');
      expect(header).toBeInTheDocument();
      expect(header).toHaveClass('navbar');
    });

    it('logo/brand link is accessible on mobile', () => {
      renderWithProviders(<Home />);

      const logoLink = screen.getByRole('link', { name: /url shortener/i });
      expect(logoLink).toBeInTheDocument();
      expect(logoLink).toHaveAttribute('href', '/');
    });

    it('login link is accessible on mobile', () => {
      renderWithProviders(<Home />);

      const loginLink = screen.getByRole('link', { name: /login/i });
      expect(loginLink).toBeInTheDocument();
      expect(loginLink).toHaveAttribute('href', '/login');
    });

    it('get started/register button is accessible on mobile', () => {
      renderWithProviders(<Home />);

      const registerLink = screen.getByRole('link', { name: /get started/i });
      expect(registerLink).toBeInTheDocument();
      expect(registerLink).toHaveAttribute('href', '/register');
    });

    it('navigation has sticky positioning for mobile usability', () => {
      renderWithProviders(<Home />);

      const header = screen.getByRole('banner');
      expect(header).toHaveClass('sticky', 'top-0', 'z-50');
    });

    it('navigation has proper background for visibility', () => {
      renderWithProviders(<Home />);

      const header = screen.getByRole('banner');
      expect(header).toHaveClass('bg-base-100/80', 'backdrop-blur-md');
    });

    it('all navigation elements are clickable on mobile', () => {
      renderWithProviders(<Home />);

      // All links should be in the document and accessible
      const links = screen.getAllByRole('link');
      expect(links.length).toBeGreaterThanOrEqual(3);

      // Verify each link is clickable (has href attribute)
      links.forEach((link) => {
        expect(link).toHaveAttribute('href');
      });
    });

    it('navigation elements have appropriate touch target sizes', () => {
      renderWithProviders(<Home />);

      // The buttons should have btn class for proper sizing
      const loginLink = screen.getByRole('link', { name: /login/i });
      expect(loginLink).toHaveClass('btn');

      const getStartedLink = screen.getByRole('link', { name: /get started/i });
      expect(getStartedLink).toHaveClass('btn');
    });
  });

  /**
   * Additional mobile responsiveness tests
   */
  describe('Additional Mobile Tests', () => {
    it('page renders without errors at 375px', () => {
      expect(() => renderWithProviders(<Home />)).not.toThrow();
    });

    it('footer is accessible on mobile', () => {
      renderWithProviders(<Home />);

      const footer = screen.getByRole('contentinfo');
      expect(footer).toBeInTheDocument();
    });

    it('main content area is scrollable on mobile', () => {
      renderWithProviders(<Home />);

      const main = screen.getByRole('main');
      expect(main).toBeInTheDocument();
    });

    it('page uses min-h-screen for full viewport height', () => {
      const { container } = renderWithProviders(<Home />);

      expect(container.querySelector('.min-h-screen')).toBeInTheDocument();
    });

    it('responsive text sizes are applied', () => {
      renderWithProviders(<HeroSection />);

      const headline = screen.getByRole('heading', { level: 1 });

      // Should have responsive text size classes
      expect(headline.className).toMatch(/text-4xl|md:text-5xl|lg:text-6xl/);
    });
  });
});
