/**
 * Responsive design tests for homepage.
 * Owner: Scenario 5 - Mobile Responsive Design
 *
 * Test coverage:
 * - Mobile (375px): Single column layout, touch-friendly CTAs
 * - Tablet (768px): 2-column feature grid
 * - Desktop (1280px): 4-column feature grid
 * - No horizontal overflow at any viewport
 * - Navbar mobile menu visibility
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, within } from '@testing-library/react';
import { Home } from '@/pages/Home';
import { renderWithProviders } from '../utils/renderWithProviders';
import { setViewport, resetViewport, VIEWPORTS } from '../utils/viewportHelpers';

describe('Mobile Responsive Design - Homepage', () => {
  afterEach(() => {
    resetViewport();
  });

  describe('Test Case 1: Feature cards stack vertically at mobile viewport (375px)', () => {
    beforeEach(() => {
      setViewport(VIEWPORTS.mobile);
    });

    it('renders feature cards in single column layout at mobile viewport', () => {
      renderWithProviders(<Home />);

      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection).toBeInTheDocument();

      // The grid should have responsive classes for single column on mobile
      const grid = featuresSection.querySelector('.grid');
      expect(grid).toBeInTheDocument();
      expect(grid).toHaveClass('grid-cols-1');

      // Verify all 4 feature cards are present
      const featureCards = [
        screen.getByTestId('feature-card-url-shortening'),
        screen.getByTestId('feature-card-click-analytics'),
        screen.getByTestId('feature-card-secure-management'),
        screen.getByTestId('feature-card-custom-short-codes'),
      ];

      featureCards.forEach((card) => {
        expect(card).toBeInTheDocument();
      });
    });

    it('homepage renders correctly at 375px width', () => {
      renderWithProviders(<Home />);

      expect(screen.getByTestId('home-page')).toBeInTheDocument();
      expect(screen.getByTestId('hero-section')).toBeInTheDocument();
      expect(screen.getByTestId('features-section')).toBeInTheDocument();
    });
  });

  describe('Test Case 2: CTA buttons have minimum 44px height for touch accessibility', () => {
    beforeEach(() => {
      setViewport(VIEWPORTS.mobile);
    });

    it('CTA buttons have min-h-[44px] class for touch accessibility', () => {
      renderWithProviders(<Home />);

      const getStartedButton = screen.getByTestId('cta-get-started');
      const signInButton = screen.getByTestId('cta-sign-in');

      // Check that buttons have the minimum height class
      expect(getStartedButton).toHaveClass('min-h-[44px]');
      expect(signInButton).toHaveClass('min-h-[44px]');
    });

    it('CTA buttons are accessible on mobile viewport', () => {
      renderWithProviders(<Home />);

      const getStartedButton = screen.getByTestId('cta-get-started');
      const signInButton = screen.getByTestId('cta-sign-in');

      // Verify buttons are visible and have proper ARIA labels
      expect(getStartedButton).toBeVisible();
      expect(signInButton).toBeVisible();
      expect(getStartedButton).toHaveAttribute('aria-label');
      expect(signInButton).toHaveAttribute('aria-label');
    });
  });

  describe('Test Case 3: No horizontal overflow on mobile viewport', () => {
    beforeEach(() => {
      setViewport(VIEWPORTS.mobile);
    });

    it('page content fits within viewport without horizontal scrollbar', () => {
      const { container } = renderWithProviders(<Home />);

      const homePage = screen.getByTestId('home-page');
      expect(homePage).toBeInTheDocument();

      // Check that the page doesn't have overflow-x classes that would allow scrolling
      expect(homePage).not.toHaveClass('overflow-x-scroll');
      expect(homePage).not.toHaveClass('overflow-x-auto');

      // The main container should use flex-col and min-h-screen for proper layout
      expect(homePage).toHaveClass('min-h-screen');
      expect(homePage).toHaveClass('flex');
      expect(homePage).toHaveClass('flex-col');
    });

    it('hero section uses responsive padding on mobile', () => {
      renderWithProviders(<Home />);

      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toBeInTheDocument();

      // Hero section should have horizontal padding
      expect(heroSection).toHaveClass('px-4');
    });

    it('features section uses proper container width', () => {
      renderWithProviders(<Home />);

      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection).toBeInTheDocument();

      // Features section should have horizontal padding
      expect(featuresSection).toHaveClass('px-4');
    });
  });

  describe('Test Case 4: Navbar hamburger menu visibility at mobile viewport', () => {
    it('hamburger menu icon is visible at mobile viewport', () => {
      setViewport(VIEWPORTS.mobile);
      renderWithProviders(<Home />);

      const hamburgerButton = screen.getByTestId('navbar-hamburger');
      expect(hamburgerButton).toBeInTheDocument();

      // The hamburger is inside a div with md:hidden class
      const hamburgerContainer = hamburgerButton.closest('.md\\:hidden');
      expect(hamburgerContainer).toBeInTheDocument();
    });

    it('desktop navigation is hidden at mobile viewport', () => {
      setViewport(VIEWPORTS.mobile);
      renderWithProviders(<Home />);

      const desktopNav = screen.getByTestId('navbar-desktop-nav');
      expect(desktopNav).toBeInTheDocument();

      // Desktop nav should have hidden md:flex classes
      expect(desktopNav).toHaveClass('hidden');
      expect(desktopNav).toHaveClass('md:flex');
    });

    it('mobile menu opens when hamburger is clicked', () => {
      setViewport(VIEWPORTS.mobile);
      renderWithProviders(<Home />);

      const hamburgerButton = screen.getByTestId('navbar-hamburger');

      // Initially, mobile menu should not be visible
      expect(screen.queryByTestId('navbar-mobile-menu')).not.toBeInTheDocument();

      // Click hamburger to open menu
      fireEvent.click(hamburgerButton);

      // Now mobile menu should be visible
      expect(screen.getByTestId('navbar-mobile-menu')).toBeInTheDocument();
    });

    it('mobile menu contains login and register links', () => {
      setViewport(VIEWPORTS.mobile);
      renderWithProviders(<Home />);

      // Open mobile menu
      fireEvent.click(screen.getByTestId('navbar-hamburger'));

      const mobileMenu = screen.getByTestId('navbar-mobile-menu');
      expect(mobileMenu).toBeInTheDocument();

      // Check for login and register links in mobile menu
      expect(screen.getByTestId('navbar-login-mobile')).toBeInTheDocument();
      expect(screen.getByTestId('navbar-register-mobile')).toBeInTheDocument();
    });
  });

  describe('Test Case 5: Feature grid at tablet viewport (768px)', () => {
    beforeEach(() => {
      setViewport(VIEWPORTS.tablet);
    });

    it('features display in 2-column grid layout at tablet viewport', () => {
      renderWithProviders(<Home />);

      const featuresSection = screen.getByTestId('features-section');
      const grid = featuresSection.querySelector('.grid');

      expect(grid).toBeInTheDocument();
      // Grid should have md:grid-cols-2 class for tablet
      expect(grid).toHaveClass('md:grid-cols-2');
    });

    it('desktop navigation is visible at tablet viewport', () => {
      renderWithProviders(<Home />);

      const desktopNav = screen.getByTestId('navbar-desktop-nav');
      expect(desktopNav).toBeInTheDocument();
      expect(desktopNav).toHaveClass('md:flex');
    });
  });

  describe('Test Case 6: Feature grid at desktop viewport (1280px)', () => {
    beforeEach(() => {
      setViewport(VIEWPORTS.desktop);
    });

    it('features display in 4-column grid layout at desktop viewport', () => {
      renderWithProviders(<Home />);

      const featuresSection = screen.getByTestId('features-section');
      const grid = featuresSection.querySelector('.grid');

      expect(grid).toBeInTheDocument();
      // Grid should have lg:grid-cols-4 class for desktop
      expect(grid).toHaveClass('lg:grid-cols-4');
    });

    it('all feature cards are visible in 4-column layout', () => {
      renderWithProviders(<Home />);

      const featureCards = [
        'feature-card-url-shortening',
        'feature-card-click-analytics',
        'feature-card-secure-management',
        'feature-card-custom-short-codes',
      ];

      featureCards.forEach((testId) => {
        expect(screen.getByTestId(testId)).toBeInTheDocument();
      });
    });

    it('desktop navigation is visible at desktop viewport', () => {
      renderWithProviders(<Home />);

      const desktopNav = screen.getByTestId('navbar-desktop-nav');
      expect(desktopNav).toBeInTheDocument();
      expect(desktopNav).toHaveClass('md:flex');

      // Login and register should be visible in desktop nav
      expect(screen.getByTestId('navbar-login')).toBeInTheDocument();
      expect(screen.getByTestId('navbar-register')).toBeInTheDocument();
    });
  });

  describe('Responsive Typography', () => {
    it('hero headline has responsive font sizes', () => {
      setViewport(VIEWPORTS.mobile);
      renderWithProviders(<Home />);

      const headline = screen.getByTestId('hero-headline');
      expect(headline).toBeInTheDocument();

      // Check for responsive text classes
      expect(headline).toHaveClass('text-4xl');
      expect(headline).toHaveClass('md:text-5xl');
      expect(headline).toHaveClass('lg:text-6xl');
    });

    it('hero subheadline has responsive font sizes', () => {
      setViewport(VIEWPORTS.mobile);
      renderWithProviders(<Home />);

      const subheadline = screen.getByTestId('hero-subheadline');
      expect(subheadline).toBeInTheDocument();

      // Check for responsive text classes
      expect(subheadline).toHaveClass('text-lg');
      expect(subheadline).toHaveClass('md:text-xl');
    });
  });

  describe('CTA Button Layout', () => {
    it('CTA buttons stack vertically on mobile (flex-col)', () => {
      setViewport(VIEWPORTS.mobile);
      renderWithProviders(<Home />);

      const heroSection = screen.getByTestId('hero-section');
      const ctaContainer = heroSection.querySelector('.flex.flex-col');

      expect(ctaContainer).toBeInTheDocument();
      expect(ctaContainer).toHaveClass('sm:flex-row');
    });

    it('CTA buttons display in row on larger screens (sm:flex-row)', () => {
      setViewport(VIEWPORTS.tablet);
      renderWithProviders(<Home />);

      const heroSection = screen.getByTestId('hero-section');
      const ctaContainer = heroSection.querySelector('.flex.flex-col.sm\\:flex-row');

      expect(ctaContainer).toBeInTheDocument();
    });
  });
});
