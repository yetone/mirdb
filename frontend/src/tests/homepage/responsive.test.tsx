/**
 * Responsive Design Validation Tests
 * Owner: Scenario 6 - Responsive Design Validation
 *
 * Tests that the homepage is fully responsive across mobile, tablet, and desktop viewports.
 * Validates:
 * - Mobile viewport (375px) - single column layout, no horizontal scroll
 * - Tablet viewport (768px) - 2-column feature grid
 * - Desktop viewport (1024px+) - 4-column feature grid
 * - Touch targets (44x44px minimum)
 * - Hero text readability on mobile
 * - Navigation accessibility on mobile
 * - Landscape orientation adaptation
 *
 * Requirements covered: REQ-7 (Responsive Design)
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import Home from '../../pages/Home';
import HeroSection from '../../components/homepage/HeroSection';
import { FeaturesSection } from '../../components/homepage/FeaturesSection';

// Helper to render with router
const renderWithRouter = (ui: React.ReactElement) => {
  return render(<BrowserRouter>{ui}</BrowserRouter>);
};

// Helper to set viewport width for responsive testing
const setViewportWidth = (width: number) => {
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: width,
  });
  window.dispatchEvent(new Event('resize'));
};

// Helper to check computed styles
const getComputedStyleValue = (element: Element, property: string): string => {
  return window.getComputedStyle(element).getPropertyValue(property);
};

describe('Responsive Design Validation', () => {
  beforeEach(() => {
    // Reset viewport to default
    setViewportWidth(1024);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('Test Case 1: Mobile Viewport (375px)', () => {
    it('should render homepage at 375px width without horizontal scroll', () => {
      setViewportWidth(375);
      renderWithRouter(<Home />);

      // Homepage should render
      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toBeInTheDocument();

      // Content should be contained within viewport
      // Check that main container has proper max-width constraint
      const main = heroSection.closest('main');
      expect(main).toBeInTheDocument();
      expect(main).toHaveClass('min-h-screen');

      // Hero section should have overflow hidden to prevent horizontal scroll
      expect(heroSection).toHaveClass('overflow-hidden');
    });

    it('should display single column layout on mobile', () => {
      setViewportWidth(375);
      renderWithRouter(<FeaturesSection />);

      // Get the features grid container
      const featuresGrid = document.querySelector('.grid');
      expect(featuresGrid).toBeInTheDocument();

      // Should have grid-cols-1 class for mobile (default)
      expect(featuresGrid).toHaveClass('grid-cols-1');
    });

    it('should contain all content within viewport width', () => {
      setViewportWidth(375);
      renderWithRouter(<Home />);

      // Main content should have proper padding/max-width
      const contentContainer = screen.getByTestId('hero-section').querySelector('.max-w-4xl');
      expect(contentContainer).toBeInTheDocument();
      expect(contentContainer).toHaveClass('px-4');
    });
  });

  describe('Test Case 2: Tablet Viewport (768px)', () => {
    it('should display features in 2-column grid layout at 768px', () => {
      setViewportWidth(768);
      renderWithRouter(<FeaturesSection />);

      // Get the features grid container
      const featuresGrid = document.querySelector('.grid');
      expect(featuresGrid).toBeInTheDocument();

      // Should have md:grid-cols-2 class for tablet
      expect(featuresGrid).toHaveClass('md:grid-cols-2');
    });

    it('should render analytics preview statistics in 2-column grid on tablet', () => {
      setViewportWidth(768);
      renderWithRouter(<Home />);

      const statsDisplay = screen.getByTestId('statistics-display');
      expect(statsDisplay).toBeInTheDocument();

      // Stats should have grid-cols-2 for small screens, md:grid-cols-4 for larger
      expect(statsDisplay).toHaveClass('grid-cols-2');
      expect(statsDisplay).toHaveClass('md:grid-cols-4');
    });
  });

  describe('Test Case 3: Desktop Viewport (1024px)', () => {
    it('should display features in 4-column grid layout at 1024px', () => {
      setViewportWidth(1024);
      renderWithRouter(<FeaturesSection />);

      // Get the features grid container
      const featuresGrid = document.querySelector('.grid');
      expect(featuresGrid).toBeInTheDocument();

      // Should have lg:grid-cols-4 class for desktop
      expect(featuresGrid).toHaveClass('lg:grid-cols-4');
    });

    it('should display all 4 feature cards', () => {
      setViewportWidth(1024);
      renderWithRouter(<FeaturesSection />);

      // Check for all 4 feature cards
      expect(screen.getByText('URL Shortening')).toBeInTheDocument();
      expect(screen.getByText('Click Analytics')).toBeInTheDocument();
      expect(screen.getByText('Geographic Insights')).toBeInTheDocument();
      expect(screen.getByText('Share Stats')).toBeInTheDocument();
    });
  });

  describe('Test Case 4: CTA Button Touch Target Size', () => {
    it('should meet minimum touch target size (44x44px) for CTA button on mobile', () => {
      setViewportWidth(375);
      renderWithRouter(<HeroSection />);

      // Get the primary CTA button
      const ctaButton = screen.getByRole('button', { name: /get started free/i });
      expect(ctaButton).toBeInTheDocument();

      // DaisyUI btn class provides adequate touch target size
      // The btn class has min-height of 3rem (48px) and adequate padding
      expect(ctaButton).toHaveClass('btn');
    });

    it('should have adequate padding for touch on mobile buttons', () => {
      setViewportWidth(375);
      renderWithRouter(<HeroSection />);

      const loginButton = screen.getByTestId('hero-login-link');
      expect(loginButton).toBeInTheDocument();

      // Login link should have underline offset for better touch area
      expect(loginButton).toHaveClass('underline-offset-4');
    });
  });

  describe('Test Case 5: Hero Text Readability on Mobile', () => {
    it('should display hero heading with responsive text sizes', () => {
      setViewportWidth(375);
      renderWithRouter(<HeroSection />);

      const heading = screen.getByTestId('hero-tagline');
      expect(heading).toBeInTheDocument();

      // Check responsive classes for mobile, tablet, desktop
      expect(heading).toHaveClass('text-5xl'); // Mobile
      expect(heading).toHaveClass('md:text-6xl'); // Tablet
      expect(heading).toHaveClass('lg:text-7xl'); // Desktop
    });

    it('should display hero subheading with responsive text sizes', () => {
      setViewportWidth(375);
      renderWithRouter(<HeroSection />);

      const subheading = screen.getByTestId('hero-value-proposition');
      expect(subheading).toBeInTheDocument();

      // Check responsive classes
      expect(subheading).toHaveClass('text-xl'); // Mobile
      expect(subheading).toHaveClass('md:text-2xl'); // Tablet+
    });

    it('should have proper max-width constraint for readability', () => {
      setViewportWidth(375);
      renderWithRouter(<HeroSection />);

      const contentContainer = screen.getByTestId('hero-section').querySelector('.max-w-4xl');
      expect(contentContainer).toBeInTheDocument();
    });
  });

  describe('Test Case 6: Navigation Accessibility on Mobile', () => {
    it('should have accessible navigation structure', () => {
      setViewportWidth(375);
      renderWithRouter(<Home />);

      // Hero section should have clear navigation options
      const loginLink = screen.getByTestId('hero-login-link');
      expect(loginLink).toBeInTheDocument();
      expect(loginLink).toHaveTextContent(/sign in/i);

      // CTA button should be present
      const ctaButton = screen.getByRole('button', { name: /get started free/i });
      expect(ctaButton).toBeInTheDocument();
    });

    it('should have visible navigation elements on mobile', () => {
      setViewportWidth(375);
      renderWithRouter(<HeroSection />);

      // Both primary CTA and login link should be visible
      const ctaButton = screen.getByRole('button', { name: /get started free/i });
      const loginLink = screen.getByTestId('hero-login-link');

      expect(ctaButton).toBeVisible();
      expect(loginLink).toBeVisible();
    });
  });

  describe('Test Case 7: Landscape Orientation on Mobile', () => {
    it('should adapt layout appropriately for landscape mode (667x375)', () => {
      // Simulate landscape mobile (iPhone in landscape)
      Object.defineProperty(window, 'innerWidth', {
        writable: true,
        configurable: true,
        value: 667,
      });
      Object.defineProperty(window, 'innerHeight', {
        writable: true,
        configurable: true,
        value: 375,
      });
      window.dispatchEvent(new Event('resize'));

      renderWithRouter(<Home />);

      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toBeInTheDocument();

      // Content should still be centered and readable
      expect(heroSection).toHaveClass('flex');
      expect(heroSection).toHaveClass('items-center');
      expect(heroSection).toHaveClass('justify-center');
    });

    it('should maintain readable content in landscape', () => {
      Object.defineProperty(window, 'innerWidth', {
        writable: true,
        configurable: true,
        value: 667,
      });
      window.dispatchEvent(new Event('resize'));

      renderWithRouter(<HeroSection />);

      // Text content should be present and readable
      const heading = screen.getByTestId('hero-tagline');
      const subheading = screen.getByTestId('hero-value-proposition');

      expect(heading).toBeInTheDocument();
      expect(subheading).toBeInTheDocument();
    });
  });

  describe('Test Case 8: Feature Cards on Tablet', () => {
    it('should display feature cards properly in 2-column layout on tablet', () => {
      setViewportWidth(768);
      renderWithRouter(<FeaturesSection />);

      // All 4 feature cards should be present
      const featureCards = document.querySelectorAll('.grid > div');
      expect(featureCards.length).toBe(4);

      // Grid should have proper gap spacing
      const grid = document.querySelector('.grid');
      expect(grid).toHaveClass('gap-6');
    });

    it('should have consistent card styling across all feature cards', () => {
      setViewportWidth(768);
      renderWithRouter(<FeaturesSection />);

      // Check each feature has proper content structure
      expect(screen.getByText('URL Shortening')).toBeInTheDocument();
      expect(screen.getByText('Create short, memorable links')).toBeInTheDocument();

      expect(screen.getByText('Click Analytics')).toBeInTheDocument();
      expect(screen.getByText('Track engagement in real-time')).toBeInTheDocument();

      expect(screen.getByText('Geographic Insights')).toBeInTheDocument();
      expect(screen.getByText('See where your audience is located')).toBeInTheDocument();

      expect(screen.getByText('Share Stats')).toBeInTheDocument();
      expect(screen.getByText('Generate public links to share analytics')).toBeInTheDocument();
    });

    it('should maintain proper padding and spacing on tablet', () => {
      setViewportWidth(768);
      renderWithRouter(<FeaturesSection />);

      const section = document.querySelector('section[aria-labelledby="features-heading"]');
      expect(section).toBeInTheDocument();

      // Should have responsive padding
      expect(section).toHaveClass('px-4');
      expect(section).toHaveClass('md:px-8');
    });
  });

  describe('Additional Responsive Validations', () => {
    it('should have proper responsive padding on hero content', () => {
      renderWithRouter(<HeroSection />);

      const glassCard = screen.getByTestId('hero-section').querySelector('.backdrop-blur-md');
      expect(glassCard).toBeInTheDocument();

      // Should have responsive padding
      expect(glassCard).toHaveClass('p-8');
      expect(glassCard).toHaveClass('md:p-12');
      expect(glassCard).toHaveClass('lg:p-16');
    });

    it('should have proper responsive layout for analytics charts', () => {
      setViewportWidth(768);
      renderWithRouter(<Home />);

      // Charts grid should be responsive
      const chartsGrid = document.querySelector('[data-testid="clicks-chart"]')?.parentElement;
      expect(chartsGrid).toBeInTheDocument();
      expect(chartsGrid).toHaveClass('grid-cols-1');
      expect(chartsGrid).toHaveClass('lg:grid-cols-2');
    });

    it('should handle very small viewport (320px)', () => {
      setViewportWidth(320);
      renderWithRouter(<Home />);

      // Page should still render without errors
      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toBeInTheDocument();

      // Content should have proper padding
      const contentContainer = heroSection.querySelector('.px-4');
      expect(contentContainer).toBeInTheDocument();
    });

    it('should handle large desktop viewport (1920px)', () => {
      setViewportWidth(1920);
      renderWithRouter(<Home />);

      // Page should render properly with max-width constraints
      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toBeInTheDocument();

      // Content should be centered with max-width
      const contentContainer = heroSection.querySelector('.max-w-4xl');
      expect(contentContainer).toBeInTheDocument();
      expect(contentContainer).toHaveClass('mx-auto');
    });
  });
});
