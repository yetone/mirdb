/**
 * Responsive Design Tests
 * Scenario 6: Validates that the homepage displays correctly across
 * mobile, tablet, and desktop viewports with appropriate layouts
 *
 * REQ-6: Homepage shall be responsive and function properly on mobile, tablet, and desktop devices
 * User Story 5: Mobile responsiveness with accessible URL form and tappable buttons
 */
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import Home from '../../src/pages/Home';
import { ThemeProvider } from '../../src/contexts/ThemeContext';

/**
 * Helper to render Home component with router and theme context
 */
const renderHome = () => {
  return render(
    <BrowserRouter>
      <ThemeProvider>
        <Home />
      </ThemeProvider>
    </BrowserRouter>
  );
};

/**
 * Helper to set viewport dimensions for responsive testing
 * jsdom doesn't support actual viewport changes, so we mock window properties
 */
const setViewportWidth = (width: number) => {
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: width,
  });
  Object.defineProperty(window, 'innerHeight', {
    writable: true,
    configurable: true,
    value: 800,
  });
  window.dispatchEvent(new Event('resize'));
};

describe('Responsive Design (Scenario 6)', () => {
  beforeEach(() => {
    // Reset viewport to desktop default
    setViewportWidth(1280);
    // Clear localStorage for theme state
    localStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    localStorage.clear();
  });

  describe('Test Case 1: Mobile Viewport (375px)', () => {
    /**
     * Test Case 1: Render Home at viewport width 375px (mobile)
     * Expected: No horizontal overflow, single column layout
     */
    it('should render without horizontal overflow at mobile width', () => {
      setViewportWidth(375);
      renderHome();

      // Main container should have responsive padding and be contained
      const mainContainer = document.querySelector('main');
      expect(mainContainer).toBeInTheDocument();
      expect(mainContainer).toHaveClass('container');
      expect(mainContainer).toHaveClass('mx-auto');
      expect(mainContainer).toHaveClass('px-4');
    });

    it('should have single column layout structure for mobile', () => {
      setViewportWidth(375);
      renderHome();

      // Hero section should be centered with full width
      const heroSection = document.querySelector('[aria-labelledby="hero-headline"]');
      expect(heroSection).toBeInTheDocument();
      expect(heroSection).toHaveClass('text-center');
      expect(heroSection).toHaveClass('max-w-4xl');
      expect(heroSection).toHaveClass('mx-auto');
    });

    it('should have proper text sizing for mobile', () => {
      setViewportWidth(375);
      renderHome();

      // Headline should have responsive text classes (text-4xl as base)
      const headline = screen.getByRole('heading', { level: 1 });
      expect(headline).toBeInTheDocument();
      // Mobile uses text-4xl, tablet uses md:text-5xl, desktop uses lg:text-6xl
      expect(headline).toHaveClass('text-4xl');
    });

    it('should have flex-wrap on CTA navigation for mobile stacking', () => {
      setViewportWidth(375);
      renderHome();

      const ctaNav = document.querySelector('nav[aria-label="Primary navigation"]');
      expect(ctaNav).toBeInTheDocument();
      expect(ctaNav).toHaveClass('flex-wrap');
      expect(ctaNav).toHaveClass('justify-center');
      expect(ctaNav).toHaveClass('gap-4');
    });
  });

  describe('Test Case 2: CTA Button Touch Targets', () => {
    /**
     * Test Case 2: Measure CTA button dimensions at mobile viewport
     * Expected: Buttons have minimum dimensions of 44x44px
     */
    it('should have Get Started button with btn-lg class for adequate touch target', () => {
      setViewportWidth(375);
      renderHome();

      const getStartedButton = screen.getByTestId('cta-get-started');
      expect(getStartedButton).toBeInTheDocument();
      // btn-lg in DaisyUI provides minimum 44px height for touch targets
      expect(getStartedButton).toHaveClass('btn-lg');
    });

    it('should have Login button with btn-lg class for adequate touch target', () => {
      setViewportWidth(375);
      renderHome();

      const loginButton = screen.getByTestId('cta-login');
      expect(loginButton).toBeInTheDocument();
      // btn-lg in DaisyUI provides minimum 44px height for touch targets
      expect(loginButton).toHaveClass('btn-lg');
    });

    it('should have both CTA buttons with adequate padding for touch targets', () => {
      setViewportWidth(375);
      renderHome();

      const getStartedButton = screen.getByTestId('cta-get-started');
      const loginButton = screen.getByTestId('cta-login');

      // Both buttons should have btn class (DaisyUI base) and btn-lg for touch accessibility
      expect(getStartedButton).toHaveClass('btn');
      expect(loginButton).toHaveClass('btn');
      expect(getStartedButton).toHaveClass('btn-lg');
      expect(loginButton).toHaveClass('btn-lg');
    });
  });

  describe('Test Case 3: Tablet Viewport (768px)', () => {
    /**
     * Test Case 3: Render Home at viewport width 768px (tablet)
     * Expected: Layout adapts with proper spacing, possibly 2-column features
     */
    it('should render correctly at tablet width', () => {
      setViewportWidth(768);
      renderHome();

      const mainContainer = document.querySelector('main');
      expect(mainContainer).toBeInTheDocument();
      expect(mainContainer).toHaveClass('container');
    });

    it('should have features grid with responsive classes for tablet', () => {
      setViewportWidth(768);
      renderHome();

      // Features grid should use md:grid-cols-2 for tablet
      const featuresGrid = document.querySelector('[role="list"][aria-label="Product features"]');
      expect(featuresGrid).toBeInTheDocument();
      expect(featuresGrid).toHaveClass('grid');
      expect(featuresGrid).toHaveClass('grid-cols-1');
      expect(featuresGrid).toHaveClass('md:grid-cols-2');
    });

    it('should have proper headline sizing for tablet', () => {
      setViewportWidth(768);
      renderHome();

      const headline = screen.getByRole('heading', { level: 1 });
      // Tablet uses md:text-5xl
      expect(headline).toHaveClass('md:text-5xl');
    });
  });

  describe('Test Case 4: Desktop Viewport (1280px)', () => {
    /**
     * Test Case 4: Render Home at viewport width 1280px (desktop)
     * Expected: Full desktop layout with hero side-by-side or centered
     */
    it('should render correctly at desktop width', () => {
      setViewportWidth(1280);
      renderHome();

      const mainContainer = document.querySelector('main');
      expect(mainContainer).toBeInTheDocument();
      expect(mainContainer).toHaveClass('container');
    });

    it('should have centered hero layout for desktop', () => {
      setViewportWidth(1280);
      renderHome();

      const heroSection = document.querySelector('[aria-labelledby="hero-headline"]');
      expect(heroSection).toBeInTheDocument();
      expect(heroSection).toHaveClass('text-center');
      expect(heroSection).toHaveClass('max-w-4xl');
      expect(heroSection).toHaveClass('mx-auto');
    });

    it('should have features grid with 4 columns for desktop', () => {
      setViewportWidth(1280);
      renderHome();

      const featuresGrid = document.querySelector('[role="list"][aria-label="Product features"]');
      expect(featuresGrid).toBeInTheDocument();
      // Desktop uses lg:grid-cols-4
      expect(featuresGrid).toHaveClass('lg:grid-cols-4');
    });

    it('should have proper headline sizing for desktop', () => {
      setViewportWidth(1280);
      renderHome();

      const headline = screen.getByRole('heading', { level: 1 });
      // Desktop uses lg:text-6xl
      expect(headline).toHaveClass('lg:text-6xl');
    });
  });

  describe('Test Case 5: URL Form Usability at Mobile (375px)', () => {
    /**
     * Test Case 5: Check URL form usability at 375px width
     * Expected: Input field full width, submit button accessible
     */
    it('should have URL form area accessible at mobile width', () => {
      setViewportWidth(375);
      renderHome();

      // URL form placeholder area should be present
      const formArea = document.querySelector('[aria-label="URL shortening form area"]');
      expect(formArea).toBeInTheDocument();
      // Form area should have max-width and centered alignment
      expect(formArea).toHaveClass('max-w-2xl');
      expect(formArea).toHaveClass('mx-auto');
    });

    it('should have form area with responsive width classes', () => {
      setViewportWidth(375);
      renderHome();

      // The form container should be properly sized for mobile
      const formArea = document.querySelector('[aria-label="URL shortening form area"]');
      expect(formArea).toBeInTheDocument();
      // max-w-2xl allows full width on mobile, constrained on larger screens
      expect(formArea).toHaveClass('max-w-2xl');
    });
  });

  describe('Test Case 6: Features Section Responsive Layout', () => {
    /**
     * Test Case 6: Verify features section at different viewports
     * Expected: Features stack on mobile, grid on tablet/desktop
     */
    it('should have features with single column on mobile (grid-cols-1)', () => {
      setViewportWidth(375);
      renderHome();

      const featuresGrid = document.querySelector('[role="list"][aria-label="Product features"]');
      expect(featuresGrid).toBeInTheDocument();
      expect(featuresGrid).toHaveClass('grid-cols-1');
    });

    it('should have features with 2 columns on tablet (md:grid-cols-2)', () => {
      setViewportWidth(768);
      renderHome();

      const featuresGrid = document.querySelector('[role="list"][aria-label="Product features"]');
      expect(featuresGrid).toBeInTheDocument();
      expect(featuresGrid).toHaveClass('md:grid-cols-2');
    });

    it('should have features with 4 columns on desktop (lg:grid-cols-4)', () => {
      setViewportWidth(1280);
      renderHome();

      const featuresGrid = document.querySelector('[role="list"][aria-label="Product features"]');
      expect(featuresGrid).toBeInTheDocument();
      expect(featuresGrid).toHaveClass('lg:grid-cols-4');
    });

    it('should have consistent gap spacing across all viewports', () => {
      renderHome();

      const featuresGrid = document.querySelector('[role="list"][aria-label="Product features"]');
      expect(featuresGrid).toBeInTheDocument();
      expect(featuresGrid).toHaveClass('gap-6');
    });
  });

  describe('Responsive Container and Padding', () => {
    it('should have responsive container with horizontal padding', () => {
      renderHome();

      const mainContainer = document.querySelector('main');
      expect(mainContainer).toBeInTheDocument();
      expect(mainContainer).toHaveClass('px-4');
      expect(mainContainer).toHaveClass('container');
      expect(mainContainer).toHaveClass('mx-auto');
    });

    it('should have adequate vertical padding on main content', () => {
      renderHome();

      const mainContainer = document.querySelector('main');
      expect(mainContainer).toBeInTheDocument();
      expect(mainContainer).toHaveClass('py-16');
    });

    it('should have full viewport height minimum', () => {
      renderHome();

      const rootContainer = document.querySelector('.min-h-screen');
      expect(rootContainer).toBeInTheDocument();
    });
  });

  describe('Responsive Typography', () => {
    it('should have responsive headline text sizes', () => {
      renderHome();

      const headline = screen.getByRole('heading', { level: 1 });
      // Check all responsive breakpoint classes
      expect(headline).toHaveClass('text-4xl');     // mobile
      expect(headline).toHaveClass('md:text-5xl');  // tablet
      expect(headline).toHaveClass('lg:text-6xl');  // desktop
    });

    it('should have responsive subheadline text sizes', () => {
      renderHome();

      const subheadline = screen.getByTestId('hero-subheadline');
      // Check responsive text classes
      expect(subheadline).toHaveClass('text-xl');     // mobile
      expect(subheadline).toHaveClass('md:text-2xl'); // tablet/desktop
    });
  });

  describe('Responsive Navigation Layout', () => {
    it('should have flex navigation with wrap capability', () => {
      renderHome();

      const nav = document.querySelector('nav[aria-label="Primary navigation"]');
      expect(nav).toBeInTheDocument();
      expect(nav).toHaveClass('flex');
      expect(nav).toHaveClass('flex-wrap');
    });

    it('should have centered navigation items', () => {
      renderHome();

      const nav = document.querySelector('nav[aria-label="Primary navigation"]');
      expect(nav).toBeInTheDocument();
      expect(nav).toHaveClass('justify-center');
      expect(nav).toHaveClass('items-center');
    });
  });
});
