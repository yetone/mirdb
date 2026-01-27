/**
 * Responsive Design Tests
 * Owner: Scenarios 6, 7, 8
 *
 * Tests for responsive behavior across viewports:
 * - Mobile viewport 375px (Scenario 6)
 * - Tablet viewport 768px (Scenario 7)
 * - Desktop viewport 1024px+ (Scenario 8)
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { screen, within } from '@testing-library/react';
import { renderWithProviders } from './testUtils';
import Home from '../../src/pages/Home';
import FeaturesSection from '../../src/components/homepage/FeaturesSection';
import HeroSection from '../../src/components/homepage/HeroSection';
import HowItWorksSection from '../../src/components/homepage/HowItWorksSection';

/**
 * Helper to set viewport width for responsive testing
 * Note: This modifies window.innerWidth and dispatches resize event
 * CSS media queries in jsdom don't actually respond to this,
 * so we test the presence of responsive CSS classes instead
 */
function setViewportWidth(width: number) {
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: width,
  });
  window.dispatchEvent(new Event('resize'));
}

/**
 * Reset viewport to default after tests
 */
function resetViewport() {
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: 1024,
  });
}

/**
 * Scenario 7: Responsive Design - Tablet Viewport (768px)
 *
 * Test homepage display on tablet devices (768px width)
 *
 * Steps:
 * 1. Set viewport to tablet (768px width)
 * 2. Navigate to homepage
 * 3. Verify layout adaptation
 * 4. Verify all sections visible
 */
describe('Scenario 7: Responsive Design - Tablet Viewport', () => {
  beforeEach(() => {
    setViewportWidth(768);
  });

  afterEach(() => {
    resetViewport();
  });

  describe('Test Case 1: Render HomePage at 768px viewport width', () => {
    it('renders homepage correctly at tablet viewport width', () => {
      renderWithProviders(<Home />);

      // Verify main homepage structure is present
      expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument();

      // Verify all main sections render
      expect(screen.getByText(/Shorten URLs/i)).toBeInTheDocument();
      expect(screen.getByTestId('features-section')).toBeInTheDocument();
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument();
    });

    it('hero section renders with appropriate tablet styling classes', () => {
      renderWithProviders(<HeroSection />);

      // Find the h1 element
      const heading = screen.getByRole('heading', { level: 1 });
      expect(heading).toBeInTheDocument();

      // Check that responsive classes are present for tablet breakpoint
      // At 768px (md breakpoint), text-4xl becomes md:text-5xl
      expect(heading.className).toContain('md:text-5xl');
      expect(heading.className).toContain('lg:text-6xl');
    });

    it('hero CTA buttons render appropriately at tablet width', () => {
      renderWithProviders(<HeroSection />);

      // Find the CTA container
      const getStartedLink = screen.getByRole('link', { name: /Get Started/i });
      const signInLink = screen.getByRole('link', { name: /Sign In/i });

      expect(getStartedLink).toBeInTheDocument();
      expect(signInLink).toBeInTheDocument();

      // The parent container should have sm:flex-row class for horizontal layout on tablet
      const ctaContainer = getStartedLink.parentElement;
      expect(ctaContainer?.className).toContain('sm:flex-row');
    });

    it('navigation renders correctly at tablet viewport', () => {
      renderWithProviders(<Home />);

      // Navbar should be present
      const nav = screen.getByRole('navigation');
      expect(nav).toBeInTheDocument();

      // Navigation links should be accessible
      const signInLinks = screen.getAllByRole('link', { name: /Sign In/i });
      expect(signInLinks.length).toBeGreaterThan(0);

      const getStartedLinks = screen.getAllByRole('link', { name: /Get Started/i });
      expect(getStartedLinks.length).toBeGreaterThan(0);
    });

    it('all homepage sections are visible without content cut off', () => {
      renderWithProviders(<Home />);

      // Hero section
      const heroHeading = screen.getByRole('heading', { level: 1 });
      expect(heroHeading).toBeInTheDocument();
      expect(heroHeading.textContent).toMatch(/Shorten URLs/i);

      // Features section
      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection).toBeInTheDocument();
      const featuresHeading = within(featuresSection).getByRole('heading', { level: 2 });
      expect(featuresHeading.textContent).toMatch(/Powerful Features/i);

      // How It Works section
      const howItWorksSection = screen.getByTestId('how-it-works-section');
      expect(howItWorksSection).toBeInTheDocument();
      const howItWorksHeading = within(howItWorksSection).getByRole('heading', { level: 2 });
      expect(howItWorksHeading.textContent).toMatch(/How It Works/i);

      // CTA section
      const ctaSection = screen.getByText(/Ready to Get Started/i);
      expect(ctaSection).toBeInTheDocument();

      // Footer
      const footer = screen.getByRole('contentinfo');
      expect(footer).toBeInTheDocument();
    });
  });

  describe('Test Case 2: Check feature cards layout at tablet', () => {
    it('features section grid has md:grid-cols-2 class for 2-column tablet layout', () => {
      renderWithProviders(<FeaturesSection />);

      // Find the features grid container
      const featuresGrid = screen.getByTestId('features-grid');
      expect(featuresGrid).toBeInTheDocument();

      // Verify it has the responsive grid classes
      // At tablet (768px = md breakpoint), should show 2 columns
      expect(featuresGrid.className).toContain('grid-cols-1');
      expect(featuresGrid.className).toContain('md:grid-cols-2');
      expect(featuresGrid.className).toContain('lg:grid-cols-4');
    });

    it('all four feature cards render correctly at tablet viewport', () => {
      renderWithProviders(<FeaturesSection />);

      // Verify all 4 feature cards are present
      const featureCard0 = screen.getByTestId('feature-card-0');
      const featureCard1 = screen.getByTestId('feature-card-1');
      const featureCard2 = screen.getByTestId('feature-card-2');
      const featureCard3 = screen.getByTestId('feature-card-3');

      expect(featureCard0).toBeInTheDocument();
      expect(featureCard1).toBeInTheDocument();
      expect(featureCard2).toBeInTheDocument();
      expect(featureCard3).toBeInTheDocument();

      // Verify feature content
      expect(screen.getByText('URL Shortening')).toBeInTheDocument();
      expect(screen.getByText('Click Analytics')).toBeInTheDocument();
      expect(screen.getByText('Dashboard Management')).toBeInTheDocument();
      expect(screen.getByText('Share Statistics')).toBeInTheDocument();
    });

    it('feature cards display with appropriate tablet styling', () => {
      renderWithProviders(<FeaturesSection />);

      const featuresGrid = screen.getByTestId('features-grid');

      // At md breakpoint (768px), the grid should use 2 columns
      // The classes grid-cols-1 md:grid-cols-2 lg:grid-cols-4 achieve this
      expect(featuresGrid.className).toContain('md:grid-cols-2');

      // Gap should be appropriate for tablet
      expect(featuresGrid.className).toContain('gap-6');
    });

    it('feature card content is not truncated at tablet width', () => {
      renderWithProviders(<FeaturesSection />);

      // Check that feature descriptions are fully visible
      const urlShorteningDesc = screen.getByText(
        /Create short, memorable links instantly/i
      );
      expect(urlShorteningDesc).toBeInTheDocument();

      const analyticsDesc = screen.getByText(
        /Track every click with detailed insights/i
      );
      expect(analyticsDesc).toBeInTheDocument();

      const dashboardDesc = screen.getByText(
        /Manage all your links in one place/i
      );
      expect(dashboardDesc).toBeInTheDocument();

      const shareDesc = screen.getByText(
        /Generate shareable stats links/i
      );
      expect(shareDesc).toBeInTheDocument();
    });
  });

  describe('How It Works section at tablet viewport', () => {
    it('how it works grid has md:grid-cols-2 class for 2-column tablet layout', () => {
      renderWithProviders(<HowItWorksSection />);

      const stepsGrid = screen.getByTestId('how-it-works-steps');
      expect(stepsGrid).toBeInTheDocument();

      // At tablet (768px = md breakpoint), should show 2 columns
      expect(stepsGrid.className).toContain('grid-cols-1');
      expect(stepsGrid.className).toContain('md:grid-cols-2');
      expect(stepsGrid.className).toContain('lg:grid-cols-4');
    });

    it('all four steps render correctly at tablet viewport', () => {
      renderWithProviders(<HowItWorksSection />);

      // Verify all 4 steps are present
      expect(screen.getByTestId('step-1')).toBeInTheDocument();
      expect(screen.getByTestId('step-2')).toBeInTheDocument();
      expect(screen.getByTestId('step-3')).toBeInTheDocument();
      expect(screen.getByTestId('step-4')).toBeInTheDocument();

      // Verify step indicators
      expect(screen.getByTestId('step-indicator-1')).toBeInTheDocument();
      expect(screen.getByTestId('step-indicator-2')).toBeInTheDocument();
      expect(screen.getByTestId('step-indicator-3')).toBeInTheDocument();
      expect(screen.getByTestId('step-indicator-4')).toBeInTheDocument();
    });

    it('step content is fully visible at tablet width', () => {
      renderWithProviders(<HowItWorksSection />);

      // Verify step titles
      expect(screen.getByText('Paste Your URL')).toBeInTheDocument();
      expect(screen.getByText('Get Short Link')).toBeInTheDocument();
      expect(screen.getByText('Share Anywhere')).toBeInTheDocument();
      expect(screen.getByText('Track Performance')).toBeInTheDocument();

      // Verify step descriptions are present
      expect(screen.getByText(/Enter your long URL into our shortener/i)).toBeInTheDocument();
      expect(screen.getByText(/Instantly receive a clean, compact URL/i)).toBeInTheDocument();
      expect(screen.getByText(/Share your shortened link on social media/i)).toBeInTheDocument();
      expect(screen.getByText(/Monitor clicks, analyze traffic sources/i)).toBeInTheDocument();
    });
  });

  describe('Typography scaling at tablet viewport', () => {
    it('hero heading uses appropriate text size for tablet', () => {
      renderWithProviders(<HeroSection />);

      const heading = screen.getByRole('heading', { level: 1 });

      // Base: text-4xl, tablet (md): text-5xl, desktop (lg): text-6xl
      expect(heading.className).toContain('text-4xl');
      expect(heading.className).toContain('md:text-5xl');
    });

    it('section headings have responsive styling', () => {
      renderWithProviders(<Home />);

      const featuresSection = screen.getByTestId('features-section');
      const featuresHeading = within(featuresSection).getByRole('heading', { level: 2 });

      // Features heading should have responsive text classes
      expect(featuresHeading.className).toContain('text-3xl');
      expect(featuresHeading.className).toContain('md:text-4xl');
    });

    it('hero subheading has responsive styling', () => {
      renderWithProviders(<HeroSection />);

      // Find the supporting paragraph
      const subheading = screen.getByText(/Transform your long URLs/i);

      // Base: text-lg, tablet (md): text-xl
      expect(subheading.className).toContain('text-lg');
      expect(subheading.className).toContain('md:text-xl');
    });
  });

  describe('Container and padding at tablet viewport', () => {
    it('main content container has appropriate padding', () => {
      renderWithProviders(<Home />);

      // The hero section should have container with padding
      const heroHeading = screen.getByRole('heading', { level: 1 });
      const heroContainer = heroHeading.closest('.container');

      expect(heroContainer).toBeInTheDocument();
      expect(heroContainer?.className).toContain('mx-auto');
      expect(heroContainer?.className).toContain('px-4');
    });

    it('sections have appropriate vertical padding for tablet', () => {
      renderWithProviders(<FeaturesSection />);

      const featuresSection = screen.getByTestId('features-section');

      // Check responsive padding: py-16 md:py-24
      expect(featuresSection.className).toContain('py-16');
      expect(featuresSection.className).toContain('md:py-24');
    });
  });
});
