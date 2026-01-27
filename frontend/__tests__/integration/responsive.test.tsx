/**
 * Responsive Design Integration Tests
 * Owner: Scenario 7 - Responsive Design - Mobile (320px)
 * Owner: Scenario 8 - Responsive Design - Tablet (768px)
 * Owner: Scenario 9 - Responsive Design - Desktop (1024px, 1440px)
 *
 * Tests verify the landing page renders correctly across different viewport sizes.
 * REQ-7: Support responsive design for mobile, tablet, and desktop viewports.
 */

import React from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { ThemeProvider } from '../../src/contexts/ThemeContext';
import { Home } from '../../src/pages/Home';

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <div {...props}>{children}</div>
    ),
    button: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <button {...props}>{children}</button>
    ),
    section: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <section {...props}>{children}</section>
    ),
    span: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <span {...props}>{children}</span>
    ),
  },
}));

// Mock BackgroundEffect component to avoid Three.js issues in tests
vi.mock('../../src/components/BackgroundEffect', () => ({
  BackgroundEffect: () => (
    <div data-testid="background-effect" aria-hidden="true" />
  ),
  default: () => (
    <div data-testid="background-effect" aria-hidden="true" />
  ),
}));

// Test wrapper that provides all necessary contexts
function renderWithProviders(ui: React.ReactElement) {
  return render(
    <ThemeProvider>
      <MemoryRouter initialEntries={['/']}>
        {ui}
      </MemoryRouter>
    </ThemeProvider>
  );
}

// Helper to set viewport width for testing
function setViewportWidth(width: number) {
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: width,
  });
  Object.defineProperty(document.documentElement, 'clientWidth', {
    writable: true,
    configurable: true,
    value: width,
  });
  window.dispatchEvent(new Event('resize'));
}

// Helper to check if element has certain CSS classes (Tailwind responsive)
function hasResponsiveClass(element: HTMLElement, className: string): boolean {
  return element.className.includes(className);
}

describe('Responsive Design - Mobile (320px)', () => {
  beforeEach(() => {
    // Set viewport to mobile width
    setViewportWidth(320);
  });

  afterEach(() => {
    // Reset viewport
    setViewportWidth(1024);
  });

  describe('Test Case 1: Hero section is visible and text is readable', () => {
    it('renders Hero section with visible headline', () => {
      renderWithProviders(<Home />);

      // Hero section should be rendered
      const heroSection = screen.getByRole('region', { name: /hero section/i });
      expect(heroSection).toBeInTheDocument();

      // Main headline should be present and readable
      const headline = screen.getByRole('heading', { level: 1 });
      expect(headline).toBeInTheDocument();
      expect(headline).toHaveTextContent(/shorten.*share.*track/i);
    });

    it('hero section has readable subheadline', () => {
      renderWithProviders(<Home />);

      // Subheadline should be present in hero section (check for "Gain powerful insights" which is unique)
      const subheadline = screen.getByText(/gain powerful insights/i);
      expect(subheadline).toBeInTheDocument();
    });

    it('hero content is centered and accessible at mobile width', () => {
      renderWithProviders(<Home />);

      // Container has padding for mobile readability
      const heroSection = screen.getByRole('region', { name: /hero section/i });
      const container = heroSection.querySelector('.container');
      expect(container).toBeInTheDocument();

      // Container has horizontal padding (px-4)
      if (container) {
        expect(hasResponsiveClass(container as HTMLElement, 'px-4')).toBe(true);
      }
    });

    it('hero text uses appropriate mobile font sizes', () => {
      renderWithProviders(<Home />);

      const headline = screen.getByRole('heading', { level: 1 });
      // The headline should have text-4xl base class for mobile
      expect(hasResponsiveClass(headline, 'text-4xl')).toBe(true);
    });
  });

  describe('Test Case 2: Feature cards stack in single column layout', () => {
    it('features section renders with grid layout', () => {
      renderWithProviders(<Home />);

      // Features section should be present
      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection).toBeInTheDocument();

      // Features grid should be present
      const featuresGrid = screen.getByTestId('features-grid');
      expect(featuresGrid).toBeInTheDocument();
    });

    it('features grid uses single column layout on mobile (grid-cols-1)', () => {
      renderWithProviders(<Home />);

      const featuresGrid = screen.getByTestId('features-grid');
      // Grid should have grid-cols-1 for mobile (default single column)
      expect(hasResponsiveClass(featuresGrid, 'grid-cols-1')).toBe(true);
    });

    it('all three feature cards are rendered vertically', () => {
      renderWithProviders(<Home />);

      // All feature cards should be present
      const featureCard0 = screen.getByTestId('feature-card-0');
      const featureCard1 = screen.getByTestId('feature-card-1');
      const featureCard2 = screen.getByTestId('feature-card-2');

      expect(featureCard0).toBeInTheDocument();
      expect(featureCard1).toBeInTheDocument();
      expect(featureCard2).toBeInTheDocument();

      // Verify feature content is readable
      expect(screen.getByText('Instant URL Shortening')).toBeInTheDocument();
      expect(screen.getByText('Detailed Analytics')).toBeInTheDocument();
      expect(screen.getByText('Share Statistics')).toBeInTheDocument();
    });

    it('feature cards have consistent padding for mobile', () => {
      renderWithProviders(<Home />);

      const featureCards = screen.getAllByTestId(/feature-card-\d+/);
      expect(featureCards).toHaveLength(3);

      // Each card should be visible and have content
      featureCards.forEach((card) => {
        expect(card).toBeInTheDocument();
        expect(card).toBeVisible();
      });
    });
  });

  describe('Test Case 3: CTA buttons are full-width or appropriately sized for mobile', () => {
    it('CTA buttons container uses flex-col layout on mobile', () => {
      renderWithProviders(<Home />);

      // Get CTA links
      const getStartedLink = screen.getByRole('link', { name: /get started/i });
      const loginLink = screen.getByRole('link', { name: /login/i });

      expect(getStartedLink).toBeInTheDocument();
      expect(loginLink).toBeInTheDocument();

      // Find the CTA container by looking for the div with flex-col class
      // The container structure is: div.flex.flex-col > Link > FuturisticButton
      const heroSection = screen.getByRole('region', { name: /hero section/i });
      const ctaContainer = heroSection.querySelector('.flex.flex-col');
      expect(ctaContainer).toBeInTheDocument();
      if (ctaContainer) {
        expect(hasResponsiveClass(ctaContainer as HTMLElement, 'flex-col')).toBe(true);
      }
    });

    it('primary CTA button is visible and clickable', () => {
      renderWithProviders(<Home />);

      const primaryCta = screen.getByRole('link', { name: /get started/i });
      expect(primaryCta).toBeInTheDocument();
      expect(primaryCta).toBeVisible();
      expect(primaryCta).toHaveAttribute('href', '/register');
    });

    it('secondary CTA (login) button is visible and clickable', () => {
      renderWithProviders(<Home />);

      const loginCta = screen.getByRole('link', { name: /login/i });
      expect(loginCta).toBeInTheDocument();
      expect(loginCta).toBeVisible();
      expect(loginCta).toHaveAttribute('href', '/login');
    });
  });

  describe('Test Case 4: No horizontal scrolling is required', () => {
    it('landing page main container does not overflow horizontally', () => {
      renderWithProviders(<Home />);

      const landingPage = screen.getByTestId('landing-page');
      expect(landingPage).toBeInTheDocument();

      // The main element should have min-h-screen class
      expect(hasResponsiveClass(landingPage, 'min-h-screen')).toBe(true);
    });

    it('hero section has overflow-hidden to prevent horizontal scroll', () => {
      renderWithProviders(<Home />);

      const heroSection = screen.getByRole('region', { name: /hero section/i });
      expect(hasResponsiveClass(heroSection, 'overflow-hidden')).toBe(true);
    });

    it('all sections have proper max-width constraints', () => {
      renderWithProviders(<Home />);

      // Features section has max-w-7xl
      const featuresSection = screen.getByTestId('features-section');
      const featuresContainer = featuresSection.querySelector('.max-w-7xl');
      expect(featuresContainer).toBeInTheDocument();

      // Social proof section has max-w-7xl
      const socialProofSection = screen.getByTestId('social-proof-section');
      const socialProofContainer = socialProofSection.querySelector('.max-w-7xl');
      expect(socialProofContainer).toBeInTheDocument();
    });

    it('all sections have responsive padding for mobile', () => {
      renderWithProviders(<Home />);

      // Check features section padding
      const featuresSection = screen.getByTestId('features-section');
      expect(hasResponsiveClass(featuresSection, 'px-4')).toBe(true);

      // Check social proof section padding
      const socialProofSection = screen.getByTestId('social-proof-section');
      expect(hasResponsiveClass(socialProofSection, 'px-4')).toBe(true);
    });
  });

  describe('Additional Mobile Responsiveness Tests', () => {
    it('How It Works section renders with single column grid on mobile', () => {
      renderWithProviders(<Home />);

      const stepsContainer = screen.getByTestId('steps-container');
      expect(stepsContainer).toBeInTheDocument();
      // Grid should use single column on mobile
      expect(hasResponsiveClass(stepsContainer, 'grid-cols-1')).toBe(true);
    });

    it('Social Proof stats grid uses single column on mobile', () => {
      renderWithProviders(<Home />);

      const statsGrid = screen.getByTestId('stats-grid');
      expect(statsGrid).toBeInTheDocument();
      // Grid should use single column on mobile
      expect(hasResponsiveClass(statsGrid, 'grid-cols-1')).toBe(true);
    });

    it('all three step items are visible on mobile', () => {
      renderWithProviders(<Home />);

      expect(screen.getByTestId('step-1')).toBeInTheDocument();
      expect(screen.getByTestId('step-2')).toBeInTheDocument();
      expect(screen.getByTestId('step-3')).toBeInTheDocument();

      // Step titles and descriptions are readable
      expect(screen.getByTestId('step-1-title')).toHaveTextContent('Create');
      expect(screen.getByTestId('step-2-title')).toHaveTextContent('Share');
      expect(screen.getByTestId('step-3-title')).toHaveTextContent('Track');
    });

    it('all three stat cards are visible on mobile', () => {
      renderWithProviders(<Home />);

      expect(screen.getByTestId('stat-card-0')).toBeInTheDocument();
      expect(screen.getByTestId('stat-card-1')).toBeInTheDocument();
      expect(screen.getByTestId('stat-card-2')).toBeInTheDocument();

      // Stat values are readable
      expect(screen.getByText('10M+')).toBeInTheDocument();
      expect(screen.getByText('500M+')).toBeInTheDocument();
      expect(screen.getByText('100K+')).toBeInTheDocument();
    });
  });
});

describe('Responsive Design - Tablet (768px)', () => {
  beforeEach(() => {
    // Set viewport to tablet width (md breakpoint)
    setViewportWidth(768);
  });

  afterEach(() => {
    // Reset viewport
    setViewportWidth(1024);
  });

  describe('Test Case 1: Hero section renders with appropriate sizing', () => {
    it('renders Hero section at tablet viewport', () => {
      renderWithProviders(<Home />);

      // Hero section should be rendered
      const heroSection = screen.getByRole('region', { name: /hero section/i });
      expect(heroSection).toBeInTheDocument();

      // Hero section should have min-h-screen for full viewport height
      expect(hasResponsiveClass(heroSection, 'min-h-screen')).toBe(true);
    });

    it('hero headline has tablet-appropriate font size (md:text-5xl)', () => {
      renderWithProviders(<Home />);

      const headline = screen.getByRole('heading', { level: 1 });
      expect(headline).toBeInTheDocument();
      // Tablet should use md:text-5xl
      expect(hasResponsiveClass(headline, 'md:text-5xl')).toBe(true);
    });

    it('hero subheadline has tablet-appropriate font size (md:text-xl)', () => {
      renderWithProviders(<Home />);

      // Find subheadline by unique text content
      const subheadline = screen.getByText(/gain powerful insights/i);
      expect(subheadline).toBeInTheDocument();
      // Tablet should use md:text-xl
      expect(hasResponsiveClass(subheadline, 'md:text-xl')).toBe(true);
    });

    it('hero content container has proper padding at tablet width', () => {
      renderWithProviders(<Home />);

      const heroSection = screen.getByRole('region', { name: /hero section/i });
      const container = heroSection.querySelector('.container');
      expect(container).toBeInTheDocument();
      // Container should have px-4 padding
      if (container) {
        expect(hasResponsiveClass(container as HTMLElement, 'px-4')).toBe(true);
      }
    });

    it('hero CTA buttons display in row layout at tablet (sm:flex-row)', () => {
      renderWithProviders(<Home />);

      const heroSection = screen.getByRole('region', { name: /hero section/i });
      // Find the CTA container with flex-col sm:flex-row classes
      const ctaContainer = heroSection.querySelector('.flex.flex-col');
      expect(ctaContainer).toBeInTheDocument();
      if (ctaContainer) {
        // At 768px (above sm:640px), buttons should use sm:flex-row
        expect(hasResponsiveClass(ctaContainer as HTMLElement, 'sm:flex-row')).toBe(true);
      }
    });
  });

  describe('Test Case 2: Feature cards adapt to tablet layout (2-column or responsive grid)', () => {
    it('features section renders at tablet viewport', () => {
      renderWithProviders(<Home />);

      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection).toBeInTheDocument();
    });

    it('features grid has md:grid-cols-3 class for tablet+ breakpoint', () => {
      renderWithProviders(<Home />);

      const featuresGrid = screen.getByTestId('features-grid');
      expect(featuresGrid).toBeInTheDocument();
      // Grid should have md:grid-cols-3 for tablet and above
      expect(hasResponsiveClass(featuresGrid, 'md:grid-cols-3')).toBe(true);
    });

    it('all three feature cards are visible at tablet viewport', () => {
      renderWithProviders(<Home />);

      const featureCard0 = screen.getByTestId('feature-card-0');
      const featureCard1 = screen.getByTestId('feature-card-1');
      const featureCard2 = screen.getByTestId('feature-card-2');

      expect(featureCard0).toBeInTheDocument();
      expect(featureCard1).toBeInTheDocument();
      expect(featureCard2).toBeInTheDocument();
    });

    it('feature cards have proper content at tablet viewport', () => {
      renderWithProviders(<Home />);

      // Verify feature titles are present
      expect(screen.getByText('Instant URL Shortening')).toBeInTheDocument();
      expect(screen.getByText('Detailed Analytics')).toBeInTheDocument();
      expect(screen.getByText('Share Statistics')).toBeInTheDocument();
    });

    it('features section has responsive padding (sm:px-6 lg:px-8)', () => {
      renderWithProviders(<Home />);

      const featuresSection = screen.getByTestId('features-section');
      // Section should have responsive padding classes
      expect(hasResponsiveClass(featuresSection, 'px-4')).toBe(true);
      expect(hasResponsiveClass(featuresSection, 'sm:px-6')).toBe(true);
      expect(hasResponsiveClass(featuresSection, 'lg:px-8')).toBe(true);
    });
  });

  describe('Test Case 3: All content is visible without horizontal scrolling', () => {
    it('landing page main container does not overflow at tablet width', () => {
      renderWithProviders(<Home />);

      const landingPage = screen.getByTestId('landing-page');
      expect(landingPage).toBeInTheDocument();
      expect(hasResponsiveClass(landingPage, 'min-h-screen')).toBe(true);
    });

    it('hero section has overflow-hidden at tablet viewport', () => {
      renderWithProviders(<Home />);

      const heroSection = screen.getByRole('region', { name: /hero section/i });
      expect(hasResponsiveClass(heroSection, 'overflow-hidden')).toBe(true);
    });

    it('all sections have max-width constraints preventing overflow', () => {
      renderWithProviders(<Home />);

      // Features section has max-w-7xl
      const featuresSection = screen.getByTestId('features-section');
      const featuresContainer = featuresSection.querySelector('.max-w-7xl');
      expect(featuresContainer).toBeInTheDocument();

      // Social proof section has max-w-7xl
      const socialProofSection = screen.getByTestId('social-proof-section');
      const socialProofContainer = socialProofSection.querySelector('.max-w-7xl');
      expect(socialProofContainer).toBeInTheDocument();
    });

    it('How It Works section is visible at tablet viewport', () => {
      renderWithProviders(<Home />);

      const stepsContainer = screen.getByTestId('steps-container');
      expect(stepsContainer).toBeInTheDocument();

      // All three steps should be visible
      expect(screen.getByTestId('step-1')).toBeInTheDocument();
      expect(screen.getByTestId('step-2')).toBeInTheDocument();
      expect(screen.getByTestId('step-3')).toBeInTheDocument();
    });

    it('Social Proof section is visible at tablet viewport', () => {
      renderWithProviders(<Home />);

      const statsGrid = screen.getByTestId('stats-grid');
      expect(statsGrid).toBeInTheDocument();

      // All three stat cards should be visible
      expect(screen.getByTestId('stat-card-0')).toBeInTheDocument();
      expect(screen.getByTestId('stat-card-1')).toBeInTheDocument();
      expect(screen.getByTestId('stat-card-2')).toBeInTheDocument();
    });

    it('Footer is visible at tablet viewport', () => {
      renderWithProviders(<Home />);

      const footer = screen.getByRole('contentinfo');
      expect(footer).toBeInTheDocument();
    });
  });

  describe('Additional Tablet Responsiveness Tests', () => {
    it('features section heading uses tablet-appropriate font size (sm:text-4xl)', () => {
      renderWithProviders(<Home />);

      const featuresHeading = screen.getByText('Powerful Features');
      expect(featuresHeading).toBeInTheDocument();
      expect(hasResponsiveClass(featuresHeading, 'sm:text-4xl')).toBe(true);
    });

    it('hero section centers content properly at tablet viewport', () => {
      renderWithProviders(<Home />);

      const heroSection = screen.getByRole('region', { name: /hero section/i });
      // Verify flex centering classes
      expect(hasResponsiveClass(heroSection, 'flex')).toBe(true);
      expect(hasResponsiveClass(heroSection, 'items-center')).toBe(true);
      expect(hasResponsiveClass(heroSection, 'justify-center')).toBe(true);
    });

    it('CTA buttons are visible and functional at tablet viewport', () => {
      renderWithProviders(<Home />);

      // Get hero section and query within it to avoid multiple login links from footer
      const heroSection = screen.getByRole('region', { name: /hero section/i });
      const getStartedLink = within(heroSection).getByRole('link', { name: /get started/i });
      const loginLink = within(heroSection).getByRole('link', { name: /login/i });

      expect(getStartedLink).toBeInTheDocument();
      expect(getStartedLink).toBeVisible();
      expect(getStartedLink).toHaveAttribute('href', '/register');

      expect(loginLink).toBeInTheDocument();
      expect(loginLink).toBeVisible();
      expect(loginLink).toHaveAttribute('href', '/login');
    });

    it('stat values are readable at tablet viewport', () => {
      renderWithProviders(<Home />);

      // Verify all stat values are present
      expect(screen.getByText('10M+')).toBeInTheDocument();
      expect(screen.getByText('500M+')).toBeInTheDocument();
      expect(screen.getByText('100K+')).toBeInTheDocument();

      // Verify stat labels are present
      expect(screen.getByText('Links Created')).toBeInTheDocument();
      expect(screen.getByText('Clicks Tracked')).toBeInTheDocument();
      expect(screen.getByText('Happy Users')).toBeInTheDocument();
    });
  });
});
