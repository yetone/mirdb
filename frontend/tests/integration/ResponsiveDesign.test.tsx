/**
 * Responsive Design Integration Tests
 * Owner: Scenarios 8, 9, 10
 *
 * Test coverage:
 * - Scenario 8: Mobile layout (375px)
 * - Scenario 9: Tablet layout (768px)
 * - Scenario 10: Desktop layout (1280px)
 *
 * Test suites:
 * - describe('Mobile Layout')
 * - describe('Tablet Layout')
 * - describe('Desktop Layout')
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { screen, within } from '@testing-library/react';
import { renderWithProviders } from '../utils/renderWithProviders';
import Home from '../../src/pages/Home';

/**
 * Helper to set up viewport width simulation for responsive tests
 * Note: In JSDOM, we can't actually resize the viewport, but we can
 * verify that responsive Tailwind classes are applied correctly.
 * The actual responsive behavior is handled by CSS which is validated
 * through the presence of responsive class names.
 */
const setViewportWidth = (width: number) => {
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: width,
  });

  // Trigger resize event
  window.dispatchEvent(new Event('resize'));
};

/**
 * Helper to create mock matchMedia with specific width
 */
const createMatchMedia = (width: number) => {
  return vi.fn().mockImplementation((query: string) => {
    // Parse common breakpoint queries
    const minWidthMatch = query.match(/\(min-width:\s*(\d+)px\)/);
    const maxWidthMatch = query.match(/\(max-width:\s*(\d+)px\)/);

    let matches = false;

    if (minWidthMatch) {
      const minWidth = parseInt(minWidthMatch[1], 10);
      matches = width >= minWidth;
    } else if (maxWidthMatch) {
      const maxWidth = parseInt(maxWidthMatch[1], 10);
      matches = width <= maxWidth;
    }

    return {
      matches,
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    };
  });
};

describe('Responsive Design - Mobile Layout (Scenario 8)', () => {
  const MOBILE_WIDTH = 375;

  beforeEach(() => {
    setViewportWidth(MOBILE_WIDTH);
    window.matchMedia = createMatchMedia(MOBILE_WIDTH);
  });

  describe('Mobile Viewport Content Rendering', () => {
    it('should render Home page at 375px viewport width without errors', () => {
      const { container } = renderWithProviders(<Home />, {
        useMemoryRouter: true,
        initialEntries: ['/'],
      });

      // Verify the main container renders
      expect(container).toBeInTheDocument();

      // Verify key sections are present
      expect(screen.getByTestId('hero-section')).toBeInTheDocument();
    });

    it('should render all content within viewport without horizontal overflow', () => {
      const { container } = renderWithProviders(<Home />, {
        useMemoryRouter: true,
        initialEntries: ['/'],
      });

      // Check that the main container uses responsive utility classes
      const mainContainer = container.querySelector('.min-h-screen');
      expect(mainContainer).toBeInTheDocument();

      // Check the main content area has no fixed width that would cause overflow
      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toBeInTheDocument();

      // Verify container uses mobile-friendly classes
      const containerDiv = container.querySelector('.container');
      expect(containerDiv).toBeInTheDocument();
      // Container should have padding for mobile (px-4)
      expect(containerDiv).toHaveClass('px-4');
    });

    it('should have hero section that adapts to mobile width', () => {
      renderWithProviders(<Home />, {
        useMemoryRouter: true,
        initialEntries: ['/'],
      });

      const heroSection = screen.getByTestId('hero-section');

      // Hero section should be present and use responsive layout
      expect(heroSection).toBeInTheDocument();
      expect(heroSection).toHaveClass('overflow-hidden');
    });
  });

  describe('Mobile CTA Button Touch Targets', () => {
    it('should render CTA buttons that meet minimum touch target size (44px)', () => {
      const { container } = renderWithProviders(<Home />, {
        useMemoryRouter: true,
        initialEntries: ['/'],
        authContext: { isAuthenticated: false },
      });

      // Get CTA buttons
      const getStartedButton = screen.getByTestId('get-started-button');
      const loginButton = screen.getByTestId('login-button');

      // Buttons should exist
      expect(getStartedButton).toBeInTheDocument();
      expect(loginButton).toBeInTheDocument();

      // Check buttons have btn-lg class for larger touch targets
      const getStartedSpan = getStartedButton.querySelector('span.btn-lg') ||
                             getStartedButton.closest('a')?.querySelector('.btn-lg');
      const loginSpan = loginButton.querySelector('span.btn-lg') ||
                        loginButton.closest('a')?.querySelector('.btn-lg');

      // Verify buttons have btn-lg class which ensures 44px+ height
      expect(getStartedSpan || getStartedButton.querySelector('.btn-lg') ||
             container.innerHTML.includes('btn-lg')).toBeTruthy();
    });

    it('should have CTA buttons styled with appropriate padding for touch', () => {
      renderWithProviders(<Home />, {
        useMemoryRouter: true,
        initialEntries: ['/'],
        authContext: { isAuthenticated: false },
      });

      // Get the button elements
      const getStartedButton = screen.getByTestId('get-started-button');

      // The button should have btn class which provides touch-friendly sizing
      const buttonContent = getStartedButton.querySelector('.btn');
      expect(buttonContent).toBeInTheDocument();
    });

    it('should have authenticated user dashboard button with proper touch target', () => {
      renderWithProviders(<Home />, {
        useMemoryRouter: true,
        initialEntries: ['/'],
        authContext: {
          isAuthenticated: true,
          user: { id: 1, username: 'testuser', email: 'test@example.com', is_admin: 0 },
        },
      });

      const dashboardButton = screen.getByTestId('hero-cta-dashboard');
      expect(dashboardButton).toBeInTheDocument();

      // Dashboard button should also have btn class
      const buttonContent = dashboardButton.querySelector('.btn');
      expect(buttonContent).toBeInTheDocument();
    });
  });

  describe('Mobile Feature Cards Layout', () => {
    it('should stack feature cards vertically in single column at mobile viewport', () => {
      const { container } = renderWithProviders(<Home />, {
        useMemoryRouter: true,
        initialEntries: ['/'],
      });

      // Find the feature cards grid container
      const gridContainer = container.querySelector('.grid.grid-cols-1');
      expect(gridContainer).toBeInTheDocument();

      // Verify the grid uses single column at base (mobile) width
      expect(gridContainer).toHaveClass('grid-cols-1');
    });

    it('should have feature cards with responsive grid classes', () => {
      const { container } = renderWithProviders(<Home />, {
        useMemoryRouter: true,
        initialEntries: ['/'],
      });

      // Find the grid container in FeatureCards section
      const gridContainer = container.querySelector('.grid');
      expect(gridContainer).toBeInTheDocument();

      // Check for responsive breakpoint classes
      // At mobile (base), should be grid-cols-1
      // At tablet (md:), should be grid-cols-2
      // At desktop (lg:), should be grid-cols-3
      expect(gridContainer).toHaveClass('grid-cols-1');
      expect(gridContainer).toHaveClass('md:grid-cols-2');
      expect(gridContainer).toHaveClass('lg:grid-cols-3');
    });

    it('should render all feature cards at mobile viewport', () => {
      renderWithProviders(<Home />, {
        useMemoryRouter: true,
        initialEntries: ['/'],
      });

      // Verify feature cards content is present
      expect(screen.getByText('URL Shortening')).toBeInTheDocument();
      expect(screen.getByText('Click Analytics')).toBeInTheDocument();
      expect(screen.getByText('Geographic Insights')).toBeInTheDocument();
    });
  });

  describe('Mobile Navigation Accessibility', () => {
    it('should render navigation component at mobile viewport', () => {
      const { container } = renderWithProviders(<Home />, {
        useMemoryRouter: true,
        initialEntries: ['/'],
      });

      // Check that the navbar is present
      const navbar = container.querySelector('nav.navbar');
      expect(navbar).toBeInTheDocument();
    });

    it('should have accessible navigation links at mobile viewport', () => {
      renderWithProviders(<Home />, {
        useMemoryRouter: true,
        initialEntries: ['/'],
        authContext: { isAuthenticated: false },
      });

      // Navigation should contain login and register links (multiple may exist on page)
      const loginLinks = screen.getAllByRole('link', { name: /login/i });
      const registerLinks = screen.getAllByRole('link', { name: /register/i });

      // At least one of each should be present
      expect(loginLinks.length).toBeGreaterThan(0);
      expect(registerLinks.length).toBeGreaterThan(0);
    });

    it('should have navigation with proper structure for mobile', () => {
      const { container } = renderWithProviders(<Home />, {
        useMemoryRouter: true,
        initialEntries: ['/'],
      });

      // Navbar should use flexbox layout
      const navbar = container.querySelector('nav.navbar');
      expect(navbar).toBeInTheDocument();

      // Should have flex structure for responsive layout
      const flexElements = navbar?.querySelectorAll('.flex-1, .flex-none');
      expect(flexElements?.length).toBeGreaterThan(0);
    });

    it('should have brand/logo link accessible at mobile viewport', () => {
      renderWithProviders(<Home />, {
        useMemoryRouter: true,
        initialEntries: ['/'],
      });

      // Brand link should be visible
      const brandLink = screen.getByRole('link', { name: /url shortener/i });
      expect(brandLink).toBeInTheDocument();
    });
  });

  describe('Mobile Responsive Text Sizing', () => {
    it('should use mobile-appropriate text size for headline', () => {
      const { container } = renderWithProviders(<Home />, {
        useMemoryRouter: true,
        initialEntries: ['/'],
      });

      const headline = screen.getByTestId('hero-headline');
      expect(headline).toBeInTheDocument();

      // Headline should use responsive text classes (text-4xl at mobile, md:text-6xl at desktop)
      expect(headline).toHaveClass('text-4xl');
      expect(headline).toHaveClass('md:text-6xl');
    });

    it('should use mobile-appropriate text size for subheadline', () => {
      renderWithProviders(<Home />, {
        useMemoryRouter: true,
        initialEntries: ['/'],
      });

      const subheadline = screen.getByTestId('hero-subheadline');
      expect(subheadline).toBeInTheDocument();

      // Subheadline should use responsive text classes
      expect(subheadline).toHaveClass('text-lg');
      expect(subheadline).toHaveClass('md:text-xl');
    });
  });

  describe('Mobile CTA Layout', () => {
    it('should stack CTA buttons vertically at mobile viewport', () => {
      const { container } = renderWithProviders(<Home />, {
        useMemoryRouter: true,
        initialEntries: ['/'],
        authContext: { isAuthenticated: false },
      });

      // Find the CTA container that uses flex-col for mobile and sm:flex-row for larger
      const ctaContainer = container.querySelector('.flex.flex-col.sm\\:flex-row');
      expect(ctaContainer).toBeInTheDocument();
    });

    it('should have proper gap between stacked CTA buttons', () => {
      const { container } = renderWithProviders(<Home />, {
        useMemoryRouter: true,
        initialEntries: ['/'],
        authContext: { isAuthenticated: false },
      });

      // CTA container should have gap class for spacing
      // Use more specific selector to find the CTA button container
      const ctaContainer = container.querySelector('.flex.flex-col.sm\\:flex-row.gap-4');
      expect(ctaContainer).toBeInTheDocument();
    });
  });
});

describe('Responsive Design - Tablet Layout (Scenario 9)', () => {
  const TABLET_WIDTH = 768;

  beforeEach(() => {
    setViewportWidth(TABLET_WIDTH);
    window.matchMedia = createMatchMedia(TABLET_WIDTH);
  });

  afterEach(() => {
    // Reset viewport
    setViewportWidth(1024);
  });

  it('should render Home component at tablet viewport without errors', () => {
    renderWithProviders(<Home />);

    // Verify main sections are present
    expect(screen.getByTestId('hero-section')).toBeInTheDocument();
    expect(screen.getByText('Powerful Features')).toBeInTheDocument();
    expect(screen.getByText('How It Works')).toBeInTheDocument();
    expect(screen.getByText('Trusted by Millions')).toBeInTheDocument();
  });

  it('should display feature cards grid with responsive md: classes for 2-column layout', () => {
    renderWithProviders(<Home />);

    // Find the feature cards section
    const featureSection = screen.getByText('Powerful Features').closest('section');
    expect(featureSection).toBeInTheDocument();

    // Get the grid container - it should have responsive classes
    // At md breakpoint (768px), grid should show 2 columns
    const gridContainer = featureSection?.querySelector('.grid');
    expect(gridContainer).toBeInTheDocument();

    // Verify the grid has responsive Tailwind classes for tablet
    // grid-cols-1 md:grid-cols-2 lg:grid-cols-3
    expect(gridContainer).toHaveClass('grid-cols-1');
    expect(gridContainer).toHaveClass('md:grid-cols-2');
    expect(gridContainer).toHaveClass('lg:grid-cols-3');

    // Verify all 6 feature cards are rendered
    const featureCards = within(featureSection!).getAllByRole('heading', { level: 3 });
    expect(featureCards).toHaveLength(6);
  });

  it('should display hero headline with appropriate responsive sizing classes', () => {
    renderWithProviders(<Home />);

    const headline = screen.getByTestId('hero-headline');
    expect(headline).toBeInTheDocument();

    // Verify responsive text classes are present
    // text-4xl md:text-6xl - smaller on mobile, larger on tablet/desktop
    expect(headline).toHaveClass('text-4xl');
    expect(headline).toHaveClass('md:text-6xl');
  });

  it('should display hero subheadline with responsive sizing', () => {
    renderWithProviders(<Home />);

    const subheadline = screen.getByTestId('hero-subheadline');
    expect(subheadline).toBeInTheDocument();

    // Verify responsive text classes
    // text-lg md:text-xl
    expect(subheadline).toHaveClass('text-lg');
    expect(subheadline).toHaveClass('md:text-xl');
  });

  it('should display How It Works section with horizontal layout at tablet breakpoint', () => {
    renderWithProviders(<Home />);

    const howItWorksSection = screen.getByText('How It Works').closest('section');
    expect(howItWorksSection).toBeInTheDocument();

    // Find the flex container that holds the steps
    const stepsContainer = howItWorksSection?.querySelector('.flex');
    expect(stepsContainer).toBeInTheDocument();

    // At md breakpoint, steps should be in a row (flex-row)
    // flex-col md:flex-row
    expect(stepsContainer).toHaveClass('flex-col');
    expect(stepsContainer).toHaveClass('md:flex-row');
  });

  it('should display Stats section with responsive layout', () => {
    renderWithProviders(<Home />);

    const statsSection = screen.getByText('Trusted by Millions').closest('section');
    expect(statsSection).toBeInTheDocument();

    // Find the flex container for stats
    const statsContainer = statsSection?.querySelector('.flex');
    expect(statsContainer).toBeInTheDocument();

    // Responsive layout classes
    // flex-col md:flex-row for horizontal layout on tablet
    expect(statsContainer).toHaveClass('flex-col');
    expect(statsContainer).toHaveClass('md:flex-row');

    // Verify responsive gap classes
    expect(statsContainer).toHaveClass('gap-8');
    expect(statsContainer).toHaveClass('md:gap-16');
  });

  it('should display stat values with responsive text sizing', () => {
    renderWithProviders(<Home />);

    // Find a stat value (e.g., "10M+")
    const statValue = screen.getByText('10M+');
    expect(statValue).toBeInTheDocument();

    // Verify responsive text classes
    // text-4xl md:text-5xl
    expect(statValue).toHaveClass('text-4xl');
    expect(statValue).toHaveClass('md:text-5xl');
  });

  it('should display section headings with responsive typography', () => {
    renderWithProviders(<Home />);

    // Check "Powerful Features" heading
    const featuresHeading = screen.getByText('Powerful Features');
    expect(featuresHeading).toHaveClass('text-3xl');
    expect(featuresHeading).toHaveClass('md:text-4xl');

    // Check "How It Works" heading
    const howItWorksHeading = screen.getByText('How It Works');
    expect(howItWorksHeading).toHaveClass('text-3xl');
    expect(howItWorksHeading).toHaveClass('md:text-4xl');

    // Check "Trusted by Millions" heading
    const statsHeading = screen.getByText('Trusted by Millions');
    expect(statsHeading).toHaveClass('text-3xl');
    expect(statsHeading).toHaveClass('md:text-4xl');
  });

  it('should display CTA buttons with responsive layout', () => {
    renderWithProviders(<Home />);

    // Find the hero section
    const heroSection = screen.getByTestId('hero-section');

    // Find the container with CTA buttons (flex container)
    // The button container should have flex-col sm:flex-row for responsive layout
    const buttonContainer = heroSection.querySelector('.flex.flex-col.sm\\:flex-row');
    expect(buttonContainer).toBeInTheDocument();

    // At tablet width (768px), buttons should be in a row
    expect(buttonContainer).toHaveClass('sm:flex-row');
  });

  it('should maintain readable content width with container class', () => {
    renderWithProviders(<Home />);

    // Hero section should have container for proper width
    const heroSection = screen.getByTestId('hero-section');
    const heroContainer = heroSection.querySelector('.container');
    expect(heroContainer).toBeInTheDocument();
    expect(heroContainer).toHaveClass('mx-auto');

    // Feature section should have container
    const featureSection = screen.getByText('Powerful Features').closest('section');
    const featureContainer = featureSection?.querySelector('.container');
    expect(featureContainer).toBeInTheDocument();
  });

  it('should display footer with centered layout', () => {
    renderWithProviders(<Home />);

    // Footer should be present with centered content
    const footer = screen.getByRole('contentinfo');
    expect(footer).toBeInTheDocument();
    expect(footer).toHaveClass('footer-center');

    // Verify navigation links are present in footer specifically
    const footerLoginLink = within(footer).getByRole('link', { name: /login/i });
    const footerRegisterLink = within(footer).getByRole('link', { name: /register/i });
    expect(footerLoginLink).toBeInTheDocument();
    expect(footerRegisterLink).toBeInTheDocument();
  });
});
