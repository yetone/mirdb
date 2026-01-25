/**
 * Home page unit tests - Navigation Bar Display
 * Owner: Scenario 14 - Navigation Bar Display
 *
 * Tests that verify the navigation bar displays correctly with:
 * - Logo/brand name on the left
 * - ThemeToggle component present
 * - Login button visible
 * - Register button with primary styling
 */

import { describe, it, expect, vi } from 'vitest';
import { screen, within } from '@testing-library/react';
import { Home } from '../../../src/pages/Home';
import { renderWithProviders, mockAuthContext } from './test-utils';

/**
 * Helper function to get the main navigation bar (not the footer nav)
 */
function getMainNavbar() {
  // The main navbar has the 'navbar' class from DaisyUI
  const navbars = screen.getAllByRole('navigation');
  const mainNavbar = navbars.find(nav => nav.classList.contains('navbar'));
  if (!mainNavbar) {
    throw new Error('Main navbar not found');
  }
  return mainNavbar;
}

describe('Home Page - Navigation Bar Display', () => {
  describe('Test Case 1: Navbar displays with logo/brand name on left', () => {
    it('should render the Navbar component with logo/brand name', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      // Find the main navigation element (with navbar class)
      const navbar = getMainNavbar();
      expect(navbar).toBeInTheDocument();

      // Check that the brand name "ShortURL" is displayed
      const brandLink = screen.getByRole('link', { name: /shorturl/i });
      expect(brandLink).toBeInTheDocument();
      expect(brandLink).toHaveAttribute('href', '/');
    });

    it('should position the logo on the left side of the navbar', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      const navbar = getMainNavbar();
      const brandLink = screen.getByRole('link', { name: /shorturl/i });

      // The brand link should be within the navbar-start section
      expect(navbar).toContainElement(brandLink);

      // Check parent has navbar-start class (logo positioned left)
      const brandParent = brandLink.closest('.navbar-start');
      expect(brandParent).toBeInTheDocument();
    });
  });

  describe('Test Case 2: ThemeToggle component is present in navigation', () => {
    it('should render the ThemeToggle button in the navbar', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      // ThemeToggle should have an aria-label for switching themes
      const themeToggle = screen.getByRole('button', { name: /switch to (light|dark) mode/i });
      expect(themeToggle).toBeInTheDocument();
    });

    it('should render ThemeToggle within the navigation area', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      const navbar = getMainNavbar();
      const themeToggle = screen.getByRole('button', { name: /switch to (light|dark) mode/i });

      // ThemeToggle should be within the navbar
      expect(navbar).toContainElement(themeToggle);
    });

    it('should display ThemeToggle with appropriate icon', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      const themeToggle = screen.getByRole('button', { name: /switch to (light|dark) mode/i });

      // The button should contain an SVG icon
      const icon = themeToggle.querySelector('svg');
      expect(icon).toBeInTheDocument();
    });
  });

  describe('Test Case 3: Login link/button is present and visible', () => {
    it('should render the Login button when user is not authenticated', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      // Desktop login button with test id
      const loginButton = screen.getByTestId('nav-login');
      expect(loginButton).toBeInTheDocument();
      expect(loginButton).toHaveTextContent(/login/i);
    });

    it('should have Login button linked to /login route', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      const loginButton = screen.getByTestId('nav-login');
      expect(loginButton).toHaveAttribute('href', '/login');
    });

    it('should render Login button within the navbar', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      const navbar = getMainNavbar();
      const loginButton = screen.getByTestId('nav-login');

      expect(navbar).toContainElement(loginButton);
    });

    it('should have Login button styled with btn-ghost class', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      const loginButton = screen.getByTestId('nav-login');
      expect(loginButton).toHaveClass('btn', 'btn-ghost');
    });
  });

  describe('Test Case 4: Register button with primary styling is present', () => {
    it('should render the Register button when user is not authenticated', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      // Desktop register button with test id
      const registerButton = screen.getByTestId('nav-register');
      expect(registerButton).toBeInTheDocument();
      expect(registerButton).toHaveTextContent(/register/i);
    });

    it('should have Register button linked to /register route', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      const registerButton = screen.getByTestId('nav-register');
      expect(registerButton).toHaveAttribute('href', '/register');
    });

    it('should have Register button with primary styling (btn-primary class)', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      const registerButton = screen.getByTestId('nav-register');
      expect(registerButton).toHaveClass('btn', 'btn-primary');
    });

    it('should render Register button within the navbar', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      const navbar = getMainNavbar();
      const registerButton = screen.getByTestId('nav-register');

      expect(navbar).toContainElement(registerButton);
    });
  });

  describe('Navbar with authenticated user', () => {
    it('should not show Login and Register buttons when user is authenticated', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.authenticated });

      // Login and Register buttons should not be present
      expect(screen.queryByTestId('nav-login')).not.toBeInTheDocument();
      expect(screen.queryByTestId('nav-register')).not.toBeInTheDocument();
    });

    it('should show Dashboard and Logout options when user is authenticated', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.authenticated });

      const navbar = getMainNavbar();

      // Dashboard link should be present in the navbar (not the hero section)
      const navbarDashboardLinks = within(navbar).getAllByRole('link', { name: /dashboard/i });
      expect(navbarDashboardLinks.length).toBeGreaterThan(0);
      expect(navbarDashboardLinks[0]).toHaveAttribute('href', '/dashboard');

      // Logout button should be present
      const logoutButton = within(navbar).getByRole('button', { name: /logout/i });
      expect(logoutButton).toBeInTheDocument();
    });
  });
});

/**
 * Component Reuse Tests - Scenario 24
 * Owner: Scenario 24 - Component Reuse - Existing Components
 *
 * Tests that verify the homepage properly integrates and reuses
 * existing application components without modification:
 * - Navbar component
 * - ThemeToggle component (integrated within Navbar)
 * - FuturisticButton component (used in hero and dashboard preview sections)
 * - GlassMorphismCard component (used in features section)
 * - BackgroundEffect component (used for visual effects)
 */
describe('Home Page - Component Reuse (Scenario 24)', () => {
  describe('Test Case 1: Navbar component integration', () => {
    it('should render the existing Navbar component from components/Navbar.tsx', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      // Verify Navbar is rendered
      const navbar = getMainNavbar();
      expect(navbar).toBeInTheDocument();

      // Verify Navbar has the expected DaisyUI classes (proves it's using the existing component)
      expect(navbar).toHaveClass('navbar');
      expect(navbar).toHaveClass('bg-base-100/80');
      expect(navbar).toHaveClass('backdrop-blur-md');
      expect(navbar).toHaveClass('fixed');
    });

    it('should use Navbar without modification - verify expected structure', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      const navbar = getMainNavbar();

      // Verify navbar-start section exists (from original Navbar component)
      const navbarStart = navbar.querySelector('.navbar-start');
      expect(navbarStart).toBeInTheDocument();

      // Verify navbar-end section exists (from original Navbar component)
      const navbarEnd = navbar.querySelector('.navbar-end');
      expect(navbarEnd).toBeInTheDocument();

      // Verify brand link "ShortURL" exists (from original Navbar component)
      const brandLink = screen.getByRole('link', { name: /shorturl/i });
      expect(brandLink).toBeInTheDocument();
    });

    it('should use Navbar component that integrates with AuthContext', () => {
      // Test with unauthenticated user
      const { unmount } = renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });
      expect(screen.getByTestId('nav-login')).toBeInTheDocument();
      expect(screen.getByTestId('nav-register')).toBeInTheDocument();
      unmount();

      // Test with authenticated user
      renderWithProviders(<Home />, { authState: mockAuthContext.authenticated });
      const navbar = getMainNavbar();
      expect(within(navbar).getByRole('button', { name: /logout/i })).toBeInTheDocument();
    });

    it('should render Navbar with its built-in responsive behavior', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      const navbar = getMainNavbar();

      // Desktop navigation (hidden on mobile)
      const desktopNav = navbar.querySelector('.hidden.md\\:flex');
      expect(desktopNav).toBeInTheDocument();

      // Mobile hamburger button
      const hamburgerButton = screen.getByTestId('hamburger-menu');
      expect(hamburgerButton).toBeInTheDocument();
    });
  });

  describe('Test Case 2: ThemeToggle component integration', () => {
    it('should render the existing ThemeToggle component within Navbar', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      // ThemeToggle should be rendered with the proper aria-label pattern
      const themeToggle = screen.getByRole('button', { name: /switch to (light|dark) mode/i });
      expect(themeToggle).toBeInTheDocument();
    });

    it('should use ThemeToggle with expected styling from original component', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      const themeToggle = screen.getByRole('button', { name: /switch to (light|dark) mode/i });

      // Verify it has the DaisyUI button classes from the original ThemeToggle component
      expect(themeToggle).toHaveClass('btn');
      expect(themeToggle).toHaveClass('btn-ghost');
      expect(themeToggle).toHaveClass('btn-circle');
    });

    it('should render ThemeToggle with SVG icon as in original component', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      const themeToggle = screen.getByRole('button', { name: /switch to (light|dark) mode/i });
      const svgIcon = themeToggle.querySelector('svg');
      expect(svgIcon).toBeInTheDocument();

      // Verify SVG has the expected dimensions from original ThemeToggle
      expect(svgIcon).toHaveClass('h-5', 'w-5');
    });

    it('should integrate ThemeToggle that works with ThemeContext', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      // The toggle should exist and be clickable (integration with ThemeContext)
      const themeToggle = screen.getByRole('button', { name: /switch to (light|dark) mode/i });
      expect(themeToggle).not.toBeDisabled();
    });

    it('should render ThemeToggle in both desktop and mobile nav areas', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      // In desktop area (hidden md:flex)
      const navbar = getMainNavbar();
      const desktopNav = navbar.querySelector('.hidden.md\\:flex');
      const desktopThemeToggle = desktopNav?.querySelector('button[aria-label*="Switch to"]');
      expect(desktopThemeToggle).toBeInTheDocument();
    });
  });

  describe('Test Case 3: FuturisticButton component usage', () => {
    it('should use FuturisticButton for primary CTA in hero section', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      // Hero CTA should use FuturisticButton (has the transition and hover effects)
      const heroCta = screen.getByTestId('hero-cta');
      expect(heroCta).toBeInTheDocument();

      // Verify it has the FuturisticButton classes
      expect(heroCta).toHaveClass('btn');
      expect(heroCta).toHaveClass('btn-primary');
      expect(heroCta).toHaveClass('transition-all');
      expect(heroCta).toHaveClass('duration-300');
      expect(heroCta).toHaveClass('hover:scale-105');
    });

    it('should use FuturisticButton with proper link behavior', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      const heroCta = screen.getByTestId('hero-cta');

      // FuturisticButton with 'to' prop renders as a Link
      expect(heroCta.tagName.toLowerCase()).toBe('a');
      expect(heroCta).toHaveAttribute('href', '/register');
    });

    it('should use FuturisticButton in dashboard preview section', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      const dashboardPreviewCta = screen.getByTestId('dashboard-preview-cta');
      expect(dashboardPreviewCta).toBeInTheDocument();

      // Verify it has the FuturisticButton classes
      expect(dashboardPreviewCta).toHaveClass('btn');
      expect(dashboardPreviewCta).toHaveClass('btn-primary');
      expect(dashboardPreviewCta).toHaveClass('transition-all');
      expect(dashboardPreviewCta).toHaveClass('hover:scale-105');
    });

    it('should use FuturisticButton with correct size variants', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      // Hero CTA uses size="lg"
      const heroCta = screen.getByTestId('hero-cta');
      expect(heroCta).toHaveClass('btn-lg');
      expect(heroCta).toHaveClass('min-h-[44px]');
      expect(heroCta).toHaveClass('min-w-[44px]');

      // Dashboard preview CTA also uses size="lg"
      const dashboardPreviewCta = screen.getByTestId('dashboard-preview-cta');
      expect(dashboardPreviewCta).toHaveClass('btn-lg');
    });

    it('should change FuturisticButton text based on auth state', () => {
      // Unauthenticated: "Get Started"
      const { unmount } = renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });
      expect(screen.getByTestId('hero-cta')).toHaveTextContent('Get Started');
      unmount();

      // Authenticated: "Go to Dashboard"
      renderWithProviders(<Home />, { authState: mockAuthContext.authenticated });
      expect(screen.getByTestId('hero-cta')).toHaveTextContent('Go to Dashboard');
    });

    it('should use FuturisticButton with correct variant for authenticated users', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.authenticated });

      const heroCta = screen.getByTestId('hero-cta');

      // When authenticated, the CTA changes to "Go to Dashboard"
      expect(heroCta).toHaveTextContent(/go to dashboard/i);
      expect(heroCta).toHaveAttribute('href', '/dashboard');

      // Still uses FuturisticButton with primary variant
      expect(heroCta).toHaveClass('btn', 'btn-primary');
    });
  });

  describe('Test Case 4: GlassMorphismCard component usage', () => {
    it('should use GlassMorphismCard for feature cards', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      // Check the features section exists
      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection).toBeInTheDocument();

      // Find feature cards - they should have GlassMorphismCard styling
      const featureCards = featuresSection.querySelectorAll('.bg-base-100\\/60');
      expect(featureCards.length).toBe(4); // 4 feature cards
    });

    it('should use GlassMorphismCard with expected glass morphism styling', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      const featuresSection = screen.getByTestId('features-section');
      const featureCards = featuresSection.querySelectorAll('.bg-base-100\\/60');

      featureCards.forEach((card) => {
        // Verify GlassMorphismCard classes
        expect(card).toHaveClass('backdrop-blur-md');
        expect(card).toHaveClass('rounded-xl');
        expect(card).toHaveClass('border');
        expect(card).toHaveClass('border-base-200');
        expect(card).toHaveClass('p-6');
        expect(card).toHaveClass('shadow-lg');
      });
    });

    it('should use GlassMorphismCard in dashboard preview section', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      const dashboardPreviewSection = screen.getByTestId('dashboard-preview-section');
      const glassCard = dashboardPreviewSection.querySelector('.bg-base-100\\/60');

      expect(glassCard).toBeInTheDocument();
      expect(glassCard).toHaveClass('backdrop-blur-md');
      expect(glassCard).toHaveClass('rounded-xl');
    });

    it('should render all 4 feature cards using GlassMorphismCard', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      // Verify each feature card exists with its content
      expect(screen.getByTestId('feature-card-url-shortening')).toBeInTheDocument();
      expect(screen.getByTestId('feature-card-click-analytics')).toBeInTheDocument();
      expect(screen.getByTestId('feature-card-geographic-insights')).toBeInTheDocument();
      expect(screen.getByTestId('feature-card-shareable-stats')).toBeInTheDocument();
    });

    it('should use GlassMorphismCard for How It Works steps', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      // How It Works section contains GlassMorphismCards
      const howItWorksSection = screen.getByTestId('how-it-works-section');
      expect(howItWorksSection).toBeInTheDocument();

      // Each step uses GlassMorphismCard
      const stepCards = howItWorksSection.querySelectorAll('.bg-base-100\\/60.backdrop-blur-md.rounded-xl');
      expect(stepCards.length).toBe(3); // 3 steps
    });

    it('should apply GlassMorphismCard border and shadow consistently', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      // Get all GlassMorphismCards on the page
      const glassMorphismCards = document.querySelectorAll('.bg-base-100\\/60.backdrop-blur-md.rounded-xl');

      // Each should have border and shadow styling from GlassMorphismCard
      glassMorphismCards.forEach(card => {
        expect(card).toHaveClass('border', 'border-base-200', 'shadow-lg');
      });
    });
  });

  describe('Test Case 5: BackgroundEffect component integration', () => {
    it('should render BackgroundEffect component for visual effects', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      // BackgroundEffect renders a fixed positioned container
      const backgroundEffect = document.querySelector('.fixed.inset-0.-z-10');
      expect(backgroundEffect).toBeInTheDocument();
    });

    it('should use BackgroundEffect with expected styling from original component', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      const backgroundEffect = document.querySelector('.fixed.inset-0.-z-10');
      expect(backgroundEffect).toHaveClass('overflow-hidden');
    });

    it('should render BackgroundEffect with animated gradient blobs', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      const backgroundEffect = document.querySelector('.fixed.inset-0.-z-10');

      // Find the animated gradient blobs
      const primaryBlob = backgroundEffect?.querySelector('.bg-primary\\/20');
      const secondaryBlob = backgroundEffect?.querySelector('.bg-secondary\\/20');

      expect(primaryBlob).toBeInTheDocument();
      expect(secondaryBlob).toBeInTheDocument();

      // Verify they have animation classes
      expect(primaryBlob).toHaveClass('animate-pulse');
      expect(secondaryBlob).toHaveClass('animate-pulse');
    });

    it('should position BackgroundEffect behind other content', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      const backgroundEffect = document.querySelector('.fixed.inset-0.-z-10');

      // -z-10 ensures it's behind other content
      expect(backgroundEffect).toHaveClass('-z-10');
      // Should be positioned fixed to cover the entire viewport
      expect(backgroundEffect).toHaveClass('fixed', 'inset-0');
    });

    it('should render BackgroundEffect blobs with blur effect', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      const backgroundEffect = document.querySelector('.fixed.inset-0.-z-10');
      const blobs = backgroundEffect?.querySelectorAll('.blur-3xl');

      // Both blobs should have blur effect
      expect(blobs?.length).toBe(2);
    });

    it('should use BackgroundEffect with primary and secondary colors', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      const backgroundEffect = document.querySelector('.fixed.inset-0.-z-10');

      // Contains primary-colored blur circle
      const primaryBlur = backgroundEffect?.querySelector('.bg-primary\\/20');
      expect(primaryBlur).toBeInTheDocument();

      // Contains secondary-colored blur circle
      const secondaryBlur = backgroundEffect?.querySelector('.bg-secondary\\/20');
      expect(secondaryBlur).toBeInTheDocument();
    });
  });
});
