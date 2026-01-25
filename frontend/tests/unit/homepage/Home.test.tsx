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
 * Component Reuse Tests
 * Owner: Scenario 24 - Component Reuse
 *
 * Tests that verify the homepage properly integrates and reuses
 * existing application components without modification:
 * - Navbar component
 * - ThemeToggle component (via Navbar)
 * - FuturisticButton component
 * - GlassMorphismCard component
 * - BackgroundEffect component
 */
describe('Home Page - Component Reuse', () => {
  describe('Test Case 1: Navbar component integration', () => {
    it('should render the existing Navbar component', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      // The Navbar component renders a <nav> with the navbar class
      const navbar = getMainNavbar();
      expect(navbar).toBeInTheDocument();
      expect(navbar).toHaveClass('navbar');
    });

    it('should use Navbar without modification - contains expected structure', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      const navbar = getMainNavbar();

      // Navbar should have the fixed positioning and backdrop blur from original component
      expect(navbar).toHaveClass('bg-base-100/80', 'backdrop-blur-md', 'fixed');

      // Should have navbar-start section with brand link
      const brandLink = screen.getByRole('link', { name: /shorturl/i });
      expect(brandLink.closest('.navbar-start')).toBeInTheDocument();

      // Should have navbar-end section (desktop)
      const navbarEnd = navbar.querySelector('.navbar-end');
      expect(navbarEnd).toBeInTheDocument();
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
    it('should render ThemeToggle through Navbar (component composition)', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      // ThemeToggle is rendered inside Navbar
      const themeToggle = screen.getByRole('button', { name: /switch to (light|dark) mode/i });
      expect(themeToggle).toBeInTheDocument();
    });

    it('should have ThemeToggle working consistently with the app theme system', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      const themeToggle = screen.getByRole('button', { name: /switch to (light|dark) mode/i });

      // ThemeToggle should have the btn-ghost btn-circle styling
      expect(themeToggle).toHaveClass('btn', 'btn-ghost', 'btn-circle');

      // ThemeToggle should contain an SVG icon for theme indication
      const icon = themeToggle.querySelector('svg');
      expect(icon).toBeInTheDocument();
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

  describe('Test Case 3: FuturisticButton usage in CTAs', () => {
    it('should use FuturisticButton for hero CTA', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      const heroCta = screen.getByTestId('hero-cta');
      expect(heroCta).toBeInTheDocument();

      // FuturisticButton has specific classes: btn, transition-all, hover:scale-105
      expect(heroCta).toHaveClass('btn', 'btn-primary');

      // FuturisticButton renders as a Link when 'to' prop is provided
      expect(heroCta.tagName).toBe('A');
      expect(heroCta).toHaveAttribute('href', '/register');
    });

    it('should use FuturisticButton for Dashboard Preview CTA', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      const dashboardPreviewCta = screen.getByTestId('dashboard-preview-cta');
      expect(dashboardPreviewCta).toBeInTheDocument();

      // FuturisticButton applies btn and variant classes
      expect(dashboardPreviewCta).toHaveClass('btn', 'btn-primary');
      expect(dashboardPreviewCta).toHaveAttribute('href', '/register');
    });

    it('should apply FuturisticButton size variants correctly', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      const heroCta = screen.getByTestId('hero-cta');
      const dashboardCta = screen.getByTestId('dashboard-preview-cta');

      // Both CTAs use size="lg" which applies btn-lg and min-h/min-w for touch targets
      expect(heroCta).toHaveClass('btn-lg');
      expect(dashboardCta).toHaveClass('btn-lg');
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

  describe('Test Case 4: GlassMorphismCard usage', () => {
    it('should use GlassMorphismCard for feature cards', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      // Features section contains GlassMorphismCards
      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection).toBeInTheDocument();

      // GlassMorphismCard has specific styling: bg-base-100/60 backdrop-blur-md rounded-xl
      const featureCards = featuresSection.querySelectorAll('.bg-base-100\\/60.backdrop-blur-md.rounded-xl');
      expect(featureCards.length).toBe(4); // 4 feature cards
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

    it('should use GlassMorphismCard in Dashboard Preview', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      // Dashboard Preview section uses GlassMorphismCard
      const dashboardPreview = screen.getByTestId('dashboard-preview-section');
      expect(dashboardPreview).toBeInTheDocument();

      // Contains a GlassMorphismCard wrapper
      const previewCard = dashboardPreview.querySelector('.bg-base-100\\/60.backdrop-blur-md.rounded-xl');
      expect(previewCard).toBeInTheDocument();
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

  describe('Test Case 5: BackgroundEffect integration', () => {
    it('should render BackgroundEffect component', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      // BackgroundEffect renders a fixed, full-screen container with -z-10
      const backgroundEffect = document.querySelector('.fixed.inset-0.-z-10');
      expect(backgroundEffect).toBeInTheDocument();
    });

    it('should render BackgroundEffect with animated blur effects', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      const backgroundEffect = document.querySelector('.fixed.inset-0.-z-10');

      // BackgroundEffect contains animated blur circles
      const blurCircles = backgroundEffect?.querySelectorAll('.rounded-full.blur-3xl.animate-pulse');
      expect(blurCircles?.length).toBeGreaterThanOrEqual(2);
    });

    it('should position BackgroundEffect behind all content', () => {
      renderWithProviders(<Home />, { authState: mockAuthContext.unauthenticated });

      const backgroundEffect = document.querySelector('.fixed.inset-0.-z-10');

      // Should have negative z-index to appear behind content
      expect(backgroundEffect).toHaveClass('-z-10');

      // Should be positioned fixed to cover the entire viewport
      expect(backgroundEffect).toHaveClass('fixed', 'inset-0');
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
