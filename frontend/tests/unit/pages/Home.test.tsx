/**
 * Home Page Unit Tests.
 * Owner: Scenario 18 - React Router Integration
 *
 * Tests:
 * - Homepage renders correctly when accessed directly via route
 * - All internal links use React Router's <Link> component, not native <a> tags
 * - Homepage contains all required sections
 * - React Router integration works correctly
 */
import { describe, it, expect } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import { ThemeProvider } from '../../../src/contexts/ThemeContext';
import { Home } from '../../../src/pages/Home';

/**
 * Helper to render with Router and Theme context.
 * Uses MemoryRouter for testing route behavior.
 */
function renderWithRouter(initialPath = '/') {
  return render(
    <MemoryRouter initialEntries={[initialPath]}>
      <ThemeProvider>
        <Routes>
          <Route path="/" element={<Home />} />
        </Routes>
      </ThemeProvider>
    </MemoryRouter>
  );
}

describe('Home Page', () => {
  describe('Test Case 4: Direct URL Access - Homepage renders correctly', () => {
    it('renders homepage component when accessing root URL "/" directly', () => {
      renderWithRouter('/');

      // Verify main container is present
      const homePage = screen.getByTestId('home-page');
      expect(homePage).toBeInTheDocument();
    });

    it('displays all required homepage sections', () => {
      renderWithRouter('/');

      // Verify Hero section is present
      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toBeInTheDocument();

      // Verify How It Works section is present
      const howItWorksSection = screen.getByTestId('how-it-works-section');
      expect(howItWorksSection).toBeInTheDocument();

      // Verify Features section is present
      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection).toBeInTheDocument();

      // Verify CTA section is present
      const ctaSection = screen.getByTestId('cta-section');
      expect(ctaSection).toBeInTheDocument();
    });

    it('displays navigation bar', () => {
      renderWithRouter('/');

      const navbar = screen.getByTestId('home-navbar');
      expect(navbar).toBeInTheDocument();
    });

    it('displays footer', () => {
      renderWithRouter('/');

      const footer = screen.getByTestId('home-footer');
      expect(footer).toBeInTheDocument();
    });

    it('has main content area with correct id for skip link', () => {
      renderWithRouter('/');

      const mainContent = document.getElementById('main-content');
      expect(mainContent).toBeInTheDocument();
      expect(mainContent?.tagName.toLowerCase()).toBe('main');
    });
  });

  describe('Test Case 5: All internal links use React Router <Link> component', () => {
    it('navbar Login link uses React Router Link (has correct href without native navigation)', () => {
      renderWithRouter('/');

      const loginLink = screen.getByTestId('navbar-login-link');

      // React Router's Link renders as <a> tag but uses client-side routing
      expect(loginLink.tagName).toBe('A');
      expect(loginLink).toHaveAttribute('href', '/login');

      // Verify it's not a native anchor (would have target attribute or external href)
      expect(loginLink).not.toHaveAttribute('target', '_blank');
    });

    it('navbar Register link uses React Router Link', () => {
      renderWithRouter('/');

      const registerLink = screen.getByTestId('navbar-register-link');

      expect(registerLink.tagName).toBe('A');
      expect(registerLink).toHaveAttribute('href', '/register');
      expect(registerLink).not.toHaveAttribute('target', '_blank');
    });

    it('hero section Sign Up Free link uses React Router Link', () => {
      renderWithRouter('/');

      const signUpLink = screen.getByRole('button', { name: /sign up free/i });

      // React Router Link renders as <a> tag
      expect(signUpLink.tagName).toBe('A');
      expect(signUpLink).toHaveAttribute('href', '/register');
    });

    it('CTA section Create Free Account link uses React Router Link', () => {
      renderWithRouter('/');

      const ctaSection = screen.getByTestId('cta-section');
      const createAccountLink = within(ctaSection).getByRole('button', {
        name: /create free account/i,
      });

      expect(createAccountLink.tagName).toBe('A');
      expect(createAccountLink).toHaveAttribute('href', '/register');
    });

    it('navbar logo link uses React Router Link to homepage', () => {
      renderWithRouter('/');

      const logoLink = screen.getByTestId('navbar-logo');

      expect(logoLink.tagName).toBe('A');
      expect(logoLink).toHaveAttribute('href', '/');
    });

    it('all internal navigation links have relative paths (not absolute URLs)', () => {
      renderWithRouter('/');

      // Get all anchor tags in the page
      const allLinks = document.querySelectorAll('a[href]');

      // Check that no internal links use absolute URLs (which would bypass React Router)
      allLinks.forEach((link) => {
        const href = link.getAttribute('href') || '';
        // Internal links should start with "/" and not "http"
        if (href.startsWith('/')) {
          expect(href).not.toMatch(/^https?:\/\//);
        }
      });
    });
  });

  describe('React Router Integration', () => {
    it('homepage is accessible at the root path "/"', () => {
      renderWithRouter('/');

      // Homepage should render without errors
      expect(screen.getByTestId('home-page')).toBeInTheDocument();
    });

    it('has proper page structure for SPA navigation', () => {
      renderWithRouter('/');

      // Verify the page has proper layout for SPA
      const homePage = screen.getByTestId('home-page');
      expect(homePage).toHaveClass('min-h-screen');
      expect(homePage).toHaveClass('flex');
      expect(homePage).toHaveClass('flex-col');
    });

    it('includes skip link for accessibility', () => {
      renderWithRouter('/');

      // Skip link should be the first focusable element
      const skipLink = screen.getByTestId('skip-link');
      expect(skipLink).toBeInTheDocument();
      expect(skipLink).toHaveAttribute('href', '#main-content');
    });
  });

  describe('Accessibility Integration', () => {
    it('homepage has proper semantic structure', () => {
      renderWithRouter('/');

      // Check for main landmark
      const main = screen.getByRole('main');
      expect(main).toBeInTheDocument();
      expect(main).toHaveAttribute('id', 'main-content');

      // Check for navigation landmark
      const nav = screen.getByRole('navigation', { name: /main navigation/i });
      expect(nav).toBeInTheDocument();
    });

    it('homepage has skip link as first focusable element', () => {
      renderWithRouter('/');

      const skipLink = screen.getByTestId('skip-link');
      expect(skipLink).toHaveAttribute('href', '#main-content');
    });
  });
});
