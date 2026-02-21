/**
 * Homepage route and rendering tests.
 * Owner: Scenario 1 - Homepage Route Accessibility
 *
 * Test coverage:
 * - Homepage renders at '/' route without authentication
 * - All major sections (Navbar, Hero, Features, Footer) are present
 * - Page does not redirect unauthenticated users
 * - Component mounts without errors
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { screen, waitFor } from '@testing-library/react';
import { renderWithProviders } from '../utils/renderWithProviders';
import { Home } from '@/pages/Home';

describe('Home Page - Route Accessibility', () => {
  beforeEach(() => {
    vi.mocked(localStorage.getItem).mockReturnValue(null);
  });

  describe('Integration Test 1: Homepage renders at "/" route', () => {
    it('should render homepage with Navbar, Hero section, Features section, and Footer visible', async () => {
      renderWithProviders(<Home />, { initialEntries: ['/'] });

      // Wait for page to render
      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument();
      });

      // Verify Navbar is visible
      expect(screen.getByTestId('navbar')).toBeInTheDocument();

      // Verify Hero section is visible
      expect(screen.getByTestId('hero-section')).toBeInTheDocument();

      // Verify Features section is visible
      expect(screen.getByTestId('features-section')).toBeInTheDocument();

      // Verify Footer is visible
      expect(screen.getByTestId('footer')).toBeInTheDocument();
    });

    it('should render hero with call-to-action buttons', async () => {
      renderWithProviders(<Home />, { initialEntries: ['/'] });

      await waitFor(() => {
        expect(screen.getByTestId('hero-section')).toBeInTheDocument();
      });

      // Verify CTA buttons exist
      expect(screen.getByTestId('cta-get-started')).toBeInTheDocument();
      expect(screen.getByTestId('cta-sign-in')).toBeInTheDocument();
    });

    it('should render all 4 feature cards', async () => {
      renderWithProviders(<Home />, { initialEntries: ['/'] });

      await waitFor(() => {
        expect(screen.getByTestId('features-section')).toBeInTheDocument();
      });

      // Verify all 4 feature cards are present by their test IDs
      expect(screen.getByTestId('feature-card-url-shortening')).toBeInTheDocument();
      expect(screen.getByTestId('feature-card-click-analytics')).toBeInTheDocument();
      expect(screen.getByTestId('feature-card-secure-management')).toBeInTheDocument();
      expect(screen.getByTestId('feature-card-custom-short-codes')).toBeInTheDocument();
    });
  });

  describe('Integration Test 2: Homepage accessible without authentication', () => {
    it('should load homepage successfully without authentication token', async () => {
      // Ensure no auth token
      vi.mocked(localStorage.getItem).mockReturnValue(null);

      renderWithProviders(<Home />, { initialEntries: ['/'] });

      // Page should load without redirect
      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument();
      });

      // Verify we are on home page (not redirected)
      expect(screen.getByTestId('navbar')).toBeInTheDocument();
    });

    it('should display Login and Register buttons in navbar when unauthenticated', async () => {
      vi.mocked(localStorage.getItem).mockReturnValue(null);

      renderWithProviders(<Home />, { initialEntries: ['/'] });

      await waitFor(() => {
        expect(screen.getByTestId('navbar')).toBeInTheDocument();
      });

      // Should show login/register links in navbar
      expect(screen.getByTestId('navbar-login')).toBeInTheDocument();
      expect(screen.getByTestId('navbar-register')).toBeInTheDocument();
    });

    it('should not require authentication to view homepage content', async () => {
      vi.mocked(localStorage.getItem).mockReturnValue(null);

      renderWithProviders(<Home />, { initialEntries: ['/'] });

      // All sections should be accessible
      await waitFor(() => {
        expect(screen.getByTestId('hero-section')).toBeInTheDocument();
      });

      expect(screen.getByTestId('features-section')).toBeInTheDocument();
      expect(screen.getByTestId('footer')).toBeInTheDocument();
    });
  });

  describe('Unit Test 3: Home component mounts without errors', () => {
    it('should mount Home component without throwing errors', () => {
      expect(() => {
        renderWithProviders(<Home />, { initialEntries: ['/'] });
      }).not.toThrow();
    });

    it('should render all required sections in correct structure', async () => {
      renderWithProviders(<Home />, { initialEntries: ['/'] });

      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument();
      });

      const homePage = screen.getByTestId('home-page');

      // Verify structural elements
      expect(homePage).toContainElement(screen.getByTestId('navbar'));
      expect(homePage).toContainElement(screen.getByTestId('hero-section'));
      expect(homePage).toContainElement(screen.getByTestId('features-section'));
      expect(homePage).toContainElement(screen.getByTestId('footer'));
    });

    it('should render background effect component', async () => {
      renderWithProviders(<Home />, { initialEntries: ['/'] });

      await waitFor(() => {
        expect(screen.getByTestId('background-effect')).toBeInTheDocument();
      });
    });

    it('should display headline in hero section', async () => {
      renderWithProviders(<Home />, { initialEntries: ['/'] });

      await waitFor(() => {
        expect(screen.getByTestId('hero-section')).toBeInTheDocument();
      });

      // Look for heading with URL shortening related content
      const heading = screen.getByRole('heading', { level: 1 });
      expect(heading).toBeInTheDocument();
      expect(heading.textContent).toMatch(/shorten|link|url|track/i);
    });

    it('should display feature section heading', async () => {
      renderWithProviders(<Home />, { initialEntries: ['/'] });

      await waitFor(() => {
        expect(screen.getByTestId('features-section')).toBeInTheDocument();
      });

      const featuresHeading = screen.getByRole('heading', { level: 2 });
      expect(featuresHeading).toBeInTheDocument();
    });
  });
});
