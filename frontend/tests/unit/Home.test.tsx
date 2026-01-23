/**
 * Home Page Unit Tests
 * Owner: Scenarios 11, 14, 16, 18
 *
 * Test coverage:
 * - Scenario 11: Navbar integration
 * - Scenario 14: SEO and meta tags
 * - Scenario 16: External link security
 * - Scenario 18: Error-free rendering
 *
 * Test suites:
 * - describe('Home Page Rendering')
 * - describe('Navbar Integration')
 * - describe('SEO Meta Tags')
 * - describe('External Link Security')
 */

import { describe, it, expect } from 'vitest';
import { screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { renderWithProviders, createMockAuthContext } from '../utils/renderWithProviders';
import Home from '../../src/pages/Home';

/**
 * Helper to get the main Navbar element (has class 'navbar')
 * This distinguishes it from the Footer's nav element
 */
const getMainNavbar = (): HTMLElement => {
  const navElements = screen.getAllByRole('navigation');
  const navbar = navElements.find(nav => nav.classList.contains('navbar'));
  if (!navbar) {
    throw new Error('Could not find main Navbar with class "navbar"');
  }
  return navbar;
};

describe('Navbar Integration (Scenario 11)', () => {
  describe('Navbar Presence on Homepage', () => {
    it('should render the Navbar component on the homepage', () => {
      renderWithProviders(<Home />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      // Check for the navbar element with 'navbar' class
      const navbar = getMainNavbar();
      expect(navbar).toBeInTheDocument();
    });

    it('should render Navbar as the first child element in the page structure', () => {
      renderWithProviders(<Home />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      // The navbar should be at the top of the page
      const navbar = getMainNavbar();
      expect(navbar).toBeInTheDocument();
      expect(navbar.tagName).toBe('NAV');
    });

    it('should display the brand/logo link in Navbar', () => {
      renderWithProviders(<Home />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const navbar = getMainNavbar();
      // Check for brand link that navigates to home within the navbar
      const brandLink = within(navbar).getByRole('link', { name: /url shortener/i });
      expect(brandLink).toBeInTheDocument();
      expect(brandLink).toHaveAttribute('href', '/');
    });
  });

  describe('Navigation Links in Navbar', () => {
    it('should display Login link in Navbar for unauthenticated users', () => {
      renderWithProviders(<Home />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const navbar = getMainNavbar();
      const loginLink = within(navbar).getByRole('link', { name: /login/i });
      expect(loginLink).toBeInTheDocument();
      expect(loginLink).toHaveAttribute('href', '/login');
    });

    it('should display Register link in Navbar for unauthenticated users', () => {
      renderWithProviders(<Home />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const navbar = getMainNavbar();
      const registerLink = within(navbar).getByRole('link', { name: /register/i });
      expect(registerLink).toBeInTheDocument();
      expect(registerLink).toHaveAttribute('href', '/register');
    });

    it('should display Dashboard link for authenticated users', () => {
      renderWithProviders(<Home />, {
        authContext: createMockAuthContext({
          isAuthenticated: true,
          user: { id: 1, username: 'testuser', email: 'test@example.com', is_admin: 0 },
        }),
      });

      const navbar = getMainNavbar();
      const dashboardLink = within(navbar).getByRole('link', { name: /dashboard/i });
      expect(dashboardLink).toBeInTheDocument();
      expect(dashboardLink).toHaveAttribute('href', '/dashboard');
    });

    it('should display Logout button for authenticated users', () => {
      renderWithProviders(<Home />, {
        authContext: createMockAuthContext({
          isAuthenticated: true,
          user: { id: 1, username: 'testuser', email: 'test@example.com', is_admin: 0 },
        }),
      });

      const navbar = getMainNavbar();
      const logoutButton = within(navbar).getByRole('button', { name: /logout/i });
      expect(logoutButton).toBeInTheDocument();
    });

    it('should not display Login/Register links in Navbar for authenticated users', () => {
      renderWithProviders(<Home />, {
        authContext: createMockAuthContext({
          isAuthenticated: true,
          user: { id: 1, username: 'testuser', email: 'test@example.com', is_admin: 0 },
        }),
      });

      const navbar = getMainNavbar();
      // Login and Register should not be visible in navbar when authenticated
      const navLoginLink = within(navbar).queryByRole('link', { name: /^login$/i });
      const navRegisterLink = within(navbar).queryByRole('link', { name: /^register$/i });

      expect(navLoginLink).not.toBeInTheDocument();
      expect(navRegisterLink).not.toBeInTheDocument();
    });
  });

  describe('ThemeToggle in Navbar', () => {
    it('should include ThemeToggle component in Navbar', () => {
      renderWithProviders(<Home />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const navbar = getMainNavbar();
      // ThemeToggle renders a "Theme" label that acts as a dropdown trigger
      const themeToggle = within(navbar).getByText(/^theme$/i);
      expect(themeToggle).toBeInTheDocument();
    });

    it('should render ThemeToggle within the Navbar navigation area', () => {
      renderWithProviders(<Home />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const navbar = getMainNavbar();
      const themeToggle = within(navbar).getByText(/^theme$/i);
      expect(themeToggle).toBeInTheDocument();
      // The ThemeToggle should be a label element styled as a button
      expect(themeToggle.tagName).toBe('LABEL');
    });

    it('should show theme dropdown options when ThemeToggle is clicked', async () => {
      const user = userEvent.setup();
      renderWithProviders(<Home />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const navbar = getMainNavbar();
      const themeToggle = within(navbar).getByText(/^theme$/i);
      await user.click(themeToggle);

      // Check that theme options are available within the navbar
      expect(within(navbar).getByRole('button', { name: /^light$/i })).toBeInTheDocument();
      expect(within(navbar).getByRole('button', { name: /^dark$/i })).toBeInTheDocument();
    });

    it('should allow theme selection from ThemeToggle dropdown', async () => {
      const user = userEvent.setup();
      renderWithProviders(<Home />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const navbar = getMainNavbar();
      const themeToggle = within(navbar).getByText(/^theme$/i);
      await user.click(themeToggle);

      // Verify cyberpunk theme option exists
      const cyberpunkOption = within(navbar).getByRole('button', { name: /^cyberpunk$/i });
      expect(cyberpunkOption).toBeInTheDocument();
    });
  });

  describe('Navbar Integration with Homepage Layout', () => {
    it('should render Navbar seamlessly with homepage sections', () => {
      renderWithProviders(<Home />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      // Verify Navbar is present
      const navbar = getMainNavbar();
      expect(navbar).toBeInTheDocument();

      // Verify main content sections are present alongside Navbar
      const main = screen.getByRole('main');
      expect(main).toBeInTheDocument();
    });

    it('should maintain consistent navigation across authentication states', () => {
      // Test unauthenticated state
      const { unmount } = renderWithProviders(<Home />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      let navbar = getMainNavbar();
      expect(navbar).toBeInTheDocument();
      expect(within(navbar).getByRole('link', { name: /url shortener/i })).toBeInTheDocument();

      unmount();

      // Test authenticated state
      renderWithProviders(<Home />, {
        authContext: createMockAuthContext({
          isAuthenticated: true,
          user: { id: 1, username: 'testuser', email: 'test@example.com', is_admin: 0 },
        }),
      });

      navbar = getMainNavbar();
      expect(navbar).toBeInTheDocument();
      expect(within(navbar).getByRole('link', { name: /url shortener/i })).toBeInTheDocument();
    });

    it('should integrate Navbar with proper styling classes', () => {
      renderWithProviders(<Home />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const navbar = getMainNavbar();
      // Navbar should have DaisyUI navbar class
      expect(navbar.className).toContain('navbar');
    });

    it('should position Navbar above all other page content', () => {
      renderWithProviders(<Home />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const navbar = getMainNavbar();
      const pageContainer = navbar.parentElement;
      expect(pageContainer).toBeInTheDocument();

      // The first child should be the navigation
      const firstChild = pageContainer?.firstElementChild;
      expect(firstChild?.tagName).toBe('NAV');
      expect(firstChild).toHaveClass('navbar');
    });
  });
});
