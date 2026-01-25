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
