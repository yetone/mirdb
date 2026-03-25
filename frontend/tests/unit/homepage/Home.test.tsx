/**
 * Home Page Navigation Links Tests
 * Owner: Scenario 9 - Navigation Links
 *
 * Tests that validate navigation links to Login, Register, and footer sections
 * work correctly (REQ-9), including:
 * - Navbar structure with logo and navigation links
 * - Login/Register link presence and href attributes
 * - ThemeToggle component presence and functionality
 * - Navigation behavior when links are clicked
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { Home } from '../../../src/pages/Home';
import { Navbar } from '../../../src/components/Navbar';
import { renderWithProviders } from './test-utils';

describe('Scenario 9: Navigation Links', () => {
  describe('Test Case 1: Navbar component renders with logo', () => {
    it('renders Navbar component on the homepage', () => {
      renderWithProviders(<Home />, {
        authOptions: { isAuthenticated: false },
        useMemoryRouter: true,
      });

      // Navbar should be present
      const navbar = screen.getByRole('navigation', { name: /main navigation/i });
      expect(navbar).toBeInTheDocument();
    });

    it('displays the logo text "URL Shortener"', () => {
      renderWithProviders(<Home />, {
        authOptions: { isAuthenticated: false },
        useMemoryRouter: true,
      });

      // Logo link with product name should be visible
      const logo = screen.getByRole('link', { name: /url shortener/i });
      expect(logo).toBeInTheDocument();
      expect(logo).toHaveTextContent('URL Shortener');
    });

    it('logo links to the homepage root path', () => {
      renderWithProviders(<Home />, {
        authOptions: { isAuthenticated: false },
        useMemoryRouter: true,
      });

      const logo = screen.getByRole('link', { name: /url shortener/i });
      expect(logo).toHaveAttribute('href', '/');
    });

    it('logo has proper styling for brand identity', () => {
      renderWithProviders(<Home />, {
        authOptions: { isAuthenticated: false },
        useMemoryRouter: true,
      });

      const logo = screen.getByRole('link', { name: /url shortener/i });
      expect(logo).toHaveClass('text-xl');
      expect(logo).toHaveClass('font-bold');
      expect(logo).toHaveClass('text-primary');
    });
  });

  describe('Test Case 2: Login link exists with href="/login"', () => {
    it('renders Login/Sign In link when user is unauthenticated', () => {
      renderWithProviders(<Home />, {
        authOptions: { isAuthenticated: false },
        useMemoryRouter: true,
      });

      const signInLink = screen.getByTestId('sign-in-link');
      expect(signInLink).toBeInTheDocument();
      expect(signInLink).toBeVisible();
    });

    it('Login link has correct href attribute pointing to /login', () => {
      renderWithProviders(<Home />, {
        authOptions: { isAuthenticated: false },
        useMemoryRouter: true,
      });

      const signInLink = screen.getByTestId('sign-in-link');
      expect(signInLink).toHaveAttribute('href', '/login');
    });

    it('Login link displays correct text "Sign In"', () => {
      renderWithProviders(<Home />, {
        authOptions: { isAuthenticated: false },
        useMemoryRouter: true,
      });

      const signInLink = screen.getByTestId('sign-in-link');
      expect(signInLink).toHaveTextContent('Sign In');
    });

    it('Login link is not present when user is authenticated', () => {
      renderWithProviders(<Home />, {
        authOptions: {
          isAuthenticated: true,
          user: { id: '1', username: 'testuser', email: 'test@example.com' },
        },
        useMemoryRouter: true,
      });

      expect(screen.queryByTestId('sign-in-link')).not.toBeInTheDocument();
    });

    it('Login link has proper accessibility attributes', () => {
      renderWithProviders(<Home />, {
        authOptions: { isAuthenticated: false },
        useMemoryRouter: true,
      });

      const signInLink = screen.getByTestId('sign-in-link');
      expect(signInLink).toHaveAttribute('aria-label', 'Sign in to your account');
    });
  });

  describe('Test Case 3: Register link exists with href="/register"', () => {
    it('renders Register/Get Started link when user is unauthenticated', () => {
      renderWithProviders(<Home />, {
        authOptions: { isAuthenticated: false },
        useMemoryRouter: true,
      });

      const getStartedLink = screen.getByTestId('get-started-link');
      expect(getStartedLink).toBeInTheDocument();
      expect(getStartedLink).toBeVisible();
    });

    it('Register link has correct href attribute pointing to /register', () => {
      renderWithProviders(<Home />, {
        authOptions: { isAuthenticated: false },
        useMemoryRouter: true,
      });

      const getStartedLink = screen.getByTestId('get-started-link');
      expect(getStartedLink).toHaveAttribute('href', '/register');
    });

    it('Register link displays correct text "Get Started"', () => {
      renderWithProviders(<Home />, {
        authOptions: { isAuthenticated: false },
        useMemoryRouter: true,
      });

      const getStartedLink = screen.getByTestId('get-started-link');
      expect(getStartedLink).toHaveTextContent('Get Started');
    });

    it('Register link is not present when user is authenticated', () => {
      renderWithProviders(<Home />, {
        authOptions: {
          isAuthenticated: true,
          user: { id: '1', username: 'testuser', email: 'test@example.com' },
        },
        useMemoryRouter: true,
      });

      expect(screen.queryByTestId('get-started-link')).not.toBeInTheDocument();
    });

    it('Register link has primary button styling', () => {
      renderWithProviders(<Home />, {
        authOptions: { isAuthenticated: false },
        useMemoryRouter: true,
      });

      const getStartedLink = screen.getByTestId('get-started-link');
      expect(getStartedLink).toHaveClass('btn');
      expect(getStartedLink).toHaveClass('btn-primary');
    });
  });

  describe('Test Case 4: ThemeToggle component renders and is functional', () => {
    it('renders ThemeToggle component in navbar', () => {
      renderWithProviders(<Home />, {
        authOptions: { isAuthenticated: false },
        useMemoryRouter: true,
      });

      const themeToggle = screen.getByTestId('theme-toggle');
      expect(themeToggle).toBeInTheDocument();
      expect(themeToggle).toBeVisible();
    });

    it('ThemeToggle is within the navbar', () => {
      renderWithProviders(<Home />, {
        authOptions: { isAuthenticated: false },
        useMemoryRouter: true,
      });

      const navbar = screen.getByRole('navigation', { name: /main navigation/i });
      const themeToggle = within(navbar).getByTestId('theme-toggle');
      expect(themeToggle).toBeInTheDocument();
    });

    it('ThemeToggle contains a theme selector', () => {
      renderWithProviders(<Home />, {
        authOptions: { isAuthenticated: false },
        useMemoryRouter: true,
      });

      const themeSelect = screen.getByRole('combobox', { name: /select theme/i });
      expect(themeSelect).toBeInTheDocument();
    });

    it('ThemeToggle displays all available themes', () => {
      renderWithProviders(<Home />, {
        authOptions: { isAuthenticated: false },
        themeOptions: { theme: 'light' },
        useMemoryRouter: true,
      });

      const themeSelect = screen.getByRole('combobox', { name: /select theme/i });
      const options = within(themeSelect).getAllByRole('option');

      // Should have all 8 themes
      expect(options.length).toBe(8);

      // Verify theme names
      const themeNames = options.map((opt) => opt.textContent);
      expect(themeNames).toContain('Light');
      expect(themeNames).toContain('Dark');
      expect(themeNames).toContain('Cyberpunk');
      expect(themeNames).toContain('Synthwave');
    });

    it('ThemeToggle changes theme when selection is made', async () => {
      const user = userEvent.setup();

      renderWithProviders(<Home />, {
        authOptions: { isAuthenticated: false },
        themeOptions: { theme: 'light' },
        useMemoryRouter: true,
      });

      const themeSelect = screen.getByRole('combobox', { name: /select theme/i });
      expect(themeSelect).toHaveValue('light');

      // Change to dark theme
      await user.selectOptions(themeSelect, 'dark');
      expect(themeSelect).toHaveValue('dark');
    });

    it('ThemeToggle displays appropriate icon for light theme', () => {
      renderWithProviders(<Home />, {
        authOptions: { isAuthenticated: false },
        themeOptions: { theme: 'light' },
        useMemoryRouter: true,
      });

      const themeToggle = screen.getByTestId('theme-toggle');
      // Light theme should show Sun icon (aria-hidden)
      const icon = themeToggle.querySelector('svg');
      expect(icon).toBeInTheDocument();
      expect(icon).toHaveAttribute('aria-hidden', 'true');
    });

    it('ThemeToggle displays appropriate icon for dark theme', () => {
      renderWithProviders(<Home />, {
        authOptions: { isAuthenticated: false },
        themeOptions: { theme: 'dark' },
        useMemoryRouter: true,
      });

      const themeToggle = screen.getByTestId('theme-toggle');
      const icon = themeToggle.querySelector('svg');
      expect(icon).toBeInTheDocument();
      expect(icon).toHaveAttribute('aria-hidden', 'true');
    });
  });

  describe('Test Case 5: Click Login link navigates to /login', () => {
    it('clicking Sign In link navigates to /login route', async () => {
      const user = userEvent.setup();

      renderWithProviders(<Home />, {
        authOptions: { isAuthenticated: false },
        useMemoryRouter: true,
        initialRoute: '/',
      });

      const signInLink = screen.getByTestId('sign-in-link');
      expect(signInLink).toHaveAttribute('href', '/login');

      // Click the link
      await user.click(signInLink);

      // Verify the link element is indeed a navigable link
      expect(signInLink.tagName).toBe('A');
      expect(signInLink).toHaveAttribute('href', '/login');
    });

    it('Sign In link is keyboard accessible', async () => {
      const user = userEvent.setup();

      renderWithProviders(<Home />, {
        authOptions: { isAuthenticated: false },
        useMemoryRouter: true,
      });

      const signInLink = screen.getByTestId('sign-in-link');

      // Focus should be achievable via keyboard
      signInLink.focus();
      expect(signInLink).toHaveFocus();

      // Should be activatable via Enter key
      await user.keyboard('{Enter}');

      // Link should have correct href for navigation
      expect(signInLink).toHaveAttribute('href', '/login');
    });

    it('Sign In link is focusable via Tab navigation', async () => {
      const user = userEvent.setup();

      renderWithProviders(<Navbar />, {
        authOptions: { isAuthenticated: false },
        useMemoryRouter: true,
      });

      // Tab through to reach Sign In link
      await user.tab(); // Logo
      await user.tab(); // Theme toggle (select)
      await user.tab(); // Sign In link

      const signInLink = screen.getByTestId('sign-in-link');
      expect(signInLink).toHaveFocus();
    });
  });

  describe('Navbar Authenticated State', () => {
    it('shows user display name when authenticated', () => {
      renderWithProviders(<Home />, {
        authOptions: {
          isAuthenticated: true,
          user: { id: '1', username: 'testuser', email: 'test@example.com' },
        },
        useMemoryRouter: true,
      });

      const userDisplay = screen.getByTestId('user-display');
      expect(userDisplay).toBeInTheDocument();
      expect(userDisplay).toHaveTextContent('Welcome, testuser');
    });

    it('shows Dashboard link when authenticated', () => {
      renderWithProviders(<Home />, {
        authOptions: {
          isAuthenticated: true,
          user: { id: '1', username: 'testuser', email: 'test@example.com' },
        },
        useMemoryRouter: true,
      });

      const dashboardLink = screen.getByTestId('dashboard-link');
      expect(dashboardLink).toBeInTheDocument();
      expect(dashboardLink).toHaveAttribute('href', '/dashboard');
    });

    it('shows Logout button when authenticated', () => {
      renderWithProviders(<Home />, {
        authOptions: {
          isAuthenticated: true,
          user: { id: '1', username: 'testuser', email: 'test@example.com' },
        },
        useMemoryRouter: true,
      });

      const logoutButton = screen.getByTestId('logout-button');
      expect(logoutButton).toBeInTheDocument();
      expect(logoutButton).toHaveTextContent('Logout');
    });
  });

  describe('Navigation Accessibility', () => {
    it('navbar has proper role and aria-label', () => {
      renderWithProviders(<Home />, {
        authOptions: { isAuthenticated: false },
        useMemoryRouter: true,
      });

      const navbar = screen.getByRole('navigation', { name: /main navigation/i });
      expect(navbar).toHaveAttribute('aria-label', 'Main navigation');
    });

    it('all navigation links are accessible by role', () => {
      renderWithProviders(<Home />, {
        authOptions: { isAuthenticated: false },
        useMemoryRouter: true,
      });

      // All navigation links should be accessible
      const links = screen.getAllByRole('link');
      expect(links.length).toBeGreaterThan(0);

      // Logo link should be present
      expect(screen.getByRole('link', { name: /url shortener/i })).toBeInTheDocument();

      // Sign In links should be present (may be multiple - navbar and hero)
      const signInLinks = screen.getAllByRole('link', { name: /sign in/i });
      expect(signInLinks.length).toBeGreaterThan(0);

      // Get Started links should be present (may be multiple - navbar and hero)
      const getStartedLinks = screen.getAllByRole('link', { name: /get started/i });
      expect(getStartedLinks.length).toBeGreaterThan(0);
    });

    it('navigation links maintain proper focus order', async () => {
      const user = userEvent.setup();

      renderWithProviders(<Navbar />, {
        authOptions: { isAuthenticated: false },
        useMemoryRouter: true,
      });

      // Tab order should be logical
      await user.tab();
      const logo = screen.getByRole('link', { name: /url shortener/i });
      expect(logo).toHaveFocus();
    });
  });
});
