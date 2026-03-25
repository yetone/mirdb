/**
 * Navigation Integration Tests
 * Tests for CTA navigation flows on the homepage
 *
 * Scenario 2: Get Started CTA Navigation - /register navigation
 * Scenario 3: Sign In CTA Navigation - /login navigation
 */

import { describe, it, expect, vi } from 'vitest';
import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { renderWithProviders } from '../../unit/homepage/test-utils';
import { Home } from '../../../src/pages/Home';
import { Navbar } from '../../../src/components/Navbar';

// Mock react-router-dom's useNavigate
const mockNavigate = vi.fn();
vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom');
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  };
});

describe('Sign In CTA Navigation', () => {
  beforeEach(() => {
    mockNavigate.mockClear();
  });

  describe('Test Case 1: Sign In button/link visibility', () => {
    it('should display Sign In button/link when user is unauthenticated', () => {
      renderWithProviders(<Home />, {
        authOptions: { isAuthenticated: false },
        useMemoryRouter: true,
      });

      // Check for Sign In link in navbar
      const navbarSignIn = screen.getByTestId('sign-in-link');
      expect(navbarSignIn).toBeInTheDocument();
      expect(navbarSignIn).toHaveTextContent('Sign In');

      // Check for Sign In link in hero section
      const heroSignIn = screen.getByTestId('hero-sign-in-link');
      expect(heroSignIn).toBeInTheDocument();
      expect(heroSignIn).toHaveTextContent('Sign In');
    });

    it('should not display Sign In when user is authenticated', () => {
      renderWithProviders(<Home />, {
        authOptions: {
          isAuthenticated: true,
          user: { id: '1', username: 'testuser', email: 'test@example.com' },
        },
        useMemoryRouter: true,
      });

      // Sign In links should not be present for authenticated users
      expect(screen.queryByTestId('sign-in-link')).not.toBeInTheDocument();
      expect(screen.queryByTestId('hero-sign-in-link')).not.toBeInTheDocument();

      // Dashboard link should be present instead
      expect(screen.getByTestId('dashboard-link')).toBeInTheDocument();
    });
  });

  describe('Test Case 2: Sign In navigation to /login', () => {
    it('should navigate to /login when clicking Sign In in navbar', async () => {
      const user = userEvent.setup();

      renderWithProviders(<Home />, {
        authOptions: { isAuthenticated: false },
        useMemoryRouter: true,
        initialRoute: '/',
      });

      const signInLink = screen.getByTestId('sign-in-link');
      expect(signInLink).toHaveAttribute('href', '/login');

      await user.click(signInLink);

      // Verify the link points to /login
      expect(signInLink.closest('a')).toHaveAttribute('href', '/login');
    });

    it('should navigate to /login when clicking Sign In in hero section', async () => {
      const user = userEvent.setup();

      renderWithProviders(<Home />, {
        authOptions: { isAuthenticated: false },
        useMemoryRouter: true,
        initialRoute: '/',
      });

      const heroSignInLink = screen.getByTestId('hero-sign-in-link');
      expect(heroSignInLink).toHaveAttribute('href', '/login');

      await user.click(heroSignInLink);

      // Verify the link points to /login
      expect(heroSignInLink).toHaveAttribute('href', '/login');
    });
  });

  describe('Test Case 3: Navbar Sign In link styling', () => {
    it('should render Sign In link in navbar with proper styling', () => {
      renderWithProviders(<Navbar />, {
        authOptions: { isAuthenticated: false },
        useMemoryRouter: true,
      });

      const signInLink = screen.getByTestId('sign-in-link');

      // Verify it's a link element
      expect(signInLink.tagName).toBe('A');

      // Verify proper href
      expect(signInLink).toHaveAttribute('href', '/login');

      // Verify it has button styling classes
      expect(signInLink).toHaveClass('btn');

      // Verify accessibility
      expect(signInLink).toHaveAttribute('aria-label', 'Sign in to your account');
    });

    it('should render Sign In link with accessible text content', () => {
      renderWithProviders(<Navbar />, {
        authOptions: { isAuthenticated: false },
        useMemoryRouter: true,
      });

      const signInLink = screen.getByRole('link', { name: /sign in/i });
      expect(signInLink).toBeInTheDocument();
      expect(signInLink).toBeVisible();
    });
  });
});

describe('Get Started CTA Navigation (Scenario 2 placeholder)', () => {
  // These tests will be implemented by Scenario 2
  it.todo('should display Get Started button when user is unauthenticated');
  it.todo('should navigate to /register when clicking Get Started');
});
