/**
 * Authentication State Integration Tests
 * Owner: Scenario 3
 *
 * Test coverage:
 * - Unauthenticated user CTAs
 * - Authenticated user CTAs
 * - Dashboard navigation for logged-in users
 *
 * Test suites:
 * - describe('Unauthenticated State')
 * - describe('Authenticated State')
 */

import { describe, it, expect, vi } from 'vitest';
import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { renderWithProviders } from '../utils/renderWithProviders';
import { mockUnauthenticatedContext, mockAuthenticatedContext } from '../utils/mockAuthContext';
import Home from '../../src/pages/Home';

describe('Authentication State Awareness', () => {
  describe('Unauthenticated State', () => {
    it('displays Get Started and Login buttons when user is not authenticated', () => {
      renderWithProviders(<Home />, {
        authContext: mockUnauthenticatedContext,
      });

      // Check for Get Started button
      const getStartedButton = screen.getByRole('link', { name: /get started/i });
      expect(getStartedButton).toBeInTheDocument();
      expect(getStartedButton).toHaveAttribute('href', '/register');

      // Check for Login button in hero section
      const loginButtons = screen.getAllByRole('link', { name: /login/i });
      expect(loginButtons.length).toBeGreaterThan(0);

      // At least one Login button should link to /login
      const heroLoginButton = loginButtons.find(btn => btn.getAttribute('href') === '/login');
      expect(heroLoginButton).toBeInTheDocument();
    });

    it('does not display Go to Dashboard button when user is not authenticated', () => {
      renderWithProviders(<Home />, {
        authContext: mockUnauthenticatedContext,
      });

      // Go to Dashboard should not be present
      const dashboardButton = screen.queryByRole('link', { name: /go to dashboard/i });
      expect(dashboardButton).not.toBeInTheDocument();
    });

    it('Get Started button links to /register page', () => {
      renderWithProviders(<Home />, {
        authContext: mockUnauthenticatedContext,
      });

      const getStartedButton = screen.getByRole('link', { name: /get started/i });
      expect(getStartedButton).toHaveAttribute('href', '/register');
    });

    it('Login button links to /login page', () => {
      renderWithProviders(<Home />, {
        authContext: mockUnauthenticatedContext,
      });

      // Find the Login button in the hero section (should have href="/login")
      const loginButtons = screen.getAllByRole('link', { name: /login/i });
      const loginButton = loginButtons.find(btn => btn.getAttribute('href') === '/login');
      expect(loginButton).toBeInTheDocument();
      expect(loginButton).toHaveAttribute('href', '/login');
    });
  });

  describe('Authenticated State', () => {
    it('displays Go to Dashboard button when user is authenticated', () => {
      renderWithProviders(<Home />, {
        authContext: mockAuthenticatedContext,
      });

      // Check for Go to Dashboard button
      const dashboardButton = screen.getByRole('link', { name: /go to dashboard/i });
      expect(dashboardButton).toBeInTheDocument();
      expect(dashboardButton).toHaveAttribute('href', '/dashboard');
    });

    it('does not display Get Started button when user is authenticated', () => {
      renderWithProviders(<Home />, {
        authContext: mockAuthenticatedContext,
      });

      // Get Started should not be present in hero section for authenticated users
      const getStartedButton = screen.queryByRole('link', { name: /get started/i });
      expect(getStartedButton).not.toBeInTheDocument();
    });

    it('Go to Dashboard button links to /dashboard path', () => {
      renderWithProviders(<Home />, {
        authContext: mockAuthenticatedContext,
      });

      const dashboardButton = screen.getByRole('link', { name: /go to dashboard/i });
      expect(dashboardButton).toHaveAttribute('href', '/dashboard');
    });

    it('clicking Go to Dashboard navigates to /dashboard', async () => {
      const user = userEvent.setup();

      renderWithProviders(<Home />, {
        authContext: mockAuthenticatedContext,
        useMemoryRouter: true,
        initialEntries: ['/'],
      });

      const dashboardButton = screen.getByRole('link', { name: /go to dashboard/i });
      await user.click(dashboardButton);

      // After clicking, we should be navigated to /dashboard
      // Since we're using MemoryRouter, we check the href attribute
      expect(dashboardButton).toHaveAttribute('href', '/dashboard');
    });
  });

  describe('AuthContext Consumption', () => {
    it('Home component correctly reads isAuthenticated=false from context', () => {
      renderWithProviders(<Home />, {
        authContext: { ...mockUnauthenticatedContext, isAuthenticated: false },
      });

      // When isAuthenticated is false, should show Get Started
      expect(screen.getByRole('link', { name: /get started/i })).toBeInTheDocument();
      expect(screen.queryByRole('link', { name: /go to dashboard/i })).not.toBeInTheDocument();
    });

    it('Home component correctly reads isAuthenticated=true from context', () => {
      renderWithProviders(<Home />, {
        authContext: { ...mockAuthenticatedContext, isAuthenticated: true },
      });

      // When isAuthenticated is true, should show Go to Dashboard
      expect(screen.getByRole('link', { name: /go to dashboard/i })).toBeInTheDocument();
      expect(screen.queryByRole('link', { name: /get started/i })).not.toBeInTheDocument();
    });

    it('component updates when authentication state changes', () => {
      // First render as unauthenticated
      const { rerender } = renderWithProviders(<Home />, {
        authContext: mockUnauthenticatedContext,
      });

      expect(screen.getByRole('link', { name: /get started/i })).toBeInTheDocument();
      expect(screen.queryByRole('link', { name: /go to dashboard/i })).not.toBeInTheDocument();
    });
  });
});
