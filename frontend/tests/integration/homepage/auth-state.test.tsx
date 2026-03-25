/**
 * Auth State Integration Tests
 * Owner: Scenario 12 - Authenticated User Experience
 *
 * Tests CTAs adapt based on authentication state (REQ-12, US-6, NFR-4)
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import React, { ReactNode } from 'react';
import { Home } from '../../../src/pages/Home';
import { AuthProvider, useAuth } from '../../../src/contexts/AuthContext';
import { ThemeProvider } from '../../../src/contexts/ThemeContext';

// Test wrapper with configurable auth state
interface TestWrapperProps {
  children: ReactNode;
  isAuthenticated?: boolean;
  user?: { id: string; username: string; email: string } | null;
  initialRoute?: string;
}

function TestWrapper({
  children,
  isAuthenticated = false,
  user = null,
  initialRoute = '/',
}: TestWrapperProps) {
  const authState = {
    isAuthenticated,
    user,
  };

  return (
    <MemoryRouter initialEntries={[initialRoute]}>
      <AuthProvider initialState={authState}>
        <ThemeProvider initialTheme="light">
          <Routes>
            <Route path="/" element={children} />
            <Route path="/dashboard" element={<div data-testid="dashboard-page">Dashboard Page</div>} />
            <Route path="/register" element={<div data-testid="register-page">Register Page</div>} />
            <Route path="/login" element={<div data-testid="login-page">Login Page</div>} />
          </Routes>
        </ThemeProvider>
      </AuthProvider>
    </MemoryRouter>
  );
}

// Helper component to test logout functionality
function HomeWithLogoutTest() {
  const { logout, isAuthenticated } = useAuth();

  return (
    <div>
      <Home />
      <button data-testid="trigger-logout" onClick={logout}>
        Trigger Logout
      </button>
      <span data-testid="auth-status">{isAuthenticated ? 'authenticated' : 'unauthenticated'}</span>
    </div>
  );
}

describe('Authenticated User Experience', () => {
  describe('Test Case 1: Unauthenticated state shows Get Started button', () => {
    it('renders Get Started button when user is not authenticated', () => {
      render(
        <TestWrapper isAuthenticated={false}>
          <Home />
        </TestWrapper>
      );

      // Check hero section CTA
      const getStartedButton = screen.getByTestId('hero-get-started-button');
      expect(getStartedButton).toBeInTheDocument();
      expect(getStartedButton).toHaveTextContent('Get Started');
    });

    it('renders Sign In link in hero when user is not authenticated', () => {
      render(
        <TestWrapper isAuthenticated={false}>
          <Home />
        </TestWrapper>
      );

      const signInLink = screen.getByTestId('hero-sign-in-link');
      expect(signInLink).toBeInTheDocument();
      expect(signInLink).toHaveTextContent('Sign In');
    });

    it('renders Get Started and Sign In in navbar when unauthenticated', () => {
      render(
        <TestWrapper isAuthenticated={false}>
          <Home />
        </TestWrapper>
      );

      const navbarGetStarted = screen.getByTestId('get-started-link');
      const navbarSignIn = screen.getByTestId('sign-in-link');

      expect(navbarGetStarted).toBeInTheDocument();
      expect(navbarSignIn).toBeInTheDocument();
    });

    it('does not show Go to Dashboard button when unauthenticated', () => {
      render(
        <TestWrapper isAuthenticated={false}>
          <Home />
        </TestWrapper>
      );

      expect(screen.queryByTestId('hero-dashboard-button')).not.toBeInTheDocument();
    });
  });

  describe('Test Case 2: Authenticated state shows Go to Dashboard button', () => {
    const mockUser = {
      id: '1',
      username: 'testuser',
      email: 'test@example.com',
    };

    it('renders Go to Dashboard button when user is authenticated', () => {
      render(
        <TestWrapper isAuthenticated={true} user={mockUser}>
          <Home />
        </TestWrapper>
      );

      const dashboardButton = screen.getByTestId('hero-dashboard-button');
      expect(dashboardButton).toBeInTheDocument();
      expect(dashboardButton).toHaveTextContent('Go to Dashboard');
    });

    it('does not render Get Started button when authenticated', () => {
      render(
        <TestWrapper isAuthenticated={true} user={mockUser}>
          <Home />
        </TestWrapper>
      );

      expect(screen.queryByTestId('hero-get-started-button')).not.toBeInTheDocument();
    });

    it('does not render hero Sign In link when authenticated', () => {
      render(
        <TestWrapper isAuthenticated={true} user={mockUser}>
          <Home />
        </TestWrapper>
      );

      expect(screen.queryByTestId('hero-sign-in-link')).not.toBeInTheDocument();
    });

    it('renders Dashboard link in navbar when authenticated', () => {
      render(
        <TestWrapper isAuthenticated={true} user={mockUser}>
          <Home />
        </TestWrapper>
      );

      const navbarDashboard = screen.getByTestId('dashboard-link');
      expect(navbarDashboard).toBeInTheDocument();
      expect(navbarDashboard).toHaveTextContent('Dashboard');
    });

    it('renders logout button in navbar when authenticated', () => {
      render(
        <TestWrapper isAuthenticated={true} user={mockUser}>
          <Home />
        </TestWrapper>
      );

      const logoutButton = screen.getByTestId('logout-button');
      expect(logoutButton).toBeInTheDocument();
      expect(logoutButton).toHaveTextContent('Logout');
    });
  });

  describe('Test Case 3: Authenticated user clicks Go to Dashboard navigates to /dashboard', () => {
    const mockUser = {
      id: '1',
      username: 'testuser',
      email: 'test@example.com',
    };

    it('navigates to /dashboard when clicking Go to Dashboard button', async () => {
      const user = userEvent.setup();

      render(
        <TestWrapper isAuthenticated={true} user={mockUser}>
          <Home />
        </TestWrapper>
      );

      const dashboardButton = screen.getByTestId('hero-dashboard-button');
      await user.click(dashboardButton);

      await waitFor(() => {
        expect(screen.getByTestId('dashboard-page')).toBeInTheDocument();
      });
    });

    it('navigates to /dashboard when clicking navbar Dashboard link', async () => {
      const user = userEvent.setup();

      render(
        <TestWrapper isAuthenticated={true} user={mockUser}>
          <Home />
        </TestWrapper>
      );

      const navbarDashboard = screen.getByTestId('dashboard-link');
      await user.click(navbarDashboard);

      await waitFor(() => {
        expect(screen.getByTestId('dashboard-page')).toBeInTheDocument();
      });
    });

    it('Go to Dashboard button has correct href attribute', () => {
      render(
        <TestWrapper isAuthenticated={true} user={mockUser}>
          <Home />
        </TestWrapper>
      );

      const dashboardButton = screen.getByTestId('hero-dashboard-button');
      expect(dashboardButton).toHaveAttribute('href', '/dashboard');
    });
  });

  describe('Test Case 4: Username displayed in navigation for authenticated users', () => {
    it('displays username when user is authenticated', () => {
      const mockUser = {
        id: '1',
        username: 'johndoe',
        email: 'john@example.com',
      };

      render(
        <TestWrapper isAuthenticated={true} user={mockUser}>
          <Home />
        </TestWrapper>
      );

      const userDisplay = screen.getByTestId('user-display');
      expect(userDisplay).toBeInTheDocument();
      expect(userDisplay).toHaveTextContent('johndoe');
    });

    it('displays welcome message with username', () => {
      const mockUser = {
        id: '2',
        username: 'janesmith',
        email: 'jane@example.com',
      };

      render(
        <TestWrapper isAuthenticated={true} user={mockUser}>
          <Home />
        </TestWrapper>
      );

      const userDisplay = screen.getByTestId('user-display');
      expect(userDisplay).toHaveTextContent('Welcome, janesmith');
    });

    it('does not display user info when unauthenticated', () => {
      render(
        <TestWrapper isAuthenticated={false}>
          <Home />
        </TestWrapper>
      );

      expect(screen.queryByTestId('user-display')).not.toBeInTheDocument();
    });
  });

  describe('Test Case 5: Logout reverts CTAs to unauthenticated state', () => {
    const mockUser = {
      id: '1',
      username: 'testuser',
      email: 'test@example.com',
    };

    it('shows authenticated CTAs before logout', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <AuthProvider initialState={{ isAuthenticated: true, user: mockUser }}>
            <ThemeProvider initialTheme="light">
              <Routes>
                <Route path="/" element={<HomeWithLogoutTest />} />
              </Routes>
            </ThemeProvider>
          </AuthProvider>
        </MemoryRouter>
      );

      expect(screen.getByTestId('auth-status')).toHaveTextContent('authenticated');
      expect(screen.getByTestId('hero-dashboard-button')).toBeInTheDocument();
    });

    it('reverts to unauthenticated CTAs after logout', async () => {
      const user = userEvent.setup();

      render(
        <MemoryRouter initialEntries={['/']}>
          <AuthProvider initialState={{ isAuthenticated: true, user: mockUser }}>
            <ThemeProvider initialTheme="light">
              <Routes>
                <Route path="/" element={<HomeWithLogoutTest />} />
              </Routes>
            </ThemeProvider>
          </AuthProvider>
        </MemoryRouter>
      );

      // Initially authenticated
      expect(screen.getByTestId('auth-status')).toHaveTextContent('authenticated');
      expect(screen.getByTestId('hero-dashboard-button')).toBeInTheDocument();
      expect(screen.queryByTestId('hero-get-started-button')).not.toBeInTheDocument();

      // Click logout button
      const logoutTrigger = screen.getByTestId('trigger-logout');
      await user.click(logoutTrigger);

      // After logout, should show unauthenticated state
      await waitFor(() => {
        expect(screen.getByTestId('auth-status')).toHaveTextContent('unauthenticated');
      });

      // Verify CTAs have reverted
      await waitFor(() => {
        expect(screen.getByTestId('hero-get-started-button')).toBeInTheDocument();
      });
      expect(screen.queryByTestId('hero-dashboard-button')).not.toBeInTheDocument();
    });

    it('clicking navbar logout button triggers logout', async () => {
      const user = userEvent.setup();

      render(
        <MemoryRouter initialEntries={['/']}>
          <AuthProvider initialState={{ isAuthenticated: true, user: mockUser }}>
            <ThemeProvider initialTheme="light">
              <Routes>
                <Route path="/" element={<HomeWithLogoutTest />} />
              </Routes>
            </ThemeProvider>
          </AuthProvider>
        </MemoryRouter>
      );

      // Initially authenticated
      expect(screen.getByTestId('logout-button')).toBeInTheDocument();

      // Click the navbar logout button
      const logoutButton = screen.getByTestId('logout-button');
      await user.click(logoutButton);

      // After logout, should show unauthenticated state
      await waitFor(() => {
        expect(screen.getByTestId('auth-status')).toHaveTextContent('unauthenticated');
      });

      // Navbar should now show Sign In and Get Started
      await waitFor(() => {
        expect(screen.getByTestId('sign-in-link')).toBeInTheDocument();
        expect(screen.getByTestId('get-started-link')).toBeInTheDocument();
      });
    });

    it('username disappears from navbar after logout', async () => {
      const user = userEvent.setup();

      render(
        <MemoryRouter initialEntries={['/']}>
          <AuthProvider initialState={{ isAuthenticated: true, user: mockUser }}>
            <ThemeProvider initialTheme="light">
              <Routes>
                <Route path="/" element={<HomeWithLogoutTest />} />
              </Routes>
            </ThemeProvider>
          </AuthProvider>
        </MemoryRouter>
      );

      // Initially shows username
      expect(screen.getByTestId('user-display')).toBeInTheDocument();
      expect(screen.getByTestId('user-display')).toHaveTextContent('testuser');

      // Click logout
      const logoutButton = screen.getByTestId('logout-button');
      await user.click(logoutButton);

      // Username should disappear
      await waitFor(() => {
        expect(screen.queryByTestId('user-display')).not.toBeInTheDocument();
      });
    });
  });

  describe('Edge cases and additional coverage', () => {
    it('handles undefined user gracefully when authenticated', () => {
      render(
        <TestWrapper isAuthenticated={true} user={undefined as any}>
          <Home />
        </TestWrapper>
      );

      // Should still show dashboard button
      expect(screen.getByTestId('hero-dashboard-button')).toBeInTheDocument();
    });

    it('Get Started button navigates to register page', async () => {
      const user = userEvent.setup();

      render(
        <TestWrapper isAuthenticated={false}>
          <Home />
        </TestWrapper>
      );

      const getStartedButton = screen.getByTestId('hero-get-started-button');
      await user.click(getStartedButton);

      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument();
      });
    });

    it('Sign In link navigates to login page', async () => {
      const user = userEvent.setup();

      render(
        <TestWrapper isAuthenticated={false}>
          <Home />
        </TestWrapper>
      );

      const signInLink = screen.getByTestId('hero-sign-in-link');
      await user.click(signInLink);

      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument();
      });
    });
  });
});
