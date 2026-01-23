/**
 * Navigation Integration Tests
 * Owner: Scenarios 2, 20
 *
 * Test coverage:
 * - Scenario 2: CTA button navigation
 * - Scenario 20: Route configuration
 *
 * Test suites:
 * - describe('CTA Navigation')
 * - describe('Route Configuration')
 * - describe('Public Route Access')
 */

import { describe, it, expect } from 'vitest';
import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { renderWithProviders, createMockAuthContext } from '../utils/renderWithProviders';
import HeroSection from '../../src/components/homepage/HeroSection';
import Home from '../../src/pages/Home';
import Login from '../../src/pages/Login';
import Register from '../../src/pages/Register';
import { Routes, Route } from 'react-router-dom';
import React from 'react';

// Test app component that includes all routes for navigation testing
const TestApp: React.FC = () => {
  return (
    <Routes>
      <Route path="/" element={<Home />} />
      <Route path="/login" element={<Login />} />
      <Route path="/register" element={<Register />} />
    </Routes>
  );
};

describe('CTA Navigation', () => {
  describe('Get Started Button', () => {
    it('should navigate to /register when Get Started button is clicked', async () => {
      const user = userEvent.setup();

      renderWithProviders(<TestApp />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
        initialEntries: ['/'],
        useMemoryRouter: true,
      });

      // Find the Get Started button
      const getStartedButton = screen.getByTestId('get-started-button');
      expect(getStartedButton).toBeInTheDocument();
      expect(getStartedButton).toHaveTextContent('Get Started');

      // Click the button
      await user.click(getStartedButton);

      // Verify navigation to register page
      await waitFor(() => {
        expect(screen.getByRole('heading', { name: 'Register' })).toBeInTheDocument();
      });
    });

    it('should have correct href attribute linking to /register', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const getStartedButton = screen.getByTestId('get-started-button');
      expect(getStartedButton).toHaveAttribute('href', '/register');
    });
  });

  describe('Login Button', () => {
    it('should navigate to /login when Login button is clicked', async () => {
      const user = userEvent.setup();

      renderWithProviders(<TestApp />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
        initialEntries: ['/'],
        useMemoryRouter: true,
      });

      // Find the Login button
      const loginButton = screen.getByTestId('login-button');
      expect(loginButton).toBeInTheDocument();
      expect(loginButton).toHaveTextContent('Login');

      // Click the button
      await user.click(loginButton);

      // Verify navigation to login page
      await waitFor(() => {
        expect(screen.getByRole('heading', { name: 'Login' })).toBeInTheDocument();
      });
    });

    it('should have correct href attribute linking to /login', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const loginButton = screen.getByTestId('login-button');
      expect(loginButton).toHaveAttribute('href', '/login');
    });
  });
});

describe('Route Configuration', () => {
  it('should render Home page at / route', () => {
    renderWithProviders(<TestApp />, {
      authContext: createMockAuthContext({ isAuthenticated: false }),
      initialEntries: ['/'],
      useMemoryRouter: true,
    });

    // Verify hero section content is displayed
    expect(screen.getByText(/Shorten Links/)).toBeInTheDocument();
    expect(screen.getByTestId('get-started-button')).toBeInTheDocument();
  });

  it('should render Login page at /login route', () => {
    renderWithProviders(<TestApp />, {
      authContext: createMockAuthContext({ isAuthenticated: false }),
      initialEntries: ['/login'],
      useMemoryRouter: true,
    });

    expect(screen.getByRole('heading', { name: 'Login' })).toBeInTheDocument();
  });

  it('should render Register page at /register route', () => {
    renderWithProviders(<TestApp />, {
      authContext: createMockAuthContext({ isAuthenticated: false }),
      initialEntries: ['/register'],
      useMemoryRouter: true,
    });

    expect(screen.getByRole('heading', { name: 'Register' })).toBeInTheDocument();
  });

  // Scenario 20: Test Case 1 - Navigate to '/' route renders Home component
  it('should render Home component when navigating to root route', () => {
    renderWithProviders(<TestApp />, {
      authContext: createMockAuthContext({ isAuthenticated: false }),
      initialEntries: ['/'],
      useMemoryRouter: true,
    });

    // Verify Home component is rendered with its main sections
    // Use getAllByRole for navigation since there may be multiple nav elements (Navbar and Footer nav)
    const navElements = screen.getAllByRole('navigation');
    expect(navElements.length).toBeGreaterThanOrEqual(1); // At least one navigation element
    expect(screen.getByRole('main')).toBeInTheDocument(); // Main content area
    expect(screen.getByText(/Shorten Links/)).toBeInTheDocument(); // Hero headline
  });

  // Scenario 20: Test Case 2 - Access '/' without auth token renders without redirect
  it('should render homepage without redirecting to login when no authentication token', () => {
    renderWithProviders(<TestApp />, {
      authContext: createMockAuthContext({
        isAuthenticated: false,
        loading: false,
        user: null
      }),
      initialEntries: ['/'],
      useMemoryRouter: true,
    });

    // Should NOT show login page (no redirect happened)
    expect(screen.queryByRole('heading', { name: 'Login' })).not.toBeInTheDocument();

    // Should show Home page content
    expect(screen.getByText(/Shorten Links/)).toBeInTheDocument();
    expect(screen.getByTestId('get-started-button')).toBeInTheDocument();
  });

  // Scenario 20: Test Case 3 - Home component is not wrapped in ProtectedLayout
  it('should render homepage as publicly accessible (not wrapped in ProtectedLayout)', () => {
    // When loading is true and user is not authenticated, ProtectedLayout would show loading spinner
    // Home page should render immediately without showing loading state
    renderWithProviders(<TestApp />, {
      authContext: createMockAuthContext({
        isAuthenticated: false,
        loading: true, // Simulating loading state that would affect ProtectedLayout
        user: null
      }),
      initialEntries: ['/'],
      useMemoryRouter: true,
    });

    // Home should render even when auth is loading (not waiting for auth check like ProtectedLayout would)
    expect(screen.getByText(/Shorten Links/)).toBeInTheDocument();

    // Should NOT show loading spinner (which ProtectedLayout would show)
    expect(screen.queryByText('Loading...')).not.toBeInTheDocument();
  });
});

describe('Public Route Access', () => {
  it('should allow unauthenticated users to access home page', () => {
    renderWithProviders(<TestApp />, {
      authContext: createMockAuthContext({ isAuthenticated: false }),
      initialEntries: ['/'],
      useMemoryRouter: true,
    });

    expect(screen.getByText(/Shorten Links/)).toBeInTheDocument();
  });

  it('should allow unauthenticated users to access login page', () => {
    renderWithProviders(<TestApp />, {
      authContext: createMockAuthContext({ isAuthenticated: false }),
      initialEntries: ['/login'],
      useMemoryRouter: true,
    });

    expect(screen.getByRole('heading', { name: 'Login' })).toBeInTheDocument();
  });

  it('should allow unauthenticated users to access register page', () => {
    renderWithProviders(<TestApp />, {
      authContext: createMockAuthContext({ isAuthenticated: false }),
      initialEntries: ['/register'],
      useMemoryRouter: true,
    });

    expect(screen.getByRole('heading', { name: 'Register' })).toBeInTheDocument();
  });

  // Scenario 20: Additional public access verification
  it('should allow authenticated users to access home page without redirect', () => {
    renderWithProviders(<TestApp />, {
      authContext: createMockAuthContext({
        isAuthenticated: true,
        user: { id: 1, username: 'testuser', email: 'test@test.com', is_admin: 0 }
      }),
      initialEntries: ['/'],
      useMemoryRouter: true,
    });

    // Home page should render for authenticated users too
    expect(screen.getByText(/Shorten Links/)).toBeInTheDocument();
  });

  it('should render home page immediately without authentication check delay', () => {
    // Test that home page doesn't wait for auth loading to complete
    renderWithProviders(<TestApp />, {
      authContext: createMockAuthContext({
        isAuthenticated: false,
        loading: true // Auth is still loading
      }),
      initialEntries: ['/'],
      useMemoryRouter: true,
    });

    // Should render content immediately, not wait for auth
    expect(screen.getByText(/Shorten Links/)).toBeInTheDocument();
    expect(screen.getByRole('main')).toBeInTheDocument();
  });
});
