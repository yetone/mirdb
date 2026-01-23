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
});
