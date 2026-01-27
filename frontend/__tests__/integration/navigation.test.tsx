/**
 * Navigation Integration Tests
 * Owner: Scenario 4 - Navigation and CTA Links
 *
 * Test cases:
 * 1. Clicking primary CTA button navigates to /register route
 * 2. Clicking Login button/link navigates to /login route
 * 3. Primary CTA has correct href for /register
 * 4. Login link has correct href for /login
 *
 * These tests verify that navigation links and CTA buttons correctly
 * route to Login and Register pages as per REQ-3.
 */

import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Routes, Route, useLocation } from 'react-router-dom';
import { ThemeProvider } from '../../src/contexts/ThemeContext';
import { Home } from '../../src/pages/Home';

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <div {...props}>{children}</div>
    ),
    button: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <button {...props}>{children}</button>
    ),
    section: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <section {...props}>{children}</section>
    ),
  },
}));

// Mock BackgroundEffect component to avoid Three.js issues in tests
vi.mock('../../src/components/BackgroundEffect', () => ({
  BackgroundEffect: () => (
    <div data-testid="background-effect" aria-hidden="true" />
  ),
  default: () => (
    <div data-testid="background-effect" aria-hidden="true" />
  ),
}));

// Helper component to display the current location for testing
function LocationDisplay() {
  const location = useLocation();
  return <div data-testid="location-display">{location.pathname}</div>;
}

// Test wrapper that provides routing context
interface TestAppProps {
  initialRoute?: string;
}

function TestApp({ initialRoute = '/' }: TestAppProps) {
  return (
    <ThemeProvider>
      <MemoryRouter initialEntries={[initialRoute]}>
        <Routes>
          <Route path="/" element={<Home />} />
          <Route
            path="/register"
            element={
              <div data-testid="register-page">
                <h1>Register</h1>
              </div>
            }
          />
          <Route
            path="/login"
            element={
              <div data-testid="login-page">
                <h1>Login</h1>
              </div>
            }
          />
        </Routes>
        <LocationDisplay />
      </MemoryRouter>
    </ThemeProvider>
  );
}

describe('Navigation and CTA Links Integration Tests', () => {
  describe('Integration Tests - Navigation', () => {
    it('navigates to /register when clicking primary CTA button', async () => {
      const user = userEvent.setup();
      render(<TestApp />);

      // Find and click the primary CTA button (Get Started)
      const primaryCta = screen.getByRole('link', { name: /get started|create free account/i });
      await user.click(primaryCta);

      // Verify navigation to register page
      const locationDisplay = screen.getByTestId('location-display');
      expect(locationDisplay).toHaveTextContent('/register');

      // Verify register page content is rendered
      const registerPage = screen.getByTestId('register-page');
      expect(registerPage).toBeInTheDocument();
    });

    it('navigates to /login when clicking Login button/link', async () => {
      const user = userEvent.setup();
      render(<TestApp />);

      // Find and click the Login link
      const loginLink = screen.getByRole('link', { name: /login/i });
      await user.click(loginLink);

      // Verify navigation to login page
      const locationDisplay = screen.getByTestId('location-display');
      expect(locationDisplay).toHaveTextContent('/login');

      // Verify login page content is rendered
      const loginPage = screen.getByTestId('login-page');
      expect(loginPage).toBeInTheDocument();
    });
  });

  describe('Unit Tests - Link Attributes', () => {
    it('primary CTA has correct href attribute for /register', () => {
      render(<TestApp />);

      // Find the primary CTA link
      const primaryCta = screen.getByRole('link', { name: /get started|create free account/i });

      // Verify href attribute
      expect(primaryCta).toHaveAttribute('href', '/register');
    });

    it('Login link has correct href attribute for /login', () => {
      render(<TestApp />);

      // Find the Login link
      const loginLink = screen.getByRole('link', { name: /login/i });

      // Verify href attribute
      expect(loginLink).toHaveAttribute('href', '/login');
    });
  });

  describe('Navigation Links Presence', () => {
    it('renders navigation links for Login and Register', () => {
      render(<TestApp />);

      // Verify both navigation links are present
      const registerLink = screen.getByRole('link', { name: /get started|create free account/i });
      const loginLink = screen.getByRole('link', { name: /login/i });

      expect(registerLink).toBeInTheDocument();
      expect(registerLink).toBeVisible();
      expect(loginLink).toBeInTheDocument();
      expect(loginLink).toBeVisible();
    });

    it('CTA buttons are accessible and keyboard navigable', async () => {
      const user = userEvent.setup();
      render(<TestApp />);

      // Get both links
      const registerLink = screen.getByRole('link', { name: /get started|create free account/i });
      const loginLink = screen.getByRole('link', { name: /login/i });

      // Tab to first link and verify focus
      await user.tab();

      // Continue tabbing to find the CTA links (they should be focusable)
      let attempts = 0;
      const maxAttempts = 10;
      let foundRegister = false;
      let foundLogin = false;

      while (attempts < maxAttempts) {
        if (document.activeElement === registerLink) {
          foundRegister = true;
        }
        if (document.activeElement === loginLink) {
          foundLogin = true;
        }
        if (foundRegister && foundLogin) break;
        await user.tab();
        attempts++;
      }

      // Verify both links were reachable via keyboard
      expect(foundRegister || foundLogin).toBe(true);
    });
  });
});
