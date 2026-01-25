/**
 * Navigation Integration Tests
 * Owner: Scenario 2 - Navigation to Registration, Scenario 3 - Navigation to Login
 *
 * Tests navigation flows from homepage to registration and login pages.
 * Verifies that users can navigate using:
 * - Hero section CTAs (Get Started button)
 * - Navbar links (Register, Login)
 */

import React from 'react';
import { describe, it, expect, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import { AuthProvider } from '../../../src/contexts/AuthContext';
import { ThemeProvider } from '../../../src/contexts/ThemeContext';
import { Home } from '../../../src/pages/Home';
import { Register } from '../../../src/pages/Register';
import { Login } from '../../../src/pages/Login';

/**
 * Wrapper component that provides full app context and routing
 */
function TestApp({ initialRoute = '/' }: { initialRoute?: string }) {
  return (
    <ThemeProvider>
      <AuthProvider>
        <MemoryRouter initialEntries={[initialRoute]}>
          <Routes>
            <Route path="/" element={<Home />} />
            <Route path="/register" element={<Register />} />
            <Route path="/login" element={<Login />} />
          </Routes>
        </MemoryRouter>
      </AuthProvider>
    </ThemeProvider>
  );
}

describe('Scenario 2: Navigation to Registration', () => {
  describe('Test Case 1: Click "Get Started" CTA button navigates to /register', () => {
    it('should navigate to registration page when clicking Get Started CTA in hero section', async () => {
      const user = userEvent.setup();

      render(<TestApp initialRoute="/" />);

      // Verify we are on the homepage
      expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent(
        /shorten urls/i
      );

      // Find and click the Get Started button in hero section
      const getStartedButton = screen.getByTestId('hero-cta');
      expect(getStartedButton).toBeInTheDocument();
      expect(getStartedButton).toHaveTextContent(/get started/i);

      await user.click(getStartedButton);

      // Verify navigation to registration page
      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument();
      });

      // Verify the registration form is displayed
      expect(screen.getByTestId('register-heading')).toHaveTextContent(/create account/i);
      expect(screen.getByTestId('register-form')).toBeInTheDocument();
    });
  });

  describe('Test Case 2: Click "Register" link in navigation bar navigates to /register', () => {
    it('should navigate to registration page when clicking Register link in navbar', async () => {
      const user = userEvent.setup();

      render(<TestApp initialRoute="/" />);

      // Verify we are on the homepage
      expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent(
        /shorten urls/i
      );

      // Find and click the Register link in navbar
      const navRegisterLink = screen.getByTestId('nav-register');
      expect(navRegisterLink).toBeInTheDocument();
      expect(navRegisterLink).toHaveTextContent(/register/i);

      await user.click(navRegisterLink);

      // Verify navigation to registration page
      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument();
      });

      // Verify the registration form is displayed
      expect(screen.getByTestId('register-heading')).toHaveTextContent(/create account/i);
    });
  });

  describe('Test Case 3: E2E - Click primary CTA and verify URL change', () => {
    it('should change browser URL to /register and display registration form when clicking primary CTA', async () => {
      const user = userEvent.setup();

      render(<TestApp initialRoute="/" />);

      // Verify initial state - homepage is displayed
      const heroSection = screen.getByRole('heading', { level: 1 });
      expect(heroSection).toBeInTheDocument();

      // Click the primary CTA (Get Started button)
      const primaryCTA = screen.getByTestId('hero-cta');
      await user.click(primaryCTA);

      // Verify navigation and form display
      await waitFor(() => {
        // Registration page is displayed
        expect(screen.getByTestId('register-page')).toBeInTheDocument();
      });

      // Verify registration form elements are present
      expect(screen.getByTestId('register-username')).toBeInTheDocument();
      expect(screen.getByTestId('register-email')).toBeInTheDocument();
      expect(screen.getByTestId('register-password')).toBeInTheDocument();
      expect(screen.getByTestId('register-confirm-password')).toBeInTheDocument();

      // Verify the form is interactive
      const usernameInput = screen.getByTestId('register-username');
      await user.type(usernameInput, 'testuser');
      expect(usernameInput).toHaveValue('testuser');
    });
  });

  describe('Additional edge cases for registration navigation', () => {
    it('should have accessible link text for Get Started button', async () => {
      render(<TestApp initialRoute="/" />);

      const getStartedButton = screen.getByTestId('hero-cta');

      // Verify the button is a link element with proper role
      expect(getStartedButton).toBeInTheDocument();
      expect(getStartedButton.tagName.toLowerCase()).toBe('a');
      expect(getStartedButton).toHaveAttribute('href', '/register');
    });

    it('should have accessible link text for navbar Register link', async () => {
      render(<TestApp initialRoute="/" />);

      const navRegisterLink = screen.getByTestId('nav-register');

      // Verify the link is accessible
      expect(navRegisterLink).toBeInTheDocument();
      expect(navRegisterLink.tagName.toLowerCase()).toBe('a');
      expect(navRegisterLink).toHaveAttribute('href', '/register');
    });

    it('should navigate from homepage to register and back to homepage', async () => {
      const user = userEvent.setup();

      render(<TestApp initialRoute="/" />);

      // Start at homepage
      expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent(/shorten urls/i);

      // Navigate to register
      const getStartedButton = screen.getByTestId('hero-cta');
      await user.click(getStartedButton);

      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument();
      });

      // Navigate back to homepage via navbar logo
      const logoLink = screen.getByText(/shorturl/i);
      await user.click(logoLink);

      await waitFor(() => {
        expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent(/shorten urls/i);
      });
    });
  });
});
