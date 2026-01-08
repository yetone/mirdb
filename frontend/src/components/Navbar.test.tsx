import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { BrowserRouter, MemoryRouter, useLocation } from 'react-router-dom';
import Navbar from './Navbar';

// Helper component to display current location for testing
function LocationDisplay() {
  const location = useLocation();
  return <div data-testid="location-display">{location.pathname}</div>;
}

describe('Navbar', () => {
  // Test Case 1: Login button/link is visible in navigation
  describe('Login button visibility', () => {
    it('renders Login button/link visible in navigation', () => {
      render(
        <BrowserRouter>
          <Navbar />
        </BrowserRouter>
      );

      // Use aria-label to find the primary login link (not mobile menu)
      const loginLink = screen.getByLabelText(/go to login page/i);
      expect(loginLink).toBeInTheDocument();
      expect(loginLink).toBeVisible();
    });

    it('Login link has correct href to /login', () => {
      render(
        <BrowserRouter>
          <Navbar />
        </BrowserRouter>
      );

      // Use aria-label to find the primary login link
      const loginLink = screen.getByLabelText(/go to login page/i);
      expect(loginLink).toHaveAttribute('href', '/login');
    });
  });

  // Test Case 2: Register/Sign Up button/link is visible in navigation
  describe('Register/Sign Up button visibility', () => {
    it('renders Register/Sign Up button/link visible in navigation', () => {
      render(
        <BrowserRouter>
          <Navbar />
        </BrowserRouter>
      );

      // Use aria-label to find the primary sign up link
      const signUpLink = screen.getByLabelText(/go to registration page/i);
      expect(signUpLink).toBeInTheDocument();
      expect(signUpLink).toBeVisible();
    });

    it('Sign Up link has correct href to /register', () => {
      render(
        <BrowserRouter>
          <Navbar />
        </BrowserRouter>
      );

      // Use aria-label to find the primary sign up link
      const signUpLink = screen.getByLabelText(/go to registration page/i);
      expect(signUpLink).toHaveAttribute('href', '/register');
    });
  });

  // Test Case 5 (partial): Logo/brand name is visible and clickable
  describe('Logo/brand navigation', () => {
    it('renders logo/brand name link visible in navigation', () => {
      render(
        <BrowserRouter>
          <Navbar />
        </BrowserRouter>
      );

      const logoLink = screen.getByRole('link', { name: /go to homepage/i });
      expect(logoLink).toBeInTheDocument();
      expect(logoLink).toBeVisible();
      expect(logoLink).toHaveTextContent('URLShort');
    });

    it('Logo link has correct href to /', () => {
      render(
        <BrowserRouter>
          <Navbar />
        </BrowserRouter>
      );

      const logoLink = screen.getByRole('link', { name: /go to homepage/i });
      expect(logoLink).toHaveAttribute('href', '/');
    });
  });

  // Additional accessibility tests
  describe('Accessibility', () => {
    it('has proper navigation role', () => {
      render(
        <BrowserRouter>
          <Navbar />
        </BrowserRouter>
      );

      const nav = screen.getByRole('navigation');
      expect(nav).toBeInTheDocument();
    });

    it('has aria-label on navigation', () => {
      render(
        <BrowserRouter>
          <Navbar />
        </BrowserRouter>
      );

      const nav = screen.getByRole('navigation', { name: /main navigation/i });
      expect(nav).toBeInTheDocument();
    });

    it('Login and Sign Up buttons have appropriate aria-labels', () => {
      render(
        <BrowserRouter>
          <Navbar />
        </BrowserRouter>
      );

      expect(screen.getByLabelText(/go to login page/i)).toBeInTheDocument();
      expect(screen.getByLabelText(/go to registration page/i)).toBeInTheDocument();
    });
  });
});
