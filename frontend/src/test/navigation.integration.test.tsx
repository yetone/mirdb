import { describe, it, expect } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import Home from '../pages/Home';
import Login from '../pages/Login';
import Register from '../pages/Register';

// Test app wrapper with all routes for integration testing
function TestApp({ initialEntries = ['/'] }: { initialEntries?: string[] }) {
  return (
    <MemoryRouter initialEntries={initialEntries}>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/login" element={<Login />} />
        <Route path="/register" element={<Register />} />
      </Routes>
    </MemoryRouter>
  );
}

describe('Navigation Links Integration Tests', () => {
  // Test Case 3: Click Login navigation link - User is navigated to /login route
  describe('Login navigation', () => {
    it('navigates to /login when Login link is clicked', async () => {
      const user = userEvent.setup();

      render(<TestApp />);

      // Verify we're on the home page initially
      expect(screen.getByRole('heading', { level: 1, name: /shorten, share, track/i })).toBeInTheDocument();

      // Find and click the Login link using aria-label (unique to navbar link)
      const loginLink = screen.getByLabelText(/go to login page/i);
      await user.click(loginLink);

      // Verify we're now on the login page
      expect(screen.getByRole('heading', { level: 1, name: /login/i })).toBeInTheDocument();

      // Verify login form elements are present
      expect(screen.getByPlaceholderText(/email/i)).toBeInTheDocument();
      expect(screen.getByPlaceholderText(/password/i)).toBeInTheDocument();
    });
  });

  // Test Case 4: Click Register navigation link - User is navigated to /register route
  describe('Register navigation', () => {
    it('navigates to /register when Sign Up link is clicked', async () => {
      const user = userEvent.setup();

      render(<TestApp />);

      // Verify we're on the home page initially
      expect(screen.getByRole('heading', { level: 1, name: /shorten, share, track/i })).toBeInTheDocument();

      // Find and click the Sign Up link using aria-label
      const signUpLink = screen.getByLabelText(/go to registration page/i);
      await user.click(signUpLink);

      // Verify we're now on the register page
      expect(screen.getByRole('heading', { level: 1, name: /create account/i })).toBeInTheDocument();

      // Verify registration form elements are present
      expect(screen.getByPlaceholderText(/john doe/i)).toBeInTheDocument();
    });
  });

  // Test Case 5: Click logo/brand name - User is navigated to / (homepage) or page scrolls to top
  describe('Logo/brand navigation', () => {
    it('navigates to homepage when logo is clicked from login page', async () => {
      const user = userEvent.setup();

      render(<TestApp initialEntries={['/login']} />);

      // Verify we're on the login page initially
      expect(screen.getByRole('heading', { level: 1, name: /login/i })).toBeInTheDocument();

      // Find and click the logo
      const logoLink = screen.getByRole('link', { name: /go to homepage/i });
      await user.click(logoLink);

      // Verify we're now on the home page
      expect(screen.getByRole('heading', { level: 1, name: /shorten, share, track/i })).toBeInTheDocument();
    });

    it('navigates to homepage when logo is clicked from register page', async () => {
      const user = userEvent.setup();

      render(<TestApp initialEntries={['/register']} />);

      // Verify we're on the register page initially
      expect(screen.getByRole('heading', { level: 1, name: /create account/i })).toBeInTheDocument();

      // Find and click the logo
      const logoLink = screen.getByRole('link', { name: /go to homepage/i });
      await user.click(logoLink);

      // Verify we're now on the home page
      expect(screen.getByRole('heading', { level: 1, name: /shorten, share, track/i })).toBeInTheDocument();
    });

    it('logo on homepage links to / (root)', async () => {
      render(<TestApp />);

      const logoLink = screen.getByRole('link', { name: /go to homepage/i });
      expect(logoLink).toHaveAttribute('href', '/');
    });
  });

  // Additional integration tests for navigation flow
  describe('Navigation flow between pages', () => {
    it('can navigate from home to login and back to home via logo', async () => {
      const user = userEvent.setup();

      render(<TestApp />);

      // Start on home
      expect(screen.getByRole('heading', { level: 1, name: /shorten, share, track/i })).toBeInTheDocument();

      // Navigate to login using aria-label
      await user.click(screen.getByLabelText(/go to login page/i));
      expect(screen.getByRole('heading', { level: 1, name: /login/i })).toBeInTheDocument();

      // Navigate back to home via logo
      await user.click(screen.getByRole('link', { name: /go to homepage/i }));
      expect(screen.getByRole('heading', { level: 1, name: /shorten, share, track/i })).toBeInTheDocument();
    });

    it('can navigate from home to register and back to home via logo', async () => {
      const user = userEvent.setup();

      render(<TestApp />);

      // Start on home
      expect(screen.getByRole('heading', { level: 1, name: /shorten, share, track/i })).toBeInTheDocument();

      // Navigate to register using aria-label
      await user.click(screen.getByLabelText(/go to registration page/i));
      expect(screen.getByRole('heading', { level: 1, name: /create account/i })).toBeInTheDocument();

      // Navigate back to home via logo
      await user.click(screen.getByRole('link', { name: /go to homepage/i }));
      expect(screen.getByRole('heading', { level: 1, name: /shorten, share, track/i })).toBeInTheDocument();
    });

    it('can navigate from login to register via Sign Up link in form', async () => {
      const user = userEvent.setup();

      render(<TestApp initialEntries={['/login']} />);

      // Start on login
      expect(screen.getByRole('heading', { level: 1, name: /login/i })).toBeInTheDocument();

      // Navigate to register via "Sign Up" link in the login form (use link within the card body)
      const cardBody = screen.getByText(/don't have an account/i).closest('div');
      const signUpLink = within(cardBody!).getByRole('link', { name: /sign up/i });
      await user.click(signUpLink);

      expect(screen.getByRole('heading', { level: 1, name: /create account/i })).toBeInTheDocument();
    });

    it('can navigate from register to login via Login link in form', async () => {
      const user = userEvent.setup();

      render(<TestApp initialEntries={['/register']} />);

      // Start on register
      expect(screen.getByRole('heading', { level: 1, name: /create account/i })).toBeInTheDocument();

      // Navigate to login via "Login" link in the form (use link within the card body)
      const cardBody = screen.getByText(/already have an account/i).closest('div');
      const loginLink = within(cardBody!).getByRole('link', { name: /login/i });
      await user.click(loginLink);

      expect(screen.getByRole('heading', { level: 1, name: /login/i })).toBeInTheDocument();
    });
  });
});
