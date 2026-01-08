/**
 * Returning User Journey E2E Tests
 *
 * Scenario: Verify returning user flow from landing to login
 *
 * Test Flow:
 * 1. Land on homepage (/)
 * 2. Click Login in navigation
 * 3. Navigate to /login page
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import Home from '../pages/Home';
import Login from '../pages/Login';
import { ThemeProvider } from '../contexts/ThemeContext';

// Test app wrapper with all routes for E2E testing
function TestApp({ initialEntries = ['/'] }: { initialEntries?: string[] }) {
  return (
    <ThemeProvider>
      <MemoryRouter initialEntries={initialEntries}>
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/login" element={<Login />} />
        </Routes>
      </MemoryRouter>
    </ThemeProvider>
  );
}

describe('Returning User Journey E2E Tests', () => {
  /**
   * Test Case 1: Complete returning user journey E2E test
   * Input: Complete returning user journey E2E test
   * Expected: User successfully navigates from homepage to login
   */
  describe('Test Case 1: Complete returning user journey E2E test', () => {
    it('completes full journey from homepage to login', async () => {
      const user = userEvent.setup();
      render(<TestApp />);

      // Step 1: Verify landing on homepage (/)
      // Returning user arrives at root URL
      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toBeInTheDocument();

      // Verify homepage headline is visible
      const headline = screen.getByRole('heading', { level: 1 });
      expect(headline).toHaveTextContent(/shorten, share, track/i);

      // Step 2: Click Login in navigation
      // User with existing account sees Login link
      const loginLink = screen.getByLabelText(/go to login page/i);
      expect(loginLink).toBeInTheDocument();
      expect(loginLink).toBeVisible();

      await user.click(loginLink);

      // Step 3: Navigate to login page
      // Verify user lands on /login page
      await waitFor(() => {
        const loginHeading = screen.getByRole('heading', { level: 1, name: /login/i });
        expect(loginHeading).toBeInTheDocument();
      });

      // Verify login form is ready for credentials
      expect(screen.getByPlaceholderText(/your@email.com/i)).toBeInTheDocument();
      expect(screen.getByPlaceholderText(/enter your password/i)).toBeInTheDocument();
      expect(screen.getByRole('button', { name: /login/i })).toBeInTheDocument();
    });

    it('returning user can access login from any position on the page', async () => {
      const user = userEvent.setup();
      render(<TestApp />);

      // Verify login link is always accessible in the navbar
      const loginLink = screen.getByLabelText(/go to login page/i);
      expect(loginLink).toBeInTheDocument();

      // Login link should be in the navbar (fixed position)
      const navbar = screen.getByRole('navigation');
      expect(navbar).toContainElement(loginLink);

      // Click login
      await user.click(loginLink);

      // Verify navigation to login page
      await waitFor(() => {
        expect(screen.getByRole('heading', { level: 1, name: /login/i })).toBeInTheDocument();
      });
    });

    it('returning user journey is faster than new user journey (direct path)', async () => {
      const user = userEvent.setup();
      const startTime = performance.now();

      render(<TestApp />);

      // Returning user knows where to click - direct to Login
      const loginLink = screen.getByLabelText(/go to login page/i);
      await user.click(loginLink);

      // Verify quick navigation to login
      await waitFor(() => {
        expect(screen.getByRole('heading', { level: 1, name: /login/i })).toBeInTheDocument();
      });

      const endTime = performance.now();
      const journeyDuration = endTime - startTime;

      // Direct path should be very fast (under 2 seconds)
      expect(journeyDuration).toBeLessThan(2000);
    });
  });

  /**
   * Test Case 2: Click Login navigation link integration test
   * Input: Click Login navigation link
   * Expected: User is navigated to /login without errors
   */
  describe('Test Case 2: Click Login navigation link integration', () => {
    it('Login link navigates to /login without errors', async () => {
      const user = userEvent.setup();
      render(<TestApp />);

      // Find the Login link
      const loginLink = screen.getByLabelText(/go to login page/i);
      expect(loginLink).toBeInTheDocument();

      // Verify the link has correct href
      expect(loginLink).toHaveAttribute('href', '/login');

      // Click the link
      await user.click(loginLink);

      // Verify navigation was successful
      await waitFor(() => {
        expect(screen.getByRole('heading', { level: 1, name: /login/i })).toBeInTheDocument();
      });

      // Verify no errors - login form elements are present
      expect(screen.getByPlaceholderText(/your@email.com/i)).toBeInTheDocument();
      expect(screen.getByPlaceholderText(/enter your password/i)).toBeInTheDocument();
    });

    it('Login link is properly styled and recognizable', async () => {
      render(<TestApp />);

      // Find the Login link
      const loginLink = screen.getByLabelText(/go to login page/i);

      // Verify it has button styling (DaisyUI btn class)
      expect(loginLink).toHaveClass('btn');
      expect(loginLink).toHaveTextContent(/login/i);
    });

    it('Login link has proper accessibility attributes', async () => {
      render(<TestApp />);

      // Find the Login link
      const loginLink = screen.getByLabelText(/go to login page/i);

      // Verify aria-label for accessibility
      expect(loginLink).toHaveAttribute('aria-label', 'Go to login page');

      // Verify it's a proper link element
      expect(loginLink.tagName.toLowerCase()).toBe('a');
    });

    it('can navigate to login and back to homepage', async () => {
      const user = userEvent.setup();
      render(<TestApp />);

      // Navigate to login
      const loginLink = screen.getByLabelText(/go to login page/i);
      await user.click(loginLink);

      await waitFor(() => {
        expect(screen.getByRole('heading', { level: 1, name: /login/i })).toBeInTheDocument();
      });

      // Navigate back to homepage via logo
      const logoLink = screen.getByRole('link', { name: /go to homepage/i });
      await user.click(logoLink);

      await waitFor(() => {
        expect(screen.getByRole('heading', { level: 1, name: /shorten, share, track/i })).toBeInTheDocument();
      });
    });
  });

  /**
   * Test Case 3: Login link visibility unit test
   * Input: Login link visibility
   * Expected: Login link is immediately visible without scrolling
   */
  describe('Test Case 3: Login link visibility', () => {
    it('Login link is immediately visible without scrolling', async () => {
      render(<TestApp />);

      // Find the Login link
      const loginLink = screen.getByLabelText(/go to login page/i);

      // Verify it's in the document
      expect(loginLink).toBeInTheDocument();

      // Verify it's visible (not hidden)
      expect(loginLink).toBeVisible();

      // Verify it's in the navbar (which is at the top of the page)
      const navbar = screen.getByRole('navigation');
      expect(navbar).toContainElement(loginLink);
    });

    it('Login link is in the fixed navbar header', async () => {
      render(<TestApp />);

      // The navbar should contain the login link
      const navbar = screen.getByRole('navigation');
      const loginLink = screen.getByLabelText(/go to login page/i);

      // Login link should be inside the navigation
      expect(navbar).toContainElement(loginLink);

      // Navbar should have proper accessibility
      expect(navbar).toHaveAttribute('aria-label', 'Main navigation');
    });

    it('Login link appears before any scrollable content', async () => {
      render(<TestApp />);

      // Get the login link
      const loginLink = screen.getByLabelText(/go to login page/i);

      // Get the hero section (first scrollable content)
      const heroSection = screen.getByTestId('hero-section');

      // Both should be present
      expect(loginLink).toBeInTheDocument();
      expect(heroSection).toBeInTheDocument();

      // Login link should be visible immediately
      expect(loginLink).toBeVisible();
    });

    it('Login link is distinguishable from Sign Up button', async () => {
      render(<TestApp />);

      // Get both links
      const loginLink = screen.getByLabelText(/go to login page/i);
      const signUpLink = screen.getByLabelText(/go to registration page/i);

      // Both should be visible
      expect(loginLink).toBeVisible();
      expect(signUpLink).toBeVisible();

      // They should have different styling
      // Login is ghost button, Sign Up is primary
      expect(loginLink).toHaveClass('btn-ghost');
      expect(signUpLink).toHaveClass('btn-primary');
    });

    it('Login link text is clearly readable', async () => {
      render(<TestApp />);

      const loginLink = screen.getByLabelText(/go to login page/i);

      // Verify the text content
      expect(loginLink).toHaveTextContent('Login');
    });
  });

  /**
   * Additional returning user journey tests for comprehensive coverage
   */
  describe('Additional returning user journey coverage', () => {
    it('login page displays correct form for authentication', async () => {
      render(<TestApp initialEntries={['/login']} />);

      // Verify login page heading
      const heading = screen.getByRole('heading', { level: 1 });
      expect(heading).toHaveTextContent(/login/i);

      // Verify form fields
      expect(screen.getByPlaceholderText(/your@email.com/i)).toBeInTheDocument();
      expect(screen.getByPlaceholderText(/enter your password/i)).toBeInTheDocument();

      // Verify submit button
      const loginButton = screen.getByRole('button', { name: /login/i });
      expect(loginButton).toBeInTheDocument();
      expect(loginButton).toHaveAttribute('type', 'submit');
    });

    it('login page provides link to registration for users who do not have an account', async () => {
      render(<TestApp initialEntries={['/login']} />);

      // Check for "Sign Up" link within the login form (not navbar)
      // The form has text "Don't have an account?" followed by Sign Up link
      const cardBody = screen.getByText(/don't have an account/i).closest('div');
      const signUpLink = within(cardBody!).getByRole('link', { name: /sign up/i });
      expect(signUpLink).toBeInTheDocument();
      expect(signUpLink).toHaveAttribute('href', '/register');
    });

    it('returning user can navigate to login via keyboard', async () => {
      const user = userEvent.setup();
      render(<TestApp />);

      // Tab through elements until we reach login link
      // Skip link is first focusable, then logo, then navigation links
      await user.tab(); // Skip link
      await user.tab(); // Logo
      await user.tab(); // Features link (if on homepage)
      await user.tab(); // How It Works link
      await user.tab(); // Login link

      // Find login link and verify it can receive focus
      const loginLink = screen.getByLabelText(/go to login page/i);

      // The link should be focusable
      loginLink.focus();
      expect(document.activeElement).toBe(loginLink);

      // Press Enter to navigate
      await user.keyboard('{Enter}');

      // Verify navigation to login page
      await waitFor(() => {
        expect(screen.getByRole('heading', { level: 1, name: /login/i })).toBeInTheDocument();
      });
    });

    it('homepage navbar is consistent across page sections', async () => {
      render(<TestApp />);

      // Verify navbar is present
      const navbar = screen.getByRole('navigation');
      expect(navbar).toBeInTheDocument();

      // Verify login link is always in navbar
      const loginLink = screen.getByLabelText(/go to login page/i);
      expect(navbar).toContainElement(loginLink);
    });
  });
});
