/**
 * Homepage Integration Tests
 * Owner: Scenarios 2, 3, 12, 14, 15, 16, 17
 *
 * Tests for the main Home page component, covering:
 * - Navigation to registration (Scenario 2)
 * - Navigation to login (Scenario 3)
 * - Navbar integration (Scenario 12)
 * - Animation presence (Scenario 14)
 * - Component reuse (Scenario 15)
 * - Route configuration (Scenario 16)
 * - Error handling (Scenario 17)
 */
import { describe, it, expect } from 'vitest';
import { screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { renderWithProviders } from './testUtils';
import Home from '../../src/pages/Home';
import Navbar from '../../src/components/Navbar';

/**
 * Scenario 3: Navigation to Login Page
 *
 * Test that returning users can navigate from homepage to login page.
 *
 * Steps:
 * 1. Navigate to homepage
 * 2. Locate sign in option
 * 3. Click sign in button
 * 4. Verify navigation to login
 */
describe('Scenario 3: Navigation to Login Page', () => {
  describe('Test Case 1: Click Sign In/Login button or link navigates to /login route', () => {
    it('navigates to /login when Sign In button in hero section is clicked', async () => {
      const user = userEvent.setup();
      renderWithProviders(<Home />);

      // Find Sign In link in hero section (button within link)
      const signInLinks = screen.getAllByRole('link', { name: /Sign In/i });
      expect(signInLinks.length).toBeGreaterThan(0);

      // Click the Sign In link
      await user.click(signInLinks[0]);

      // The link should have href="/login"
      expect(signInLinks[0]).toHaveAttribute('href', '/login');
    });

    it('navigates to /login when Sign In link in navbar is clicked', async () => {
      const user = userEvent.setup();
      renderWithProviders(<Home />);

      // Find the navbar Sign In link - it's the first link with "Sign In" (navbar comes before hero)
      const signInLinks = screen.getAllByRole('link', { name: /Sign In/i });
      // First link is from navbar (btn btn-ghost class), second is from hero section
      const navbarSignInLink = signInLinks.find(link => link.classList.contains('btn-ghost'));
      expect(navbarSignInLink).toBeDefined();
      expect(navbarSignInLink).toBeInTheDocument();

      await user.click(navbarSignInLink!);

      // Verify navigation target
      expect(navbarSignInLink).toHaveAttribute('href', '/login');
    });
  });

  describe('Test Case 2: Login link/button has href=/login or onClick navigates to /login', () => {
    it('hero section Sign In link has correct href attribute', () => {
      renderWithProviders(<Home />);

      // Find all Sign In links
      const signInLinks = screen.getAllByRole('link', { name: /Sign In/i });

      // All Sign In links should have href="/login"
      signInLinks.forEach(link => {
        expect(link).toHaveAttribute('href', '/login');
      });
    });

    it('Sign In button is wrapped in a link with correct href', () => {
      renderWithProviders(<Home />);

      // Find the Sign In button
      const signInButton = screen.getByRole('button', { name: /Sign In/i });
      expect(signInButton).toBeInTheDocument();

      // The button should be inside a link with href="/login"
      const parentLink = signInButton.closest('a');
      expect(parentLink).toHaveAttribute('href', '/login');
    });
  });

  describe('Test Case 3: Navbar contains accessible login navigation', () => {
    it('Navbar renders Sign In link for unauthenticated users', () => {
      renderWithProviders(<Navbar />);

      const signInLink = screen.getByRole('link', { name: /Sign In/i });
      expect(signInLink).toBeInTheDocument();
      expect(signInLink).toHaveAttribute('href', '/login');
    });

    it('Navbar Sign In link is accessible via keyboard', async () => {
      renderWithProviders(<Navbar />);

      const signInLink = screen.getByRole('link', { name: /Sign In/i });

      // Link should be focusable
      signInLink.focus();
      expect(document.activeElement).toBe(signInLink);
    });

    it('Navbar Sign In link has visible text', () => {
      renderWithProviders(<Navbar />);

      const signInLink = screen.getByRole('link', { name: /Sign In/i });
      expect(signInLink).toHaveTextContent('Sign In');
    });

    it('Home page includes Navbar with login navigation', () => {
      renderWithProviders(<Home />);

      // Navbar should be present with navigation
      const nav = screen.getByRole('navigation');
      expect(nav).toBeInTheDocument();

      // Sign In links should be accessible within the page (one in navbar, one in hero)
      const signInLinks = screen.getAllByRole('link', { name: /Sign In/i });
      expect(signInLinks.length).toBeGreaterThan(0);

      // All should have correct href
      signInLinks.forEach(link => {
        expect(link).toHaveAttribute('href', '/login');
      });
    });
  });
});
