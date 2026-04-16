/**
 * Integration Tests for Homepage Navigation.
 * Owner: Scenario 1 - Homepage Basic Display and Navigation
 *
 * Tests the homepage integration with Navbar, ThemeToggle, and BackgroundEffect components.
 */

import { describe, it, expect } from 'vitest';
import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { renderWithProviders } from '../../unit/homepage/setup';
import App from '../../../src/App';

describe('Homepage Basic Display and Navigation', () => {
  describe('Homepage Rendering', () => {
    it('renders homepage without errors at "/" route', async () => {
      renderWithProviders(<App />);

      // The homepage should render with the HeroSection content
      await waitFor(() => {
        expect(document.querySelector('main')).toBeInTheDocument();
      });
    });
  });

  describe('Navbar Integration', () => {
    it('displays Navbar with Logo', async () => {
      renderWithProviders(<App />);

      // Check for the logo link
      const logoLink = screen.getByRole('link', { name: /shorturl/i });
      expect(logoLink).toBeInTheDocument();
      expect(logoLink).toHaveAttribute('href', '/');
    });

    it('displays Login link in navbar for unauthenticated users', async () => {
      renderWithProviders(<App />);

      const loginLink = screen.getByRole('link', { name: /login/i });
      expect(loginLink).toBeInTheDocument();
      expect(loginLink).toHaveAttribute('href', '/login');
    });

    it('displays Register link in navbar for unauthenticated users', async () => {
      renderWithProviders(<App />);

      const registerLink = screen.getByRole('link', { name: /register/i });
      expect(registerLink).toBeInTheDocument();
      expect(registerLink).toHaveAttribute('href', '/register');
    });
  });

  describe('ThemeToggle Integration', () => {
    it('displays ThemeToggle component in navbar', async () => {
      renderWithProviders(<App />);

      // ThemeToggle is a select with aria-label "Select theme"
      const themeToggle = screen.getByRole('combobox', { name: /select theme/i });
      expect(themeToggle).toBeInTheDocument();
    });

    it('ThemeToggle contains theme options', async () => {
      renderWithProviders(<App />);

      const themeToggle = screen.getByRole('combobox', { name: /select theme/i });
      const options = themeToggle.querySelectorAll('option');

      expect(options.length).toBeGreaterThanOrEqual(4);
      expect(screen.getByRole('option', { name: /light/i })).toBeInTheDocument();
      expect(screen.getByRole('option', { name: /dark/i })).toBeInTheDocument();
    });
  });

  describe('BackgroundEffect Integration', () => {
    it('renders BackgroundEffect visual elements', async () => {
      renderWithProviders(<App />);

      // BackgroundEffect uses a fixed positioned div with specific classes
      const backgroundContainer = document.querySelector('.fixed.inset-0.-z-10');
      expect(backgroundContainer).toBeInTheDocument();
    });
  });

  describe('Navigation Functionality', () => {
    it('navigates to /login route when Login link is clicked', async () => {
      const user = userEvent.setup();
      renderWithProviders(<App />);

      const loginLink = screen.getByRole('link', { name: /login/i });
      await user.click(loginLink);

      // After clicking, the Login page should render
      await waitFor(() => {
        expect(window.location.pathname).toBe('/login');
      });
    });

    it('navigates to /register route when Register link is clicked', async () => {
      const user = userEvent.setup();
      renderWithProviders(<App />);

      const registerLink = screen.getByRole('link', { name: /register/i });
      await user.click(registerLink);

      // After clicking, the Register page should render
      await waitFor(() => {
        expect(window.location.pathname).toBe('/register');
      });
    });
  });
});
