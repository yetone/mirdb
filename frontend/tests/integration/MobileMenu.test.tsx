/**
 * Integration Tests for MobileMenu.
 * Owner: Scenario 9 - Mobile Responsive Design
 *
 * Tests:
 * - Mobile menu integration with navigation
 * - Theme switching within mobile menu
 * - Navigation links work correctly
 * - Menu state management across interactions
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, within, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { BrowserRouter, MemoryRouter, Route, Routes } from 'react-router-dom';
import { ThemeProvider } from '../../src/contexts/ThemeContext';
import { AuthProvider } from '../../src/contexts/AuthContext';
import { HomeNavbar } from '../../src/components/layout/HomeNavbar';

function renderWithProviders(ui: React.ReactElement) {
  return render(
    <BrowserRouter>
      <ThemeProvider>
        <AuthProvider>
          {ui}
        </AuthProvider>
      </ThemeProvider>
    </BrowserRouter>
  );
}

function renderWithRoutes() {
  return render(
    <MemoryRouter initialEntries={['/']}>
      <ThemeProvider>
        <AuthProvider>
          <Routes>
            <Route path="/" element={<HomeNavbar />} />
            <Route path="/login" element={<div data-testid="login-page">Login Page</div>} />
            <Route path="/register" element={<div data-testid="register-page">Register Page</div>} />
          </Routes>
        </AuthProvider>
      </ThemeProvider>
    </MemoryRouter>
  );
}

describe('MobileMenu Integration', () => {
  beforeEach(() => {
    document.body.style.overflow = '';
  });

  afterEach(() => {
    document.body.style.overflow = '';
  });

  describe('hamburger menu interaction', () => {
    it('opens mobile menu when hamburger button is clicked', async () => {
      const user = userEvent.setup();
      renderWithProviders(<HomeNavbar />);

      // Initially the menu should not be rendered (null when closed)
      expect(screen.queryByTestId('mobile-menu')).not.toBeInTheDocument();

      // Click the hamburger button
      const hamburgerButton = screen.getByTestId('hamburger-button');
      await user.click(hamburgerButton);

      // Menu should now be visible
      const menu = screen.getByTestId('mobile-menu');
      expect(menu).toBeInTheDocument();
      expect(menu).toHaveAttribute('aria-hidden', 'false');
    });

    it('closes mobile menu when close button is clicked', async () => {
      const user = userEvent.setup();
      renderWithProviders(<HomeNavbar />);

      // Open the menu
      const hamburgerButton = screen.getByTestId('hamburger-button');
      await user.click(hamburgerButton);

      // Verify it's open
      expect(screen.getByTestId('mobile-menu')).toBeInTheDocument();

      // Click the close button
      const closeButton = screen.getByTestId('mobile-menu-close');
      await user.click(closeButton);

      // Menu should be closed (not in DOM)
      expect(screen.queryByTestId('mobile-menu')).not.toBeInTheDocument();
    });

    it('closes mobile menu when backdrop is clicked', async () => {
      const user = userEvent.setup();
      renderWithProviders(<HomeNavbar />);

      // Open the menu
      const hamburgerButton = screen.getByTestId('hamburger-button');
      await user.click(hamburgerButton);

      // Click the backdrop
      const backdrop = screen.getByTestId('mobile-menu-backdrop');
      await user.click(backdrop);

      // Menu should be closed (not in DOM)
      expect(screen.queryByTestId('mobile-menu')).not.toBeInTheDocument();
    });

    it('closes mobile menu when Escape key is pressed', async () => {
      const user = userEvent.setup();
      renderWithProviders(<HomeNavbar />);

      // Open the menu
      const hamburgerButton = screen.getByTestId('hamburger-button');
      await user.click(hamburgerButton);

      // Press Escape
      fireEvent.keyDown(document, { key: 'Escape' });

      // Menu should be closed (not in DOM)
      expect(screen.queryByTestId('mobile-menu')).not.toBeInTheDocument();
    });
  });

  describe('navigation from mobile menu', () => {
    it('navigates to login page when Login link is clicked', async () => {
      const user = userEvent.setup();
      renderWithRoutes();

      // Open the menu
      const hamburgerButton = screen.getByTestId('hamburger-button');
      await user.click(hamburgerButton);

      // Click Login link
      const loginLink = screen.getByTestId('mobile-menu-login');
      await user.click(loginLink);

      // Should navigate to login page
      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument();
      });
    });

    it('navigates to register page when Register link is clicked', async () => {
      const user = userEvent.setup();
      renderWithRoutes();

      // Open the menu
      const hamburgerButton = screen.getByTestId('hamburger-button');
      await user.click(hamburgerButton);

      // Click Register link
      const registerLink = screen.getByTestId('mobile-menu-register');
      await user.click(registerLink);

      // Should navigate to register page
      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument();
      });
    });

    it('closes menu after navigation link is clicked', async () => {
      const user = userEvent.setup();
      renderWithProviders(<HomeNavbar />);

      // Open the menu
      const hamburgerButton = screen.getByTestId('hamburger-button');
      await user.click(hamburgerButton);

      // Click Login link
      const loginLink = screen.getByTestId('mobile-menu-login');
      await user.click(loginLink);

      // Menu should be closed (not in DOM)
      expect(screen.queryByTestId('mobile-menu')).not.toBeInTheDocument();
    });
  });

  describe('theme switching in mobile menu', () => {
    it('allows theme toggle from within mobile menu', async () => {
      const user = userEvent.setup();
      renderWithProviders(<HomeNavbar />);

      // Open the menu
      const hamburgerButton = screen.getByTestId('hamburger-button');
      await user.click(hamburgerButton);

      // Find theme toggle in mobile menu
      const mobileMenu = screen.getByTestId('mobile-menu');
      const themeToggle = within(mobileMenu).getByTestId('theme-toggle');
      expect(themeToggle).toBeInTheDocument();

      // Click theme toggle - it should work without errors
      await user.click(themeToggle);
    });
  });

  describe('body scroll management', () => {
    it('prevents body scroll when menu is open', async () => {
      const user = userEvent.setup();
      renderWithProviders(<HomeNavbar />);

      // Open the menu
      const hamburgerButton = screen.getByTestId('hamburger-button');
      await user.click(hamburgerButton);

      expect(document.body.style.overflow).toBe('hidden');
    });

    it('restores body scroll when menu is closed', async () => {
      const user = userEvent.setup();
      renderWithProviders(<HomeNavbar />);

      // Open the menu
      const hamburgerButton = screen.getByTestId('hamburger-button');
      await user.click(hamburgerButton);

      // Close the menu
      const closeButton = screen.getByTestId('mobile-menu-close');
      await user.click(closeButton);

      expect(document.body.style.overflow).toBe('');
    });
  });

  describe('menu state persistence', () => {
    it('maintains closed state after multiple open/close cycles', async () => {
      const user = userEvent.setup();
      renderWithProviders(<HomeNavbar />);

      const hamburgerButton = screen.getByTestId('hamburger-button');

      // Cycle 1: Open and close
      await user.click(hamburgerButton);
      expect(screen.getByTestId('mobile-menu')).toBeInTheDocument();
      await user.click(screen.getByTestId('mobile-menu-close'));
      expect(screen.queryByTestId('mobile-menu')).not.toBeInTheDocument();

      // Cycle 2: Open and close via Escape
      await user.click(hamburgerButton);
      expect(screen.getByTestId('mobile-menu')).toBeInTheDocument();
      fireEvent.keyDown(document, { key: 'Escape' });
      expect(screen.queryByTestId('mobile-menu')).not.toBeInTheDocument();

      // Cycle 3: Open and close via backdrop
      await user.click(hamburgerButton);
      expect(screen.getByTestId('mobile-menu')).toBeInTheDocument();
      await user.click(screen.getByTestId('mobile-menu-backdrop'));
      expect(screen.queryByTestId('mobile-menu')).not.toBeInTheDocument();
    });
  });
});
