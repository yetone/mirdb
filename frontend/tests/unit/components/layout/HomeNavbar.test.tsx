/**
 * Unit Tests for HomeNavbar Component.
 * Owner: Scenario 3 - Navigation Bar Functionality
 *
 * Tests:
 * - Navbar renders with all required elements
 * - Logo is rendered and left-aligned
 * - Theme toggle is present in center
 * - Login and Register links are visible on the right
 * - Navbar has exactly 60px height
 */
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import { ThemeProvider } from '../../../../src/contexts/ThemeContext';
import { HomeNavbar } from '../../../../src/components/layout/HomeNavbar';

function renderWithProviders(ui: React.ReactElement) {
  return render(
    <BrowserRouter>
      <ThemeProvider>
        {ui}
      </ThemeProvider>
    </BrowserRouter>
  );
}

describe('HomeNavbar', () => {
  // Test Case 1: Render Navbar component with all elements
  it('renders with logo, theme toggle, Login and Register links', () => {
    renderWithProviders(<HomeNavbar />);

    // Check navbar is rendered
    const navbar = screen.getByTestId('home-navbar');
    expect(navbar).toBeInTheDocument();

    // Check logo is present
    const logo = screen.getByTestId('navbar-logo');
    expect(logo).toBeInTheDocument();
    expect(logo).toHaveTextContent('URL Shortener');

    // Check theme toggle is present
    const themeToggle = screen.getByTestId('theme-toggle');
    expect(themeToggle).toBeInTheDocument();

    // Check Login link is present
    const loginLink = screen.getByTestId('navbar-login-link');
    expect(loginLink).toBeInTheDocument();
    expect(loginLink).toHaveTextContent('Login');

    // Check Register link is present
    const registerLink = screen.getByTestId('navbar-register-link');
    expect(registerLink).toBeInTheDocument();
    expect(registerLink).toHaveTextContent('Register');
  });

  // Test Case 4: Measure navbar height
  it('has exactly 60px height', () => {
    renderWithProviders(<HomeNavbar />);

    const navbar = screen.getByTestId('home-navbar');
    expect(navbar).toHaveClass('h-[60px]');
    expect(navbar).toHaveClass('min-h-[60px]');
    expect(navbar).toHaveClass('max-h-[60px]');
  });

  // Test Case 5: Check logo presence and left-alignment
  it('renders logo/brand name that is left-aligned', () => {
    renderWithProviders(<HomeNavbar />);

    const logo = screen.getByTestId('navbar-logo');
    expect(logo).toBeInTheDocument();
    expect(logo).toHaveTextContent('URL Shortener');

    // Logo should be within navbar-start for left alignment
    const navbarStart = logo.closest('.navbar-start');
    expect(navbarStart).toBeInTheDocument();
  });

  it('has theme toggle in the center section', () => {
    renderWithProviders(<HomeNavbar />);

    const themeToggle = screen.getByTestId('theme-toggle');
    expect(themeToggle).toBeInTheDocument();

    // Theme toggle should be within navbar-center
    const navbarCenter = themeToggle.closest('.navbar-center');
    expect(navbarCenter).toBeInTheDocument();
  });

  it('has Login and Register links in the right section', () => {
    renderWithProviders(<HomeNavbar />);

    const loginLink = screen.getByTestId('navbar-login-link');
    const registerLink = screen.getByTestId('navbar-register-link');

    // Both links should be within navbar-end for right alignment
    const navbarEnd = loginLink.closest('.navbar-end');
    expect(navbarEnd).toBeInTheDocument();
    expect(navbarEnd).toContainElement(registerLink);
  });

  it('has correct links for Login and Register', () => {
    renderWithProviders(<HomeNavbar />);

    const loginLink = screen.getByTestId('navbar-login-link');
    const registerLink = screen.getByTestId('navbar-register-link');

    expect(loginLink).toHaveAttribute('href', '/login');
    expect(registerLink).toHaveAttribute('href', '/register');
  });

  it('has proper accessibility attributes', () => {
    renderWithProviders(<HomeNavbar />);

    const navbar = screen.getByRole('navigation');
    expect(navbar).toHaveAttribute('aria-label', 'Main navigation');

    const logo = screen.getByTestId('navbar-logo');
    expect(logo).toHaveAttribute('aria-label', 'URL Shortener Home');
  });

  it('renders the Link2 icon in the logo', () => {
    renderWithProviders(<HomeNavbar />);

    const logo = screen.getByTestId('navbar-logo');
    const svg = logo.querySelector('svg');
    expect(svg).toBeInTheDocument();
    expect(svg).toHaveAttribute('aria-hidden', 'true');
  });
});
