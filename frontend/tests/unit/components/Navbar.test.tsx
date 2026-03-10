/**
 * Navbar Component Tests
 * Owner: Scenario 3 - Navigation Menu
 *
 * Unit tests for the Navbar component verifying:
 * - Logo/Brand presence
 * - Login link presence
 * - Register link presence
 * - ThemeToggle integration
 * - Navigation links point to correct routes
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import { Navbar } from '../../../src/components/Navbar';

// Wrapper component with Router context
function renderWithRouter(component: React.ReactNode) {
  return render(<BrowserRouter>{component}</BrowserRouter>);
}

describe('Navbar', () => {
  beforeEach(() => {
    // Reset localStorage before each test
    localStorage.clear();
  });

  it('renders the navbar container', () => {
    renderWithRouter(<Navbar />);
    const navbar = screen.getByTestId('navbar');
    expect(navbar).toBeInTheDocument();
    expect(navbar).toHaveAttribute('role', 'navigation');
  });

  it('renders the logo/brand link', () => {
    renderWithRouter(<Navbar />);
    const logo = screen.getByTestId('navbar-logo');
    expect(logo).toBeInTheDocument();
    expect(logo).toHaveTextContent('URL Shortener');
    expect(logo).toHaveAttribute('href', '/');
  });

  it('renders the Login link with correct href', () => {
    renderWithRouter(<Navbar />);
    const loginLink = screen.getByTestId('navbar-login');
    expect(loginLink).toBeInTheDocument();
    expect(loginLink).toHaveTextContent('Login');
    expect(loginLink).toHaveAttribute('href', '/login');
  });

  it('renders the Register link with correct href', () => {
    renderWithRouter(<Navbar />);
    const registerLink = screen.getByTestId('navbar-register');
    expect(registerLink).toBeInTheDocument();
    expect(registerLink).toHaveTextContent('Register');
    expect(registerLink).toHaveAttribute('href', '/register');
  });

  it('renders the ThemeToggle component', () => {
    renderWithRouter(<Navbar />);
    const themeToggle = screen.getByTestId('theme-toggle');
    expect(themeToggle).toBeInTheDocument();
    expect(themeToggle).toHaveAttribute('aria-label');
  });

  it('contains all required elements: Logo, Login, Register, and ThemeToggle', () => {
    renderWithRouter(<Navbar />);

    // Verify all elements exist
    expect(screen.getByTestId('navbar-logo')).toBeInTheDocument();
    expect(screen.getByTestId('navbar-login')).toBeInTheDocument();
    expect(screen.getByTestId('navbar-register')).toBeInTheDocument();
    expect(screen.getByTestId('theme-toggle')).toBeInTheDocument();
  });

  it('has sticky positioning class', () => {
    renderWithRouter(<Navbar />);
    const navbar = screen.getByTestId('navbar');
    expect(navbar).toHaveClass('sticky');
    expect(navbar).toHaveClass('top-0');
  });

  it('has proper accessibility attributes', () => {
    renderWithRouter(<Navbar />);
    const navbar = screen.getByTestId('navbar');
    expect(navbar).toHaveAttribute('aria-label', 'Main navigation');
  });
});
