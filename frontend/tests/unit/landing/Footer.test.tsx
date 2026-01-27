/**
 * Unit Tests for Footer Component
 * Owner: Scenario 5 - Footer Section
 *
 * Tests:
 * 1. Footer element is present in the DOM
 * 2. Copyright notice with current year is present
 * 3. Login link to /login is present
 * 4. Register link to /register is present
 * 5. ThemeToggle component is rendered
 */
import { describe, it, expect } from 'vitest';
import { render, screen } from './setup';
import Footer from '../../../src/components/landing/Footer';

describe('Footer', () => {
  it('renders footer element in the DOM', () => {
    render(<Footer />);

    // Check that footer element is present with proper role
    const footer = screen.getByRole('contentinfo');
    expect(footer).toBeInTheDocument();
  });

  it('displays copyright notice with current year', () => {
    render(<Footer />);

    const currentYear = new Date().getFullYear();

    // Check for copyright symbol and current year
    const copyrightText = screen.getByText(new RegExp(`©\\s*${currentYear}`, 'i'));
    expect(copyrightText).toBeInTheDocument();
    expect(copyrightText).toHaveTextContent('URL Shortener');
  });

  it('contains a link to /login page', () => {
    render(<Footer />);

    // Find the Login link
    const loginLink = screen.getByRole('link', { name: /login/i });
    expect(loginLink).toBeInTheDocument();
    expect(loginLink).toHaveAttribute('href', '/login');
  });

  it('contains a link to /register page', () => {
    render(<Footer />);

    // Find the Register link
    const registerLink = screen.getByRole('link', { name: /register/i });
    expect(registerLink).toBeInTheDocument();
    expect(registerLink).toHaveAttribute('href', '/register');
  });

  it('renders ThemeToggle component', () => {
    render(<Footer />);

    // ThemeToggle renders a button with aria-label for switching themes
    const themeToggleButton = screen.getByRole('button', { name: /switch to (light|dark) mode/i });
    expect(themeToggleButton).toBeInTheDocument();
  });

  it('has proper footer navigation with aria-label', () => {
    render(<Footer />);

    // Check for footer navigation element
    const footerNav = screen.getByRole('navigation', { name: /footer navigation/i });
    expect(footerNav).toBeInTheDocument();
  });

  it('footer has appropriate styling classes', () => {
    render(<Footer />);

    const footer = screen.getByRole('contentinfo');
    expect(footer).toHaveClass('footer');
    expect(footer).toHaveClass('footer-center');
  });
});
