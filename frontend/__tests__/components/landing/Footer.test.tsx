/**
 * Footer Component Tests
 * Owner: Scenario 6 - Footer Section
 *
 * Test cases:
 * 1. Footer section is present at the bottom of the page
 * 2. Footer contains Home navigation link
 * 3. Footer contains Login navigation link
 * 4. Footer contains Register navigation link
 * 5. Footer contains branding/logo element
 * 6. Footer contains copyright notice
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '../../test-utils';
import { Footer } from '../../../src/components/landing/Footer';

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <div {...props}>{children}</div>
    ),
  },
}));

describe('Footer', () => {
  it('renders the footer section', () => {
    render(<Footer />);

    // Check that footer element is present
    const footer = screen.getByTestId('footer');
    expect(footer).toBeInTheDocument();
    expect(footer.tagName.toLowerCase()).toBe('footer');
  });

  it('contains Home navigation link', () => {
    render(<Footer />);

    // Check for Home link
    const homeLink = screen.getByTestId('footer-home-link');
    expect(homeLink).toBeInTheDocument();
    expect(homeLink).toHaveAttribute('href', '/');
    expect(homeLink).toHaveTextContent('Home');
  });

  it('contains Login navigation link', () => {
    render(<Footer />);

    // Check for Login link
    const loginLink = screen.getByTestId('footer-login-link');
    expect(loginLink).toBeInTheDocument();
    expect(loginLink).toHaveAttribute('href', '/login');
    expect(loginLink).toHaveTextContent('Login');
  });

  it('contains Register navigation link', () => {
    render(<Footer />);

    // Check for Register link
    const registerLink = screen.getByTestId('footer-register-link');
    expect(registerLink).toBeInTheDocument();
    expect(registerLink).toHaveAttribute('href', '/register');
    expect(registerLink).toHaveTextContent('Register');
  });

  it('contains branding/logo element', () => {
    render(<Footer />);

    // Check for branding/logo
    const logo = screen.getByTestId('footer-logo');
    expect(logo).toBeInTheDocument();
    expect(logo).toHaveTextContent(/linkshort/i);
  });

  it('contains copyright notice', () => {
    render(<Footer />);

    // Check for copyright notice
    const copyright = screen.getByTestId('footer-copyright');
    expect(copyright).toBeInTheDocument();
    expect(copyright).toHaveTextContent(/©/);
    expect(copyright).toHaveTextContent(/all rights reserved/i);
  });

  it('has proper semantic HTML structure with footer element', () => {
    render(<Footer />);

    // Check for footer role/element with aria-label
    const footer = screen.getByRole('contentinfo');
    expect(footer).toBeInTheDocument();
    expect(footer).toHaveAttribute('aria-label', 'Site footer');
  });

  it('has navigation element with proper aria-label', () => {
    render(<Footer />);

    // Check for navigation element
    const nav = screen.getByRole('navigation', { name: /footer navigation/i });
    expect(nav).toBeInTheDocument();
  });

  it('renders copyright with current year', () => {
    render(<Footer />);

    const currentYear = new Date().getFullYear().toString();
    const copyright = screen.getByTestId('footer-copyright');
    expect(copyright).toHaveTextContent(currentYear);
  });
});
