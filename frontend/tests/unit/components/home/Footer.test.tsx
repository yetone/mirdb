/**
 * Footer Unit Tests
 * Owner: Scenario 4 - Footer Navigation
 *
 * Test coverage:
 * - Component renders without errors
 * - Home link present and navigates to /
 * - Login link present and navigates to /login
 * - Register link present and navigates to /register
 * - Navigation to /login works
 * - Navigation to /register works
 * - Copyright text is present
 * - Copyright displays current year (2026)
 * - Footer has contentinfo role
 * - Footer content is centered
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import userEvent from '@testing-library/user-event';
import { Footer } from '@/components/home/Footer';

// Helper to render with router
const renderWithRouter = (ui: React.ReactElement, { route = '/' } = {}) => {
  return render(
    <MemoryRouter initialEntries={[route]}>
      {ui}
    </MemoryRouter>
  );
};

describe('Footer', () => {
  // Test Case 1: Component renders without throwing errors
  it('renders without throwing errors', () => {
    expect(() => renderWithRouter(<Footer />)).not.toThrow();
  });

  // Test Case 2: Home link is present and links to /
  it('displays Home link that points to home route', () => {
    renderWithRouter(<Footer />);

    const homeLink = screen.getByRole('link', { name: /home/i });
    expect(homeLink).toBeInTheDocument();
    expect(homeLink).toHaveAttribute('href', '/');
  });

  // Test Case 3: Login link is present and links to /login
  it('displays Login link that points to /login', () => {
    renderWithRouter(<Footer />);

    const loginLink = screen.getByRole('link', { name: /login/i });
    expect(loginLink).toBeInTheDocument();
    expect(loginLink).toHaveAttribute('href', '/login');
  });

  // Test Case 4: Register link is present and links to /register
  it('displays Register link that points to /register', () => {
    renderWithRouter(<Footer />);

    const registerLink = screen.getByRole('link', { name: /register/i });
    expect(registerLink).toBeInTheDocument();
    expect(registerLink).toHaveAttribute('href', '/register');
  });

  // Test Case 5: Click Login link navigates to /login
  it('Login link navigates to /login when clicked', async () => {
    const user = userEvent.setup();

    render(
      <MemoryRouter initialEntries={['/']}>
        <Routes>
          <Route path="/" element={<Footer />} />
          <Route path="/login" element={<div data-testid="login-page">Login Page</div>} />
        </Routes>
      </MemoryRouter>
    );

    const loginLink = screen.getByRole('link', { name: /login/i });
    await user.click(loginLink);
    expect(screen.getByTestId('login-page')).toBeInTheDocument();
  });

  // Test Case 6: Click Register link navigates to /register
  it('Register link navigates to /register when clicked', async () => {
    const user = userEvent.setup();

    render(
      <MemoryRouter initialEntries={['/']}>
        <Routes>
          <Route path="/" element={<Footer />} />
          <Route path="/register" element={<div data-testid="register-page">Register Page</div>} />
        </Routes>
      </MemoryRouter>
    );

    const registerLink = screen.getByRole('link', { name: /register/i });
    await user.click(registerLink);
    expect(screen.getByTestId('register-page')).toBeInTheDocument();
  });

  // Test Case 7: Copyright text with symbol is present
  it('displays copyright symbol and text', () => {
    renderWithRouter(<Footer />);

    // Look for copyright symbol (© or &copy;) in the rendered text
    const copyrightText = screen.getByText(/©/);
    expect(copyrightText).toBeInTheDocument();
    expect(copyrightText.textContent).toMatch(/URL Shortener/i);
  });

  // Test Case 8: Copyright displays current year (2026)
  it('displays current year in copyright', () => {
    renderWithRouter(<Footer />);

    const currentYear = new Date().getFullYear().toString();
    const copyrightText = screen.getByText(new RegExp(currentYear));
    expect(copyrightText).toBeInTheDocument();
  });

  // Test Case 9: Footer has contentinfo role (via footer element or role attribute)
  it('has contentinfo role for accessibility', () => {
    renderWithRouter(<Footer />);

    const footer = screen.getByRole('contentinfo');
    expect(footer).toBeInTheDocument();
  });

  // Test Case 10: Footer content is horizontally centered
  it('has centered layout classes', () => {
    renderWithRouter(<Footer />);

    const footer = screen.getByRole('contentinfo');
    // Check for centering classes on the footer or its container
    const container = footer.querySelector('.mx-auto');
    expect(container).toBeInTheDocument();
    expect(container?.className).toMatch(/items-center/);
  });

  // Additional tests for accessibility and semantic structure
  it('has navigation landmark with accessible label', () => {
    renderWithRouter(<Footer />);

    const nav = screen.getByRole('navigation', { name: /footer navigation/i });
    expect(nav).toBeInTheDocument();
  });

  it('navigation links are in a list for semantic structure', () => {
    renderWithRouter(<Footer />);

    const nav = screen.getByRole('navigation', { name: /footer navigation/i });
    const list = nav.querySelector('ul');
    expect(list).toBeInTheDocument();

    const listItems = nav.querySelectorAll('li');
    expect(listItems.length).toBe(3); // Home, Login, Register
  });

  it('all links have hover transition classes', () => {
    renderWithRouter(<Footer />);

    const links = screen.getAllByRole('link');
    links.forEach(link => {
      expect(link.className).toMatch(/transition/);
    });
  });
});
