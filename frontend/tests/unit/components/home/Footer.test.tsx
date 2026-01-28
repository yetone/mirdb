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
import { render, screen, within } from '@testing-library/react';
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
  it('displays Home link that points to "/"', () => {
    renderWithRouter(<Footer />);

    const homeLink = screen.getByRole('link', { name: /home/i });
    expect(homeLink).toBeInTheDocument();
    expect(homeLink).toHaveAttribute('href', '/');
  });

  // Test Case 3: Login link is present and links to /login
  it('displays Login link that points to "/login"', () => {
    renderWithRouter(<Footer />);

    const loginLink = screen.getByRole('link', { name: /login/i });
    expect(loginLink).toBeInTheDocument();
    expect(loginLink).toHaveAttribute('href', '/login');
  });

  // Test Case 4: Register link is present and links to /register
  it('displays Register link that points to "/register"', () => {
    renderWithRouter(<Footer />);

    const registerLink = screen.getByRole('link', { name: /register/i });
    expect(registerLink).toBeInTheDocument();
    expect(registerLink).toHaveAttribute('href', '/register');
  });

  // Test Case 5: Click Login link navigates to /login
  it('Login link navigates to /login', async () => {
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
  it('Register link navigates to /register', async () => {
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
  it('displays copyright text with copyright symbol', () => {
    renderWithRouter(<Footer />);

    const copyrightElement = screen.getByTestId('copyright');
    expect(copyrightElement).toBeInTheDocument();
    expect(copyrightElement.textContent).toMatch(/©/);
    expect(copyrightElement.textContent).toMatch(/URL Shortener/i);
  });

  // Test Case 8: Copyright displays current year (2026)
  it('displays current year in copyright dynamically', () => {
    renderWithRouter(<Footer />);

    const currentYear = new Date().getFullYear().toString();
    const copyrightElement = screen.getByTestId('copyright');

    expect(copyrightElement.textContent).toContain(currentYear);
  });

  // Test Case 9: Footer has contentinfo role (via footer element or role attribute)
  it('uses semantic footer element with contentinfo role', () => {
    renderWithRouter(<Footer />);

    const footerElement = screen.getByRole('contentinfo');
    expect(footerElement).toBeInTheDocument();
    expect(footerElement.tagName.toLowerCase()).toBe('footer');
  });

  // Test Case 10: Footer content is horizontally centered
  it('has centered layout with flex items-center classes', () => {
    renderWithRouter(<Footer />);

    const footerElement = screen.getByRole('contentinfo');
    const innerContainer = footerElement.firstElementChild;

    // Check for centering classes
    expect(innerContainer?.className).toMatch(/mx-auto/);
    expect(innerContainer?.className).toMatch(/items-center/);
    expect(innerContainer?.className).toMatch(/flex/);
  });

  // Additional test for navigation accessibility
  it('has accessible navigation with aria-label', () => {
    renderWithRouter(<Footer />);

    const nav = screen.getByRole('navigation', { name: /footer navigation/i });
    expect(nav).toBeInTheDocument();
  });

  // Test navigation links are in a list for semantic structure
  it('navigation links are in a list for semantic structure', () => {
    renderWithRouter(<Footer />);

    const nav = screen.getByRole('navigation', { name: /footer navigation/i });
    const list = nav.querySelector('ul');
    expect(list).toBeInTheDocument();

    const listItems = nav.querySelectorAll('li');
    expect(listItems.length).toBe(3); // Home, Login, Register
  });

  // Test all three links are present in navigation
  it('contains all required navigation links', () => {
    renderWithRouter(<Footer />);

    const nav = screen.getByRole('navigation');
    const links = within(nav).getAllByRole('link');

    expect(links).toHaveLength(3);
    expect(links[0]).toHaveTextContent(/home/i);
    expect(links[1]).toHaveTextContent(/login/i);
    expect(links[2]).toHaveTextContent(/register/i);
  });

  // Test all links have hover transition classes
  it('all links have hover transition classes', () => {
    renderWithRouter(<Footer />);

    const links = screen.getAllByRole('link');
    links.forEach(link => {
      expect(link.className).toMatch(/transition/);
    });
  });
});
