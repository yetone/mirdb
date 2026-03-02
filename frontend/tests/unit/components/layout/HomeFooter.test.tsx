/**
 * Unit Tests for HomeFooter Component.
 * Owner: Scenario 8 - Footer Section
 *
 * Tests:
 * - Footer renders with logo, copyright, and navigation links
 * - Footer has minimum height of 150px
 * - Login and Register links navigate correctly
 * - Copyright notice displays current year
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { BrowserRouter, MemoryRouter } from 'react-router-dom';
import { HomeFooter } from '../../../../src/components/layout/HomeFooter';

function renderWithRouter(ui: React.ReactElement, { route = '/' } = {}) {
  return render(
    <MemoryRouter initialEntries={[route]}>
      {ui}
    </MemoryRouter>
  );
}

describe('HomeFooter', () => {
  // Test Case 1: Render Footer component with all elements
  describe('Render Footer component', () => {
    it('renders with logo, copyright, and navigation links', () => {
      renderWithRouter(<HomeFooter />);

      // Check footer is rendered
      const footer = screen.getByTestId('home-footer');
      expect(footer).toBeInTheDocument();

      // Check brand/logo is present
      const brand = screen.getByTestId('footer-brand');
      expect(brand).toBeInTheDocument();
      expect(brand).toHaveTextContent('URL Shortener');

      // Check copyright notice is present
      const copyright = screen.getByTestId('footer-copyright');
      expect(copyright).toBeInTheDocument();

      // Check Login link is present
      const loginLink = screen.getByTestId('footer-login-link');
      expect(loginLink).toBeInTheDocument();
      expect(loginLink).toHaveTextContent('Login');

      // Check Register link is present
      const registerLink = screen.getByTestId('footer-register-link');
      expect(registerLink).toBeInTheDocument();
      expect(registerLink).toHaveTextContent('Register');
    });
  });

  // Test Case 2: Check footer minimum height
  describe('Check footer minimum height', () => {
    it('has min-height of 150px', () => {
      renderWithRouter(<HomeFooter />);

      const footer = screen.getByTestId('home-footer');
      expect(footer).toHaveClass('min-h-[150px]');
    });
  });

  // Test Case 3: Click Login link in footer
  describe('Click Login link in footer', () => {
    it('has Login link that navigates to /login route', () => {
      renderWithRouter(<HomeFooter />);

      const loginLink = screen.getByTestId('footer-login-link');
      expect(loginLink).toHaveAttribute('href', '/login');
    });
  });

  // Test Case 4: Click Register link in footer
  describe('Click Register link in footer', () => {
    it('has Register link that navigates to /register route', () => {
      renderWithRouter(<HomeFooter />);

      const registerLink = screen.getByTestId('footer-register-link');
      expect(registerLink).toHaveAttribute('href', '/register');
    });
  });

  // Test Case 5: Check copyright year
  describe('Check copyright year', () => {
    const originalDate = Date;

    beforeEach(() => {
      // Mock Date to return a fixed year
      vi.useFakeTimers();
      vi.setSystemTime(new Date('2026-03-02'));
    });

    afterEach(() => {
      vi.useRealTimers();
    });

    it('displays current year in copyright notice', () => {
      renderWithRouter(<HomeFooter />);

      const copyright = screen.getByTestId('footer-copyright');
      expect(copyright).toHaveTextContent('2026');
      expect(copyright).toHaveTextContent('Copyright');
      expect(copyright).toHaveTextContent('URL Shortener');
    });
  });

  // Additional tests for accessibility
  describe('Accessibility', () => {
    it('has proper accessibility attributes', () => {
      renderWithRouter(<HomeFooter />);

      const footer = screen.getByRole('contentinfo');
      expect(footer).toHaveAttribute('aria-label', 'Site footer');

      // Navigation should have aria-label
      const nav = screen.getByRole('navigation', { name: 'Footer navigation' });
      expect(nav).toBeInTheDocument();
    });

    it('renders the Link2 icon in the brand', () => {
      renderWithRouter(<HomeFooter />);

      const footer = screen.getByTestId('home-footer');
      const svg = footer.querySelector('svg');
      expect(svg).toBeInTheDocument();
      expect(svg).toHaveAttribute('aria-hidden', 'true');
    });
  });

  // Test custom className prop
  describe('Custom className', () => {
    it('accepts and applies custom className', () => {
      renderWithRouter(<HomeFooter className="custom-class" />);

      const footer = screen.getByTestId('home-footer');
      expect(footer).toHaveClass('custom-class');
    });
  });
});
