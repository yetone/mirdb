/**
 * Footer component tests.
 * Owner: Scenario 7 - Footer Section Display
 *
 * Test coverage:
 * - Footer renders with navigation links
 * - Copyright displays current year
 * - Links navigate to correct routes
 * - Footer styling matches DaisyUI patterns
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { Footer } from '@/components/layout/Footer';
import { renderWithProviders } from '../../utils/renderWithProviders';

describe('Footer Section Display', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('Test Case 1: Footer renders with navigation links and copyright', () => {
    it('should render Footer component with all required elements', () => {
      renderWithProviders(<Footer />);

      // Verify Footer is present
      const footer = screen.getByTestId('footer');
      expect(footer).toBeInTheDocument();

      // Verify footer is a semantic footer element
      expect(footer.tagName).toBe('FOOTER');
    });

    it('should display Home navigation link', () => {
      renderWithProviders(<Footer />);

      const homeLink = screen.getByRole('link', { name: /home/i });
      expect(homeLink).toBeInTheDocument();
      expect(homeLink).toHaveTextContent('Home');
    });

    it('should display Login navigation link', () => {
      renderWithProviders(<Footer />);

      const loginLink = screen.getByRole('link', { name: /login/i });
      expect(loginLink).toBeInTheDocument();
      expect(loginLink).toHaveTextContent('Login');
    });

    it('should display Register navigation link', () => {
      renderWithProviders(<Footer />);

      const registerLink = screen.getByRole('link', { name: /register/i });
      expect(registerLink).toBeInTheDocument();
      expect(registerLink).toHaveTextContent('Register');
    });

    it('should display copyright text', () => {
      renderWithProviders(<Footer />);

      const copyright = screen.getByTestId('footer-copyright');
      expect(copyright).toBeInTheDocument();
      expect(copyright).toHaveTextContent(/copyright/i);
    });

    it('should have all three navigation links in nav element', () => {
      renderWithProviders(<Footer />);

      const footer = screen.getByTestId('footer');
      const navElement = footer.querySelector('nav');
      expect(navElement).toBeInTheDocument();

      const links = within(navElement!).getAllByRole('link');
      expect(links).toHaveLength(3);
    });
  });

  describe('Test Case 2: Copyright text displays current year dynamically', () => {
    it('should display current year in copyright text', () => {
      renderWithProviders(<Footer />);

      const copyright = screen.getByTestId('footer-copyright');
      const currentYear = new Date().getFullYear().toString();
      expect(copyright).toHaveTextContent(currentYear);
    });

    it('should include copyright symbol and year', () => {
      renderWithProviders(<Footer />);

      const copyright = screen.getByTestId('footer-copyright');
      const currentYear = new Date().getFullYear();
      expect(copyright).toHaveTextContent(`Copyright © ${currentYear}`);
    });

    it('should display company name in copyright', () => {
      renderWithProviders(<Footer />);

      const copyright = screen.getByTestId('footer-copyright');
      expect(copyright).toHaveTextContent('URLShort');
    });

    it('should include all rights reserved text', () => {
      renderWithProviders(<Footer />);

      const copyright = screen.getByTestId('footer-copyright');
      expect(copyright).toHaveTextContent('All rights reserved');
    });

    it('should format copyright text correctly with all components', () => {
      renderWithProviders(<Footer />);

      const copyright = screen.getByTestId('footer-copyright');
      const currentYear = new Date().getFullYear();
      const expectedText = `Copyright © ${currentYear} - URLShort. All rights reserved.`;
      expect(copyright).toHaveTextContent(expectedText);
    });
  });

  describe('Test Case 3: Links navigate to correct routes (/, /login, /register)', () => {
    it('should have Home link with correct href pointing to /', () => {
      renderWithProviders(<Footer />);

      const homeLink = screen.getByRole('link', { name: /home/i });
      expect(homeLink).toHaveAttribute('href', '/');
    });

    it('should have Login link with correct href pointing to /login', () => {
      renderWithProviders(<Footer />);

      const loginLink = screen.getByRole('link', { name: /login/i });
      expect(loginLink).toHaveAttribute('href', '/login');
    });

    it('should have Register link with correct href pointing to /register', () => {
      renderWithProviders(<Footer />);

      const registerLink = screen.getByRole('link', { name: /register/i });
      expect(registerLink).toHaveAttribute('href', '/register');
    });

    it('should navigate when Home link is clicked', async () => {
      const user = userEvent.setup();
      renderWithProviders(<Footer />, { initialEntries: ['/other'] });

      const homeLink = screen.getByRole('link', { name: /home/i });
      expect(homeLink).toHaveAttribute('href', '/');

      // Verify the link is clickable
      await user.click(homeLink);
    });

    it('should navigate when Login link is clicked', async () => {
      const user = userEvent.setup();
      renderWithProviders(<Footer />, { initialEntries: ['/'] });

      const loginLink = screen.getByRole('link', { name: /login/i });
      expect(loginLink).toHaveAttribute('href', '/login');

      // Verify the link is clickable
      await user.click(loginLink);
    });

    it('should navigate when Register link is clicked', async () => {
      const user = userEvent.setup();
      renderWithProviders(<Footer />, { initialEntries: ['/'] });

      const registerLink = screen.getByRole('link', { name: /register/i });
      expect(registerLink).toHaveAttribute('href', '/register');

      // Verify the link is clickable
      await user.click(registerLink);
    });

    it('should use React Router Link components (anchor tags)', () => {
      renderWithProviders(<Footer />);

      const homeLink = screen.getByRole('link', { name: /home/i });
      const loginLink = screen.getByRole('link', { name: /login/i });
      const registerLink = screen.getByRole('link', { name: /register/i });

      expect(homeLink.tagName).toBe('A');
      expect(loginLink.tagName).toBe('A');
      expect(registerLink.tagName).toBe('A');
    });
  });

  describe('Test Case 4: Footer uses DaisyUI footer-center with bg-base-200', () => {
    it('should have footer DaisyUI class', () => {
      renderWithProviders(<Footer />);

      const footer = screen.getByTestId('footer');
      expect(footer).toHaveClass('footer');
    });

    it('should have footer-center DaisyUI class for centered layout', () => {
      renderWithProviders(<Footer />);

      const footer = screen.getByTestId('footer');
      expect(footer).toHaveClass('footer-center');
    });

    it('should have bg-base-200 background class', () => {
      renderWithProviders(<Footer />);

      const footer = screen.getByTestId('footer');
      expect(footer).toHaveClass('bg-base-200');
    });

    it('should have text-base-content class for text color', () => {
      renderWithProviders(<Footer />);

      const footer = screen.getByTestId('footer');
      expect(footer).toHaveClass('text-base-content');
    });

    it('should have padding class', () => {
      renderWithProviders(<Footer />);

      const footer = screen.getByTestId('footer');
      expect(footer).toHaveClass('p-10');
    });

    it('should have all required DaisyUI classes combined', () => {
      renderWithProviders(<Footer />);

      const footer = screen.getByTestId('footer');
      expect(footer).toHaveClass('footer', 'footer-center', 'bg-base-200', 'text-base-content', 'p-10');
    });

    it('should have link-hover class on navigation links', () => {
      renderWithProviders(<Footer />);

      const homeLink = screen.getByRole('link', { name: /home/i });
      const loginLink = screen.getByRole('link', { name: /login/i });
      const registerLink = screen.getByRole('link', { name: /register/i });

      expect(homeLink).toHaveClass('link', 'link-hover');
      expect(loginLink).toHaveClass('link', 'link-hover');
      expect(registerLink).toHaveClass('link', 'link-hover');
    });

    it('should have grid layout for navigation links', () => {
      renderWithProviders(<Footer />);

      const footer = screen.getByTestId('footer');
      const navElement = footer.querySelector('nav');

      expect(navElement).toHaveClass('grid');
      expect(navElement).toHaveClass('grid-flow-col');
      expect(navElement).toHaveClass('gap-4');
    });
  });

  describe('Accessibility', () => {
    it('should have proper semantic footer element', () => {
      renderWithProviders(<Footer />);

      const footer = screen.getByTestId('footer');
      expect(footer.tagName).toBe('FOOTER');
    });

    it('should have navigation element for footer links', () => {
      renderWithProviders(<Footer />);

      const footer = screen.getByTestId('footer');
      const nav = footer.querySelector('nav');
      expect(nav).toBeInTheDocument();
    });

    it('should have aside element for copyright', () => {
      renderWithProviders(<Footer />);

      const footer = screen.getByTestId('footer');
      const aside = footer.querySelector('aside');
      expect(aside).toBeInTheDocument();
    });

    it('should have focusable navigation links', () => {
      renderWithProviders(<Footer />);

      const homeLink = screen.getByRole('link', { name: /home/i });
      const loginLink = screen.getByRole('link', { name: /login/i });
      const registerLink = screen.getByRole('link', { name: /register/i });

      // Links should be focusable by default (not have tabindex=-1)
      expect(homeLink).not.toHaveAttribute('tabindex', '-1');
      expect(loginLink).not.toHaveAttribute('tabindex', '-1');
      expect(registerLink).not.toHaveAttribute('tabindex', '-1');
    });

    it('should be keyboard navigable through all links', async () => {
      const user = userEvent.setup();
      renderWithProviders(<Footer />);

      const homeLink = screen.getByRole('link', { name: /home/i });
      const loginLink = screen.getByRole('link', { name: /login/i });
      const registerLink = screen.getByRole('link', { name: /register/i });

      // Focus on first link
      homeLink.focus();
      expect(document.activeElement).toBe(homeLink);

      // Tab to next link
      await user.tab();
      expect(document.activeElement).toBe(loginLink);

      // Tab to next link
      await user.tab();
      expect(document.activeElement).toBe(registerLink);
    });
  });
});
