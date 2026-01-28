/**
 * Footer Component Unit Tests
 * Owner: Scenario 9 - Footer Component
 *
 * Tests for:
 * - Copyright notice display with current year
 * - Login and register links
 * - Semantic footer HTML element
 * - Theme integration
 *
 * Requirements:
 * - REQ-9: Include footer with essential links and copyright
 */

import { describe, it, expect, vi } from 'vitest';
import { screen, within } from '@testing-library/react';
import { Footer } from '@/components/homepage/Footer';
import { renderWithProviders } from '../../../utils/test-helpers';
import { ThemeProvider } from '@/contexts/ThemeContext';
import React from 'react';

describe('Footer Component', () => {
  describe('Test Case 1: Footer renders with copyright notice visible', () => {
    it('should render the footer with copyright notice', () => {
      renderWithProviders(<Footer />);

      // Check that the footer element exists
      const footerElement = screen.getByRole('contentinfo');
      expect(footerElement).toBeInTheDocument();

      // Check that copyright text is visible
      expect(screen.getByText(/All rights reserved/i)).toBeInTheDocument();
    });
  });

  describe('Test Case 2: Copyright text includes current year and service name', () => {
    it('should display copyright with current year', () => {
      const currentYear = new Date().getFullYear();
      renderWithProviders(<Footer />);

      const copyrightText = screen.getByText(new RegExp(`©\\s*${currentYear}`, 'i'));
      expect(copyrightText).toBeInTheDocument();
    });

    it('should display the service name in copyright', () => {
      renderWithProviders(<Footer />);

      expect(screen.getByText(/URL Shortener/i)).toBeInTheDocument();
    });

    it('should allow custom copyright year via prop', () => {
      const customYear = 2025;
      renderWithProviders(<Footer copyrightYear={customYear} />);

      const copyrightText = screen.getByText(new RegExp(`©\\s*${customYear}`, 'i'));
      expect(copyrightText).toBeInTheDocument();
    });
  });

  describe('Test Case 3: Login link is present', () => {
    it('should display a login link', () => {
      renderWithProviders(<Footer />);

      const loginLink = screen.getByRole('link', { name: /login/i });
      expect(loginLink).toBeInTheDocument();
      expect(loginLink).toHaveAttribute('href', '/login');
    });
  });

  describe('Test Case 4: Register link is present', () => {
    it('should display a register link', () => {
      renderWithProviders(<Footer />);

      const registerLink = screen.getByRole('link', { name: /register/i });
      expect(registerLink).toBeInTheDocument();
      expect(registerLink).toHaveAttribute('href', '/register');
    });
  });

  describe('Test Case 5: Component renders a footer element (semantic HTML)', () => {
    it('should render a semantic footer element', () => {
      const { container } = renderWithProviders(<Footer />);

      const footerElement = container.querySelector('footer');
      expect(footerElement).toBeInTheDocument();
    });

    it('should have contentinfo role for accessibility', () => {
      renderWithProviders(<Footer />);

      const footerElement = screen.getByRole('contentinfo');
      expect(footerElement).toBeInTheDocument();
      expect(footerElement.tagName.toLowerCase()).toBe('footer');
    });

    it('should have navigation with proper aria-label', () => {
      renderWithProviders(<Footer />);

      const nav = screen.getByRole('navigation', { name: /footer navigation/i });
      expect(nav).toBeInTheDocument();
    });
  });

  describe('Test Case 6: Footer respects current theme colors and styling', () => {
    it('should render with theme-aware classes', () => {
      const { container } = renderWithProviders(<Footer />);

      const footerElement = container.querySelector('footer');
      expect(footerElement).toBeInTheDocument();

      // Check for DaisyUI theme classes that respect the theme
      expect(footerElement).toHaveClass('bg-base-300');
      expect(footerElement).toHaveClass('text-base-content');
    });

    it('should render correctly within ThemeProvider', () => {
      // Mock localStorage
      const localStorageMock = {
        getItem: vi.fn().mockReturnValue('dark'),
        setItem: vi.fn(),
        removeItem: vi.fn(),
        clear: vi.fn(),
        length: 0,
        key: vi.fn(),
      };
      Object.defineProperty(window, 'localStorage', { value: localStorageMock });

      const { container } = renderWithProviders(
        <ThemeProvider>
          <Footer />
        </ThemeProvider>
      );

      const footerElement = container.querySelector('footer');
      expect(footerElement).toBeInTheDocument();

      // Footer should still have base classes that respond to theme
      expect(footerElement).toHaveClass('footer');
    });

    it('should have proper styling classes for centered layout', () => {
      const { container } = renderWithProviders(<Footer />);

      const footerElement = container.querySelector('footer');
      expect(footerElement).toHaveClass('footer-center');
    });

    it('should have links with hover styling', () => {
      renderWithProviders(<Footer />);

      const loginLink = screen.getByRole('link', { name: /login/i });
      const registerLink = screen.getByRole('link', { name: /register/i });

      // Check for DaisyUI hover classes
      expect(loginLink).toHaveClass('link-hover');
      expect(registerLink).toHaveClass('link-hover');
    });
  });

  describe('Additional Edge Cases', () => {
    it('should have proper padding', () => {
      const { container } = renderWithProviders(<Footer />);

      const footerElement = container.querySelector('footer');
      expect(footerElement).toHaveClass('p-10');
    });

    it('should have navigation links in a grid layout', () => {
      renderWithProviders(<Footer />);

      const nav = screen.getByRole('navigation', { name: /footer navigation/i });
      expect(nav).toHaveClass('grid');
      expect(nav).toHaveClass('grid-flow-col');
    });
  });
});
