/**
 * Landing Page Unit Tests - Navigation Links
 * Owner: Scenario 4 - Navigation Links
 *
 * Tests for navigation header with Login/Register links
 */
import { describe, it, expect } from 'vitest';
import { screen } from '@testing-library/react';
import { renderWithProviders } from './setup';
import Home from '../../../src/pages/Home';

describe('Landing Page Navigation', () => {
  describe('Test Case 1: Navigation bar presence', () => {
    it('renders navigation bar in the header', () => {
      renderWithProviders(<Home />);

      // The navbar should be in a header element
      const header = screen.getByRole('banner');
      expect(header).toBeInTheDocument();

      // Navigation should contain navigation bar class
      expect(header).toHaveClass('navbar');
    });
  });

  describe('Test Case 2: Login link', () => {
    it('renders a Login link with correct href', () => {
      renderWithProviders(<Home />);

      const loginLink = screen.getByRole('link', { name: /login/i });
      expect(loginLink).toBeInTheDocument();
      expect(loginLink).toHaveAttribute('href', '/login');
    });
  });

  describe('Test Case 3: Register/Sign Up link', () => {
    it('renders a Register or Sign Up link/button', () => {
      renderWithProviders(<Home />);

      // Could be "Register", "Sign Up", or "Get Started"
      const registerElement = screen.getByRole('link', { name: /get started|register|sign up/i });
      expect(registerElement).toBeInTheDocument();
      expect(registerElement).toHaveAttribute('href', '/register');
    });
  });

  describe('Test Case 4: Logo/brand element', () => {
    it('renders logo/brand name that links to homepage', () => {
      renderWithProviders(<Home />);

      // Should have URL Shortener brand text
      const logoLink = screen.getByRole('link', { name: /url shortener/i });
      expect(logoLink).toBeInTheDocument();
      expect(logoLink).toHaveAttribute('href', '/');
    });
  });
});
