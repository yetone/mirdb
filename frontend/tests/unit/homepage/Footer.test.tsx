/**
 * Unit tests for Footer component
 * Scenario 8 - Footer Section
 *
 * Tests the footer displays correctly with:
 * - Copyright notice with current year
 * - Relevant links
 * - Proper positioning at page bottom
 */

import { describe, it, expect, vi } from 'vitest';
import { screen } from '@testing-library/react';
import { Footer } from '../../../src/components/homepage/Footer';
import { renderWithProviders } from './test-utils';

describe('Footer', () => {
  describe('Test Case 1: Footer contains copyright notice with current year', () => {
    it('should display copyright notice with current year', () => {
      renderWithProviders(<Footer />);

      const copyright = screen.getByTestId('footer-copyright');
      expect(copyright).toBeInTheDocument();

      const currentYear = new Date().getFullYear();
      expect(copyright).toHaveTextContent(`${currentYear}`);
    });

    it('should display "Copyright ©" text', () => {
      renderWithProviders(<Footer />);

      const copyright = screen.getByTestId('footer-copyright');
      expect(copyright).toHaveTextContent(/copyright/i);
      expect(copyright).toHaveTextContent('©');
    });

    it('should display the application name in copyright', () => {
      renderWithProviders(<Footer />);

      const copyright = screen.getByTestId('footer-copyright');
      expect(copyright).toHaveTextContent(/url shortener/i);
    });

    it('should display "All rights reserved" text', () => {
      renderWithProviders(<Footer />);

      const copyright = screen.getByTestId('footer-copyright');
      expect(copyright).toHaveTextContent(/all rights reserved/i);
    });

    it('should update year dynamically', () => {
      // Mock Date to test year updates
      const mockDate = new Date('2025-06-15');
      vi.useFakeTimers();
      vi.setSystemTime(mockDate);

      renderWithProviders(<Footer />);

      const copyright = screen.getByTestId('footer-copyright');
      expect(copyright).toHaveTextContent('2025');

      vi.useRealTimers();
    });
  });

  describe('Test Case 2: Footer element exists and is positioned at bottom', () => {
    it('should render footer element with proper test ID', () => {
      renderWithProviders(<Footer />);

      const footer = screen.getByTestId('footer');
      expect(footer).toBeInTheDocument();
    });

    it('should render as a semantic footer element', () => {
      renderWithProviders(<Footer />);

      const footer = screen.getByRole('contentinfo');
      expect(footer).toBeInTheDocument();
    });

    it('should have footer CSS classes for positioning', () => {
      renderWithProviders(<Footer />);

      const footer = screen.getByTestId('footer');
      expect(footer).toHaveClass('footer');
      expect(footer).toHaveClass('footer-center');
    });

    it('should have proper background styling', () => {
      renderWithProviders(<Footer />);

      const footer = screen.getByTestId('footer');
      expect(footer).toHaveClass('bg-base-200');
    });

    it('should have border for visual separation', () => {
      renderWithProviders(<Footer />);

      const footer = screen.getByTestId('footer');
      expect(footer).toHaveClass('border-t');
    });
  });

  describe('Footer Links', () => {
    it('should display Terms of Service link', () => {
      renderWithProviders(<Footer />);

      const termsLink = screen.getByRole('link', { name: /terms of service/i });
      expect(termsLink).toBeInTheDocument();
      expect(termsLink).toHaveAttribute('href', '/terms');
    });

    it('should display Privacy Policy link', () => {
      renderWithProviders(<Footer />);

      const privacyLink = screen.getByRole('link', { name: /privacy policy/i });
      expect(privacyLink).toBeInTheDocument();
      expect(privacyLink).toHaveAttribute('href', '/privacy');
    });

    it('should display Contact link', () => {
      renderWithProviders(<Footer />);

      const contactLink = screen.getByRole('link', { name: /contact/i });
      expect(contactLink).toBeInTheDocument();
      expect(contactLink).toHaveAttribute('href', '/contact');
    });

    it('should have accessible navigation for footer links', () => {
      renderWithProviders(<Footer />);

      const nav = screen.getByRole('navigation', { name: /footer navigation/i });
      expect(nav).toBeInTheDocument();
    });

    it('should have all footer links accessible', () => {
      renderWithProviders(<Footer />);

      const links = screen.getAllByRole('link');
      expect(links.length).toBe(3);
    });
  });

  describe('Accessibility', () => {
    it('should have proper semantic structure', () => {
      renderWithProviders(<Footer />);

      // Footer should use contentinfo role (implied by <footer>)
      const footer = screen.getByRole('contentinfo');
      expect(footer).toBeInTheDocument();

      // Navigation should be labeled
      const nav = screen.getByRole('navigation', { name: /footer navigation/i });
      expect(nav).toBeInTheDocument();
    });

    it('should have readable text contrast', () => {
      renderWithProviders(<Footer />);

      const footer = screen.getByTestId('footer');
      // Check that proper text color classes are applied
      expect(footer).toHaveClass('text-base-content');
    });
  });
});
