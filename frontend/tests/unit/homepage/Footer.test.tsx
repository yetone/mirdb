/**
 * Footer Unit Tests
 * Owner: Scenario 10 - Footer Section
 *
 * Tests footer rendering including:
 * - Footer element with semantic HTML
 * - Terms of Service link
 * - Privacy Policy link
 * - Contact link
 * - Copyright notice with year
 */

import { describe, it, expect } from 'vitest';
import { screen } from '@testing-library/react';
import { Footer } from '../../../src/components/homepage/Footer';
import { renderWithProviders } from './test-utils';

describe('Footer', () => {
  describe('Test Case 1: Footer element exists using semantic HTML', () => {
    it('renders footer element with semantic HTML', () => {
      renderWithProviders(<Footer />);

      const footer = screen.getByRole('contentinfo');
      expect(footer).toBeInTheDocument();
      expect(footer).toBeVisible();
    });

    it('footer has correct test id', () => {
      renderWithProviders(<Footer />);

      const footer = screen.getByTestId('footer');
      expect(footer).toBeInTheDocument();
    });

    it('footer contains navigation section', () => {
      renderWithProviders(<Footer />);

      const nav = screen.getByRole('navigation', { name: /footer navigation/i });
      expect(nav).toBeInTheDocument();
    });
  });

  describe('Test Case 2: Terms of Service link is present', () => {
    it('displays Terms of Service link', () => {
      renderWithProviders(<Footer />);

      const termsLink = screen.getByTestId('footer-terms-link');
      expect(termsLink).toBeInTheDocument();
      expect(termsLink).toBeVisible();
    });

    it('Terms link has correct text', () => {
      renderWithProviders(<Footer />);

      const termsLink = screen.getByTestId('footer-terms-link');
      expect(termsLink).toHaveTextContent('Terms of Service');
    });

    it('Terms link navigates to /terms', () => {
      renderWithProviders(<Footer />);

      const termsLink = screen.getByTestId('footer-terms-link');
      expect(termsLink).toHaveAttribute('href', '/terms');
    });

    it('Terms link is accessible via role', () => {
      renderWithProviders(<Footer />);

      const termsLink = screen.getByRole('link', { name: /terms of service/i });
      expect(termsLink).toBeInTheDocument();
    });
  });

  describe('Test Case 3: Privacy Policy link is present', () => {
    it('displays Privacy Policy link', () => {
      renderWithProviders(<Footer />);

      const privacyLink = screen.getByTestId('footer-privacy-link');
      expect(privacyLink).toBeInTheDocument();
      expect(privacyLink).toBeVisible();
    });

    it('Privacy link has correct text', () => {
      renderWithProviders(<Footer />);

      const privacyLink = screen.getByTestId('footer-privacy-link');
      expect(privacyLink).toHaveTextContent('Privacy Policy');
    });

    it('Privacy link navigates to /privacy', () => {
      renderWithProviders(<Footer />);

      const privacyLink = screen.getByTestId('footer-privacy-link');
      expect(privacyLink).toHaveAttribute('href', '/privacy');
    });

    it('Privacy link is accessible via role', () => {
      renderWithProviders(<Footer />);

      const privacyLink = screen.getByRole('link', { name: /privacy policy/i });
      expect(privacyLink).toBeInTheDocument();
    });
  });

  describe('Test Case 4: Contact link is present', () => {
    it('displays Contact link', () => {
      renderWithProviders(<Footer />);

      const contactLink = screen.getByTestId('footer-contact-link');
      expect(contactLink).toBeInTheDocument();
      expect(contactLink).toBeVisible();
    });

    it('Contact link has correct text', () => {
      renderWithProviders(<Footer />);

      const contactLink = screen.getByTestId('footer-contact-link');
      expect(contactLink).toHaveTextContent('Contact');
    });

    it('Contact link navigates to /contact', () => {
      renderWithProviders(<Footer />);

      const contactLink = screen.getByTestId('footer-contact-link');
      expect(contactLink).toHaveAttribute('href', '/contact');
    });

    it('Contact link is accessible via role', () => {
      renderWithProviders(<Footer />);

      const contactLink = screen.getByRole('link', { name: /contact/i });
      expect(contactLink).toBeInTheDocument();
    });
  });

  describe('Test Case 5: Copyright notice with year is displayed', () => {
    it('displays copyright notice', () => {
      renderWithProviders(<Footer />);

      const copyright = screen.getByTestId('footer-copyright');
      expect(copyright).toBeInTheDocument();
      expect(copyright).toBeVisible();
    });

    it('copyright includes current year', () => {
      renderWithProviders(<Footer />);

      const copyright = screen.getByTestId('footer-copyright');
      const currentYear = new Date().getFullYear().toString();
      expect(copyright.textContent).toContain(currentYear);
    });

    it('copyright includes copyright symbol', () => {
      renderWithProviders(<Footer />);

      const copyright = screen.getByTestId('footer-copyright');
      expect(copyright.textContent).toMatch(/\u00A9/);
    });

    it('copyright includes brand name', () => {
      renderWithProviders(<Footer />);

      const copyright = screen.getByTestId('footer-copyright');
      expect(copyright.textContent).toMatch(/url shortener/i);
    });

    it('copyright includes all rights reserved', () => {
      renderWithProviders(<Footer />);

      const copyright = screen.getByTestId('footer-copyright');
      expect(copyright.textContent).toMatch(/all rights reserved/i);
    });
  });

  describe('Accessibility', () => {
    it('footer has proper role for accessibility', () => {
      renderWithProviders(<Footer />);

      const footer = screen.getByRole('contentinfo');
      expect(footer).toBeInTheDocument();
    });

    it('navigation has aria-label for accessibility', () => {
      renderWithProviders(<Footer />);

      const nav = screen.getByRole('navigation', { name: /footer navigation/i });
      expect(nav).toHaveAttribute('aria-label', 'Footer navigation');
    });

    it('all links are keyboard accessible', () => {
      renderWithProviders(<Footer />);

      const links = screen.getAllByRole('link');
      links.forEach(link => {
        expect(link).not.toHaveAttribute('tabindex', '-1');
      });
    });
  });
});
