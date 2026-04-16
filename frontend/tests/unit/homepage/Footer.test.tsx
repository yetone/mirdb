/**
 * Unit Tests for Footer Component.
 * Owner: Scenario 10 - Footer Section
 *
 * Tests the footer section displays copyright information with current year
 * and optional links with consistent styling.
 */
import { describe, it, expect } from 'vitest';
import { screen } from '@testing-library/react';
import { renderWithProviders } from './setup';
import { Footer } from '../../../src/components/homepage/Footer';

describe('Footer', () => {
  describe('Test Case 1: Footer component renders at bottom of page', () => {
    it('renders the footer section element', () => {
      renderWithProviders(<Footer />);

      const footerSection = screen.getByTestId('footer-section');
      expect(footerSection).toBeInTheDocument();
    });

    it('uses semantic footer element', () => {
      renderWithProviders(<Footer />);

      const footerSection = screen.getByTestId('footer-section');
      expect(footerSection.tagName).toBe('FOOTER');
    });

    it('footer is visible on the page', () => {
      renderWithProviders(<Footer />);

      const footerSection = screen.getByTestId('footer-section');
      expect(footerSection).toBeVisible();
    });
  });

  describe('Test Case 2: Copyright notice includes 2026 or current year', () => {
    it('displays copyright text with current year', () => {
      renderWithProviders(<Footer />);

      const copyright = screen.getByTestId('footer-copyright');
      expect(copyright).toBeInTheDocument();

      const currentYear = new Date().getFullYear().toString();
      expect(copyright.textContent).toContain(currentYear);
    });

    it('copyright text includes "2026" in 2026', () => {
      renderWithProviders(<Footer />);

      const copyright = screen.getByTestId('footer-copyright');
      // This test verifies the year is dynamically set
      const year = new Date().getFullYear();
      expect(copyright.textContent).toContain(year.toString());
    });

    it('copyright includes service name', () => {
      renderWithProviders(<Footer />);

      const copyright = screen.getByTestId('footer-copyright');
      expect(copyright.textContent).toMatch(/ShortURL/i);
    });

    it('copyright includes reserved rights text', () => {
      renderWithProviders(<Footer />);

      const copyright = screen.getByTestId('footer-copyright');
      expect(copyright.textContent).toMatch(/all rights reserved/i);
    });
  });

  describe('Test Case 3: Footer maintains consistent styling with rest of homepage', () => {
    it('footer has proper container styling', () => {
      renderWithProviders(<Footer />);

      const footerSection = screen.getByTestId('footer-section');
      expect(footerSection).toHaveClass('w-full');
      expect(footerSection).toHaveClass('py-8');
      expect(footerSection).toHaveClass('px-4');
    });

    it('footer has border styling for visual separation', () => {
      renderWithProviders(<Footer />);

      const footerSection = screen.getByTestId('footer-section');
      expect(footerSection).toHaveClass('border-t');
    });

    it('footer uses theme-aware background styling', () => {
      renderWithProviders(<Footer />);

      const footerSection = screen.getByTestId('footer-section');
      // Uses DaisyUI's base-200 color which adapts to theme
      expect(footerSection.className).toContain('bg-base-200');
    });

    it('copyright text has muted styling consistent with homepage', () => {
      renderWithProviders(<Footer />);

      const copyright = screen.getByTestId('footer-copyright');
      // Uses theme-aware opacity for muted text
      expect(copyright.className).toContain('text-base-content');
      expect(copyright).toHaveClass('text-sm');
    });
  });

  describe('Footer Links', () => {
    it('renders without links when none provided', () => {
      renderWithProviders(<Footer />);

      const links = screen.queryByTestId('footer-links');
      expect(links).not.toBeInTheDocument();
    });

    it('renders links when provided', () => {
      const testLinks = [
        { label: 'Privacy Policy', href: '/privacy' },
        { label: 'Terms of Service', href: '/terms' },
      ];

      renderWithProviders(<Footer links={testLinks} />);

      const linksNav = screen.getByTestId('footer-links');
      expect(linksNav).toBeInTheDocument();
    });

    it('displays correct link labels', () => {
      const testLinks = [
        { label: 'Privacy Policy', href: '/privacy' },
        { label: 'Terms of Service', href: '/terms' },
      ];

      renderWithProviders(<Footer links={testLinks} />);

      expect(screen.getByText('Privacy Policy')).toBeInTheDocument();
      expect(screen.getByText('Terms of Service')).toBeInTheDocument();
    });

    it('links have correct href attributes', () => {
      const testLinks = [
        { label: 'Privacy Policy', href: '/privacy' },
        { label: 'Terms of Service', href: '/terms' },
      ];

      renderWithProviders(<Footer links={testLinks} />);

      const privacyLink = screen.getByTestId('footer-link-0');
      const termsLink = screen.getByTestId('footer-link-1');

      expect(privacyLink).toHaveAttribute('href', '/privacy');
      expect(termsLink).toHaveAttribute('href', '/terms');
    });

    it('links have accessible hover and focus styling', () => {
      const testLinks = [{ label: 'Privacy Policy', href: '/privacy' }];

      renderWithProviders(<Footer links={testLinks} />);

      const link = screen.getByTestId('footer-link-0');
      expect(link.className).toContain('hover:text-primary');
      expect(link.className).toContain('focus:ring-2');
    });
  });

  describe('Accessibility', () => {
    it('footer links navigation has proper aria-label', () => {
      const testLinks = [{ label: 'Privacy Policy', href: '/privacy' }];

      renderWithProviders(<Footer links={testLinks} />);

      const linksNav = screen.getByTestId('footer-links');
      expect(linksNav).toHaveAttribute('aria-label', 'Footer navigation');
    });

    it('uses nav element for link grouping', () => {
      const testLinks = [{ label: 'Privacy Policy', href: '/privacy' }];

      renderWithProviders(<Footer links={testLinks} />);

      const linksNav = screen.getByTestId('footer-links');
      expect(linksNav.tagName).toBe('NAV');
    });

    it('copyright text is readable with proper text sizing', () => {
      renderWithProviders(<Footer />);

      const copyright = screen.getByTestId('footer-copyright');
      expect(copyright).toHaveClass('text-sm');
    });
  });

  describe('Responsive Design', () => {
    it('footer has responsive flex layout', () => {
      renderWithProviders(<Footer />);

      const footerSection = screen.getByTestId('footer-section');
      const innerContainer = footerSection.querySelector('.flex');
      expect(innerContainer).toHaveClass('flex-col');
      expect(innerContainer).toHaveClass('md:flex-row');
    });
  });
});
