/**
 * Footer Component Tests
 * Owner: Scenario 11 - Footer Display
 *
 * Test cases:
 * 1. Footer is rendered at the bottom of the page
 * 2. Copyright information is displayed
 * 3. Links to About, Terms of Service, Privacy Policy, Contact are present
 * 4. Footer styling matches current theme
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { Footer } from '../../src/components/Footer';
import { ThemeProvider } from '../../src/contexts/ThemeContext';

function renderWithProviders(ui: React.ReactElement) {
  return render(<ThemeProvider>{ui}</ThemeProvider>);
}

describe('Footer', () => {
  describe('Test Case 1: Footer is rendered at the bottom of the page', () => {
    it('renders the footer component', () => {
      renderWithProviders(<Footer />);
      const footer = screen.getByTestId('footer');
      expect(footer).toBeInTheDocument();
    });

    it('renders as a footer element', () => {
      renderWithProviders(<Footer />);
      const footer = screen.getByRole('contentinfo');
      expect(footer).toBeInTheDocument();
    });

    it('has proper container styling for bottom placement', () => {
      renderWithProviders(<Footer />);
      const footer = screen.getByTestId('footer');
      expect(footer).toHaveClass('w-full');
    });
  });

  describe('Test Case 2: Copyright information is displayed', () => {
    it('displays copyright text', () => {
      renderWithProviders(<Footer />);
      const copyright = screen.getByTestId('footer-copyright');
      expect(copyright).toBeInTheDocument();
      expect(copyright).toHaveTextContent('URL Shortener');
      expect(copyright).toHaveTextContent('All rights reserved');
    });

    it('displays the current year in copyright', () => {
      renderWithProviders(<Footer />);
      const currentYear = new Date().getFullYear();
      const copyright = screen.getByTestId('footer-copyright');
      expect(copyright).toHaveTextContent(currentYear.toString());
    });

    it('displays the copyright symbol', () => {
      renderWithProviders(<Footer />);
      const copyright = screen.getByTestId('footer-copyright');
      expect(copyright.textContent).toContain('\u00A9');
    });
  });

  describe('Test Case 3: Links to About, Terms of Service, Privacy Policy, Contact are present', () => {
    it('renders the footer links container', () => {
      renderWithProviders(<Footer />);
      const linksContainer = screen.getByTestId('footer-links');
      expect(linksContainer).toBeInTheDocument();
    });

    it('renders About link', () => {
      renderWithProviders(<Footer />);
      const aboutLink = screen.getByTestId('footer-link-about');
      expect(aboutLink).toBeInTheDocument();
      expect(aboutLink).toHaveTextContent('About');
      expect(aboutLink).toHaveAttribute('href', '/about');
    });

    it('renders Terms of Service link', () => {
      renderWithProviders(<Footer />);
      const termsLink = screen.getByTestId('footer-link-terms-of-service');
      expect(termsLink).toBeInTheDocument();
      expect(termsLink).toHaveTextContent('Terms of Service');
      expect(termsLink).toHaveAttribute('href', '/terms');
    });

    it('renders Privacy Policy link', () => {
      renderWithProviders(<Footer />);
      const privacyLink = screen.getByTestId('footer-link-privacy-policy');
      expect(privacyLink).toBeInTheDocument();
      expect(privacyLink).toHaveTextContent('Privacy Policy');
      expect(privacyLink).toHaveAttribute('href', '/privacy');
    });

    it('renders Contact link', () => {
      renderWithProviders(<Footer />);
      const contactLink = screen.getByTestId('footer-link-contact');
      expect(contactLink).toBeInTheDocument();
      expect(contactLink).toHaveTextContent('Contact');
      expect(contactLink).toHaveAttribute('href', '/contact');
    });

    it('has proper navigation role for accessibility', () => {
      renderWithProviders(<Footer />);
      const nav = screen.getByRole('navigation', { name: /footer navigation/i });
      expect(nav).toBeInTheDocument();
    });

    it('renders all four links', () => {
      renderWithProviders(<Footer />);
      const links = screen.getAllByRole('link');
      expect(links).toHaveLength(4);
    });
  });

  describe('Test Case 4: Footer styling matches current theme', () => {
    it('uses theme-aware background color', () => {
      renderWithProviders(<Footer />);
      const footer = screen.getByTestId('footer');
      expect(footer).toHaveClass('bg-base-200');
    });

    it('uses theme-aware border color', () => {
      renderWithProviders(<Footer />);
      const footer = screen.getByTestId('footer');
      expect(footer).toHaveClass('border-base-300');
    });

    it('uses theme-aware text color for copyright', () => {
      renderWithProviders(<Footer />);
      const copyright = screen.getByTestId('footer-copyright');
      expect(copyright).toHaveClass('text-base-content/70');
    });

    it('links have hover effect for theme integration', () => {
      renderWithProviders(<Footer />);
      const aboutLink = screen.getByTestId('footer-link-about');
      expect(aboutLink).toHaveClass('hover:text-primary');
    });

    it('links have transition styling', () => {
      renderWithProviders(<Footer />);
      const aboutLink = screen.getByTestId('footer-link-about');
      expect(aboutLink).toHaveClass('transition-colors');
    });
  });

  describe('Responsive Design', () => {
    it('has responsive flex layout classes', () => {
      renderWithProviders(<Footer />);
      const footer = screen.getByTestId('footer');
      const container = footer.querySelector('.container');
      expect(container?.querySelector('.flex')).toBeInTheDocument();
    });

    it('has responsive gap classes for links', () => {
      renderWithProviders(<Footer />);
      const linksContainer = screen.getByTestId('footer-links');
      expect(linksContainer).toHaveClass('gap-4', 'md:gap-6');
    });
  });
});
