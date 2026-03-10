/**
 * Unit tests for Footer component
 * Owner: Scenario 9 - Footer Section
 *
 * Test coverage:
 * - Rendering of footer links (Privacy Policy, Terms of Service, Contact)
 * - Copyright notice with current year
 * - Accessibility attributes
 * - Responsive layout classes
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import { Footer } from '../../../src/components/landing/Footer';
import { FOOTER_CONTENT } from '../../../src/constants/landingContent';

// Wrapper component for router context
function renderWithRouter(ui: React.ReactElement) {
  return render(<BrowserRouter>{ui}</BrowserRouter>);
}

describe('Footer', () => {
  describe('Test Case 1: Render Footer component with links', () => {
    it('should render footer section with proper semantic element', () => {
      renderWithRouter(<Footer />);

      const footer = screen.getByTestId('footer-section');
      expect(footer).toBeInTheDocument();
      expect(footer.tagName).toBe('FOOTER');
    });

    it('should contain Privacy Policy link', () => {
      renderWithRouter(<Footer />);

      const privacyLink = screen.getByTestId('footer-link-privacy-policy');
      expect(privacyLink).toBeInTheDocument();
      expect(privacyLink).toHaveTextContent('Privacy Policy');
      expect(privacyLink).toHaveAttribute('href', '/privacy');
    });

    it('should contain Terms of Service link', () => {
      renderWithRouter(<Footer />);

      const termsLink = screen.getByTestId('footer-link-terms-of-service');
      expect(termsLink).toBeInTheDocument();
      expect(termsLink).toHaveTextContent('Terms of Service');
      expect(termsLink).toHaveAttribute('href', '/terms');
    });

    it('should contain Contact link', () => {
      renderWithRouter(<Footer />);

      const contactLink = screen.getByTestId('footer-link-contact');
      expect(contactLink).toBeInTheDocument();
      expect(contactLink).toHaveTextContent('Contact');
      expect(contactLink).toHaveAttribute('href', '/contact');
    });

    it('should render all expected links from content constants', () => {
      renderWithRouter(<Footer />);

      FOOTER_CONTENT.links.forEach((link) => {
        const linkElement = screen.getByRole('link', { name: link.label });
        expect(linkElement).toBeInTheDocument();
        expect(linkElement).toHaveAttribute('href', link.href);
      });
    });
  });

  describe('Test Case 4: Footer copyright text with current year', () => {
    let mockDate: Date;

    beforeEach(() => {
      // Mock the Date to control the year
      mockDate = new Date('2026-03-10');
      vi.useFakeTimers();
      vi.setSystemTime(mockDate);
    });

    afterEach(() => {
      vi.useRealTimers();
    });

    it('should display copyright notice', () => {
      renderWithRouter(<Footer />);

      const copyright = screen.getByTestId('footer-copyright');
      expect(copyright).toBeInTheDocument();
    });

    it('should include the current year in copyright', () => {
      renderWithRouter(<Footer />);

      const copyright = screen.getByTestId('footer-copyright');
      expect(copyright).toHaveTextContent('2026');
    });

    it('should include the company name from content constants', () => {
      renderWithRouter(<Footer />);

      const copyright = screen.getByTestId('footer-copyright');
      expect(copyright).toHaveTextContent(FOOTER_CONTENT.copyright);
    });

    it('should display full copyright format', () => {
      renderWithRouter(<Footer />);

      const copyright = screen.getByTestId('footer-copyright');
      // Should contain copyright symbol, year, and company name
      expect(copyright.textContent).toMatch(/©\s*2026/);
      expect(copyright.textContent).toMatch(/URL Shortener/);
    });
  });

  describe('Accessibility', () => {
    it('should have role="contentinfo" for footer element', () => {
      renderWithRouter(<Footer />);

      const footer = screen.getByTestId('footer-section');
      expect(footer).toHaveAttribute('role', 'contentinfo');
    });

    it('should have navigation with aria-label', () => {
      renderWithRouter(<Footer />);

      const nav = screen.getByTestId('footer-nav');
      expect(nav).toHaveAttribute('aria-label', 'Footer navigation');
    });

    it('should have accessible links with proper link role', () => {
      renderWithRouter(<Footer />);

      const links = screen.getAllByRole('link');
      expect(links.length).toBeGreaterThanOrEqual(3);
      links.forEach((link) => {
        expect(link).toHaveAttribute('href');
      });
    });

    it('should have focus indicators for links', () => {
      renderWithRouter(<Footer />);

      const privacyLink = screen.getByTestId('footer-link-privacy-policy');
      // Check that focus styling classes are present
      expect(privacyLink.className).toMatch(/focus:/);
    });
  });

  describe('Visual styling', () => {
    it('should have background styling', () => {
      renderWithRouter(<Footer />);

      const footer = screen.getByTestId('footer-section');
      expect(footer.className).toMatch(/bg-/);
    });

    it('should have proper padding for visual spacing', () => {
      renderWithRouter(<Footer />);

      const footer = screen.getByTestId('footer-section');
      expect(footer.className).toMatch(/py-|p-/);
    });

    it('should have border styling for visual separation', () => {
      renderWithRouter(<Footer />);

      const footer = screen.getByTestId('footer-section');
      expect(footer.className).toMatch(/border/);
    });
  });

  describe('Responsive layout', () => {
    it('should have responsive flex classes for mobile stacking', () => {
      renderWithRouter(<Footer />);

      // Check for responsive flex classes that handle mobile vs desktop
      const footer = screen.getByTestId('footer-section');
      const content = footer.querySelector('.flex');
      expect(content).toBeTruthy();
      // Should have flex-col for mobile stacking
      expect(content?.className).toMatch(/flex-col/);
      // Should have md:flex-row for desktop horizontal layout
      expect(content?.className).toMatch(/md:flex-row/);
    });

    it('should have responsive gap classes', () => {
      renderWithRouter(<Footer />);

      const footer = screen.getByTestId('footer-section');
      const content = footer.querySelector('.flex');
      expect(content?.className).toMatch(/gap-/);
    });
  });

  describe('Links navigation', () => {
    it('should use React Router Link components for client-side navigation', () => {
      renderWithRouter(<Footer />);

      // Links should be <a> elements from React Router's Link
      const privacyLink = screen.getByTestId('footer-link-privacy-policy');
      expect(privacyLink.tagName).toBe('A');
    });

    it('should have hover state transition classes', () => {
      renderWithRouter(<Footer />);

      const privacyLink = screen.getByTestId('footer-link-privacy-policy');
      expect(privacyLink.className).toMatch(/transition/);
      expect(privacyLink.className).toMatch(/hover:/);
    });
  });
});
