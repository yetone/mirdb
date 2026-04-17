/**
 * Footer Tests
 * Owner: Scenario 6 - Footer Navigation and Content
 *
 * Tests for the Footer component:
 * - Footer semantic element rendering
 * - Navigation links (Home, Features, Pricing, About, Contact)
 * - Legal links (Privacy Policy, Terms of Service)
 * - Copyright notice with current year
 * - Social media links
 */

import { describe, it, expect } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { Footer } from '@/components/homepage/Footer';

describe('Footer', () => {
  // Test Case 1: Footer semantic element exists
  describe('Footer container', () => {
    it('renders the footer semantic element', () => {
      render(<Footer />);

      const footer = screen.getByTestId('footer');
      expect(footer).toBeInTheDocument();
      expect(footer.tagName).toBe('FOOTER');
    });

    it('has proper accessibility attributes', () => {
      render(<Footer />);

      const footer = screen.getByTestId('footer');
      expect(footer).toHaveAttribute('role', 'contentinfo');
      expect(footer).toHaveAttribute('aria-label', 'Site footer');
    });
  });

  // Test Case 2: Navigation links are present
  describe('Navigation links', () => {
    it('renders navigation links section', () => {
      render(<Footer />);

      const navLinks = screen.getByTestId('footer-nav-links');
      expect(navLinks).toBeInTheDocument();
    });

    it('contains Home link', () => {
      render(<Footer />);

      const homeLink = screen.getByTestId('footer-link-home');
      expect(homeLink).toBeInTheDocument();
      expect(homeLink).toHaveAttribute('href', '/');
      expect(homeLink).toHaveTextContent('Home');
    });

    it('contains Features link', () => {
      render(<Footer />);

      const featuresLink = screen.getByTestId('footer-link-features');
      expect(featuresLink).toBeInTheDocument();
      expect(featuresLink).toHaveAttribute('href', '/#features');
      expect(featuresLink).toHaveTextContent('Features');
    });

    it('contains Pricing link', () => {
      render(<Footer />);

      const pricingLink = screen.getByTestId('footer-link-pricing');
      expect(pricingLink).toBeInTheDocument();
      expect(pricingLink).toHaveAttribute('href', '/pricing');
      expect(pricingLink).toHaveTextContent('Pricing');
    });

    it('contains About link', () => {
      render(<Footer />);

      const aboutLink = screen.getByTestId('footer-link-about');
      expect(aboutLink).toBeInTheDocument();
      expect(aboutLink).toHaveAttribute('href', '/about');
      expect(aboutLink).toHaveTextContent('About');
    });

    it('contains Contact link', () => {
      render(<Footer />);

      const contactLink = screen.getByTestId('footer-link-contact');
      expect(contactLink).toBeInTheDocument();
      expect(contactLink).toHaveAttribute('href', '/contact');
      expect(contactLink).toHaveTextContent('Contact');
    });

    it('all navigation links are present (Home, Features, Pricing, About, Contact)', () => {
      render(<Footer />);

      const navLinks = screen.getByTestId('footer-nav-links');
      const links = within(navLinks).getAllByRole('link');

      expect(links).toHaveLength(5);
      expect(screen.getByTestId('footer-link-home')).toBeInTheDocument();
      expect(screen.getByTestId('footer-link-features')).toBeInTheDocument();
      expect(screen.getByTestId('footer-link-pricing')).toBeInTheDocument();
      expect(screen.getByTestId('footer-link-about')).toBeInTheDocument();
      expect(screen.getByTestId('footer-link-contact')).toBeInTheDocument();
    });
  });

  // Test Case 3: Privacy Policy link exists and is clickable
  describe('Privacy Policy link', () => {
    it('renders Privacy Policy link', () => {
      render(<Footer />);

      const privacyLink = screen.getByTestId('footer-link-privacy-policy');
      expect(privacyLink).toBeInTheDocument();
    });

    it('Privacy Policy link has correct text', () => {
      render(<Footer />);

      const privacyLink = screen.getByTestId('footer-link-privacy-policy');
      expect(privacyLink).toHaveTextContent('Privacy Policy');
    });

    it('Privacy Policy link is clickable with proper href', () => {
      render(<Footer />);

      const privacyLink = screen.getByTestId('footer-link-privacy-policy');
      expect(privacyLink.tagName).toBe('A');
      expect(privacyLink).toHaveAttribute('href', '/privacy');
    });
  });

  // Test Case 4: Terms of Service link exists and is clickable
  describe('Terms of Service link', () => {
    it('renders Terms of Service link', () => {
      render(<Footer />);

      const termsLink = screen.getByTestId('footer-link-terms-of-service');
      expect(termsLink).toBeInTheDocument();
    });

    it('Terms of Service link has correct text', () => {
      render(<Footer />);

      const termsLink = screen.getByTestId('footer-link-terms-of-service');
      expect(termsLink).toHaveTextContent('Terms of Service');
    });

    it('Terms of Service link is clickable with proper href', () => {
      render(<Footer />);

      const termsLink = screen.getByTestId('footer-link-terms-of-service');
      expect(termsLink.tagName).toBe('A');
      expect(termsLink).toHaveAttribute('href', '/terms');
    });
  });

  // Test Case 5: Copyright notice with year is displayed
  describe('Copyright notice', () => {
    it('renders copyright notice', () => {
      render(<Footer />);

      const copyright = screen.getByTestId('footer-copyright');
      expect(copyright).toBeInTheDocument();
    });

    it('copyright includes current year', () => {
      render(<Footer />);

      const copyright = screen.getByTestId('footer-copyright');
      const currentYear = new Date().getFullYear();

      expect(copyright.textContent).toContain(currentYear.toString());
    });

    it('copyright includes copyright symbol and brand name', () => {
      render(<Footer />);

      const copyright = screen.getByTestId('footer-copyright');

      expect(copyright.textContent).toContain('©');
      expect(copyright.textContent).toContain('LinkShort');
    });

    it('copyright has proper format', () => {
      render(<Footer />);

      const copyright = screen.getByTestId('footer-copyright');
      const currentYear = new Date().getFullYear();

      expect(copyright.textContent).toMatch(
        new RegExp(`©\\s*${currentYear}\\s+\\w+.*All rights reserved`, 'i')
      );
    });
  });

  // Test Case 6: Social media icons/links are present
  describe('Social media links', () => {
    it('renders social media links section', () => {
      render(<Footer />);

      const socialLinks = screen.getByTestId('footer-social-links');
      expect(socialLinks).toBeInTheDocument();
    });

    it('contains Twitter social link', () => {
      render(<Footer />);

      const twitterLink = screen.getByTestId('footer-social-twitter');
      expect(twitterLink).toBeInTheDocument();
      expect(twitterLink.tagName).toBe('A');
      expect(twitterLink).toHaveAttribute('target', '_blank');
      expect(twitterLink).toHaveAttribute('rel', 'noopener noreferrer');
    });

    it('contains GitHub social link', () => {
      render(<Footer />);

      const githubLink = screen.getByTestId('footer-social-github');
      expect(githubLink).toBeInTheDocument();
      expect(githubLink.tagName).toBe('A');
      expect(githubLink).toHaveAttribute('target', '_blank');
    });

    it('contains LinkedIn social link', () => {
      render(<Footer />);

      const linkedinLink = screen.getByTestId('footer-social-linkedin');
      expect(linkedinLink).toBeInTheDocument();
      expect(linkedinLink.tagName).toBe('A');
      expect(linkedinLink).toHaveAttribute('target', '_blank');
    });

    it('contains Facebook social link', () => {
      render(<Footer />);

      const facebookLink = screen.getByTestId('footer-social-facebook');
      expect(facebookLink).toBeInTheDocument();
      expect(facebookLink.tagName).toBe('A');
      expect(facebookLink).toHaveAttribute('target', '_blank');
    });

    it('social links have accessible labels', () => {
      render(<Footer />);

      const twitterLink = screen.getByTestId('footer-social-twitter');
      const githubLink = screen.getByTestId('footer-social-github');
      const linkedinLink = screen.getByTestId('footer-social-linkedin');
      const facebookLink = screen.getByTestId('footer-social-facebook');

      expect(twitterLink).toHaveAttribute('aria-label', 'Twitter');
      expect(githubLink).toHaveAttribute('aria-label', 'GitHub');
      expect(linkedinLink).toHaveAttribute('aria-label', 'LinkedIn');
      expect(facebookLink).toHaveAttribute('aria-label', 'Facebook');
    });

    it('social links contain icon elements', () => {
      render(<Footer />);

      const socialLinks = screen.getByTestId('footer-social-links');

      // Icons are hidden from screen readers with aria-hidden
      // So we check for svg elements directly
      const svgs = socialLinks.querySelectorAll('svg');
      expect(svgs.length).toBeGreaterThanOrEqual(4);
    });

    it('multiple social media links are present', () => {
      render(<Footer />);

      const socialLinks = screen.getByTestId('footer-social-links');
      const links = within(socialLinks).getAllByRole('link');

      expect(links.length).toBeGreaterThanOrEqual(3);
    });
  });

  // Additional integration tests
  describe('Footer integration', () => {
    it('has proper section headings', () => {
      render(<Footer />);

      expect(screen.getByTestId('footer-nav-heading')).toHaveTextContent('Quick Links');
      expect(screen.getByTestId('footer-legal-heading')).toHaveTextContent('Legal');
      expect(screen.getByTestId('footer-social-heading')).toHaveTextContent('Connect With Us');
    });

    it('renders with proper grid layout', () => {
      render(<Footer />);

      const footer = screen.getByTestId('footer');
      const gridContainer = footer.querySelector('.grid');

      expect(gridContainer).toBeInTheDocument();
    });

    it('legal links section is separate from navigation', () => {
      render(<Footer />);

      const legalLinks = screen.getByTestId('footer-legal-links');
      const navLinks = screen.getByTestId('footer-nav-links');

      expect(legalLinks).not.toBe(navLinks);

      const legalLinkElements = within(legalLinks).getAllByRole('link');
      expect(legalLinkElements).toHaveLength(2);
    });
  });
});
