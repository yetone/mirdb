/**
 * Footer component unit tests.
 * Owner: Scenario 5 - Footer Section
 *
 * Tests for:
 * - Semantic footer element
 * - Legal links (Privacy Policy, Terms of Service)
 * - Social media links with proper aria-labels
 * - Copyright notice with current year
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '../../../tests/setup/test-utils';
import { Footer } from './Footer';

describe('Footer', () => {
  describe('Semantic Structure', () => {
    it('renders a semantic footer element', () => {
      render(<Footer />);
      const footer = screen.getByRole('contentinfo');
      expect(footer).toBeInTheDocument();
    });
  });

  describe('Legal Links', () => {
    it('renders Privacy Policy link', () => {
      render(<Footer />);
      const privacyLink = screen.getByRole('link', { name: /privacy policy/i });
      expect(privacyLink).toBeInTheDocument();
      expect(privacyLink).toHaveAttribute('href', '/privacy');
    });

    it('renders Terms of Service link', () => {
      render(<Footer />);
      const termsLink = screen.getByRole('link', { name: /terms of service/i });
      expect(termsLink).toBeInTheDocument();
      expect(termsLink).toHaveAttribute('href', '/terms');
    });
  });

  describe('Social Media Links', () => {
    it('renders Twitter link with proper aria-label', () => {
      render(<Footer />);
      const twitterLink = screen.getByRole('link', { name: /twitter/i });
      expect(twitterLink).toBeInTheDocument();
      expect(twitterLink).toHaveAttribute('href', 'https://twitter.com');
      expect(twitterLink).toHaveAttribute('target', '_blank');
      expect(twitterLink).toHaveAttribute('rel', 'noopener noreferrer');
    });

    it('renders GitHub link with proper aria-label', () => {
      render(<Footer />);
      const githubLink = screen.getByRole('link', { name: /github/i });
      expect(githubLink).toBeInTheDocument();
      expect(githubLink).toHaveAttribute('href', 'https://github.com');
      expect(githubLink).toHaveAttribute('target', '_blank');
      expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
    });

    it('renders LinkedIn link with proper aria-label', () => {
      render(<Footer />);
      const linkedinLink = screen.getByRole('link', { name: /linkedin/i });
      expect(linkedinLink).toBeInTheDocument();
      expect(linkedinLink).toHaveAttribute('href', 'https://linkedin.com');
      expect(linkedinLink).toHaveAttribute('target', '_blank');
      expect(linkedinLink).toHaveAttribute('rel', 'noopener noreferrer');
    });
  });

  describe('Copyright Notice', () => {
    it('displays copyright text with current year', () => {
      render(<Footer />);
      const currentYear = new Date().getFullYear();
      const copyrightText = screen.getByText(new RegExp(`© ${currentYear}`, 'i'));
      expect(copyrightText).toBeInTheDocument();
    });

    it('includes the product name in copyright', () => {
      render(<Footer />);
      const copyrightText = screen.getByText(/product landing page/i);
      expect(copyrightText).toBeInTheDocument();
    });
  });

  describe('Accessibility', () => {
    it('has navigation landmark for footer links', () => {
      render(<Footer />);
      const nav = screen.getByRole('navigation', { name: /footer/i });
      expect(nav).toBeInTheDocument();
    });

    it('all links are keyboard accessible', () => {
      render(<Footer />);
      const links = screen.getAllByRole('link');
      links.forEach(link => {
        expect(link).not.toHaveAttribute('tabindex', '-1');
      });
    });
  });
});
