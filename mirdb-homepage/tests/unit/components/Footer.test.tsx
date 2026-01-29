/**
 * Unit tests for Footer section component.
 * Owner: Scenario 9 - Footer Section
 *
 * Tests cover:
 * - Footer component rendering
 * - GitHub link presence and attributes
 * - License information display
 * - Acknowledgments section presence
 * - Copyright notice
 * - Accessibility features
 */

import { describe, it, expect } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { Footer } from '../../../src/components/Footer/Footer';

describe('Footer', () => {
  describe('rendering', () => {
    it('renders the footer section with correct role', () => {
      render(<Footer />);
      const footer = screen.getByRole('contentinfo');
      expect(footer).toBeInTheDocument();
    });

    it('renders footer with aria-label for accessibility', () => {
      render(<Footer />);
      const footer = screen.getByRole('contentinfo');
      expect(footer).toHaveAttribute('aria-label', 'Site footer');
    });

    it('renders the MirDB project title', () => {
      render(<Footer />);
      expect(screen.getByText('MirDB')).toBeInTheDocument();
    });

    it('renders footer with custom className when provided', () => {
      render(<Footer className="custom-footer-class" />);
      const footer = screen.getByRole('contentinfo');
      expect(footer).toHaveClass('custom-footer-class');
    });
  });

  describe('GitHub link', () => {
    it('renders the GitHub repository link', () => {
      render(<Footer />);
      const githubLink = screen.getByLabelText(/View MirDB on GitHub/i);
      expect(githubLink).toBeInTheDocument();
    });

    it('GitHub link points to correct default URL', () => {
      render(<Footer />);
      const githubLink = screen.getByLabelText(/View MirDB on GitHub/i);
      expect(githubLink).toHaveAttribute('href', 'https://github.com/mirdb/mirdb');
    });

    it('GitHub link opens in new tab', () => {
      render(<Footer />);
      const githubLink = screen.getByLabelText(/View MirDB on GitHub/i);
      expect(githubLink).toHaveAttribute('target', '_blank');
    });

    it('GitHub link has secure rel attribute', () => {
      render(<Footer />);
      const githubLink = screen.getByLabelText(/View MirDB on GitHub/i);
      expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
    });

    it('renders GitHub icon in the link', () => {
      render(<Footer />);
      const githubLink = screen.getByLabelText(/View MirDB on GitHub/i);
      const svg = githubLink.querySelector('svg');
      expect(svg).toBeInTheDocument();
      expect(svg).toHaveAttribute('aria-hidden', 'true');
    });

    it('accepts custom GitHub URL', () => {
      const customUrl = 'https://github.com/custom/repo';
      render(<Footer githubUrl={customUrl} />);
      const githubLink = screen.getByLabelText(/View MirDB on GitHub/i);
      expect(githubLink).toHaveAttribute('href', customUrl);
    });

    it('displays "GitHub Repository" text', () => {
      render(<Footer />);
      expect(screen.getByText('GitHub Repository')).toBeInTheDocument();
    });
  });

  describe('license information', () => {
    it('displays license type', () => {
      render(<Footer />);
      const licenseLink = screen.getByTestId('license-link');
      expect(licenseLink).toHaveTextContent('MIT License');
    });

    it('license link points to correct URL', () => {
      render(<Footer />);
      const licenseLink = screen.getByTestId('license-link');
      expect(licenseLink).toHaveAttribute('href', 'https://opensource.org/licenses/MIT');
    });

    it('license link opens in new tab', () => {
      render(<Footer />);
      const licenseLink = screen.getByTestId('license-link');
      expect(licenseLink).toHaveAttribute('target', '_blank');
    });

    it('license link has secure rel attribute', () => {
      render(<Footer />);
      const licenseLink = screen.getByTestId('license-link');
      expect(licenseLink).toHaveAttribute('rel', 'noopener noreferrer');
    });

    it('accepts custom license type', () => {
      render(<Footer licenseType="Apache 2.0" />);
      const licenseLink = screen.getByTestId('license-link');
      expect(licenseLink).toHaveTextContent('Apache 2.0 License');
    });

    it('accepts custom license URL', () => {
      const customLicenseUrl = 'https://www.apache.org/licenses/LICENSE-2.0';
      render(<Footer licenseUrl={customLicenseUrl} />);
      const licenseLink = screen.getByTestId('license-link');
      expect(licenseLink).toHaveAttribute('href', customLicenseUrl);
    });

    it('displays "Released under the" text', () => {
      render(<Footer />);
      expect(screen.getByText(/Released under the/i)).toBeInTheDocument();
    });
  });

  describe('acknowledgments section', () => {
    it('renders acknowledgments heading', () => {
      render(<Footer />);
      expect(screen.getByText('Acknowledgments')).toBeInTheDocument();
    });

    it('renders acknowledgments section', () => {
      render(<Footer />);
      const acknowledgments = screen.getByTestId('acknowledgments');
      expect(acknowledgments).toBeInTheDocument();
    });

    it('acknowledges Rust language', () => {
      render(<Footer />);
      const acknowledgments = screen.getByTestId('acknowledgments');
      expect(within(acknowledgments).getByText(/Built with Rust/i)).toBeInTheDocument();
    });

    it('acknowledges Tokio async runtime', () => {
      render(<Footer />);
      const acknowledgments = screen.getByTestId('acknowledgments');
      expect(within(acknowledgments).getByText(/Tokio/i)).toBeInTheDocument();
    });

    it('contains Tokio link', () => {
      render(<Footer />);
      const tokioLink = screen.getByRole('link', { name: /Tokio/i });
      expect(tokioLink).toHaveAttribute('href', 'https://tokio.rs');
    });

    it('Tokio link opens in new tab', () => {
      render(<Footer />);
      const tokioLink = screen.getByRole('link', { name: /Tokio/i });
      expect(tokioLink).toHaveAttribute('target', '_blank');
      expect(tokioLink).toHaveAttribute('rel', 'noopener noreferrer');
    });

    it('thanks contributors', () => {
      render(<Footer />);
      const acknowledgments = screen.getByTestId('acknowledgments');
      expect(within(acknowledgments).getByText(/contributors/i)).toBeInTheDocument();
    });
  });

  describe('copyright notice', () => {
    it('displays copyright symbol and year', () => {
      render(<Footer />);
      const currentYear = new Date().getFullYear();
      expect(screen.getByText(new RegExp(`© ${currentYear} MirDB`))).toBeInTheDocument();
    });

    it('displays "All rights reserved"', () => {
      render(<Footer />);
      expect(screen.getByText(/All rights reserved/i)).toBeInTheDocument();
    });
  });

  describe('navigation links', () => {
    it('renders footer navigation', () => {
      render(<Footer />);
      const nav = screen.getByRole('navigation', { name: /footer navigation/i });
      expect(nav).toBeInTheDocument();
    });

    it('contains Features link', () => {
      render(<Footer />);
      const featuresLink = screen.getByRole('link', { name: /Features/i });
      expect(featuresLink).toHaveAttribute('href', '#features');
    });

    it('contains Installation link', () => {
      render(<Footer />);
      const installationLink = screen.getByRole('link', { name: /Installation/i });
      expect(installationLink).toHaveAttribute('href', '#installation');
    });

    it('renders Links heading', () => {
      render(<Footer />);
      expect(screen.getByText('Links')).toBeInTheDocument();
    });
  });

  describe('project description', () => {
    it('displays project description', () => {
      render(<Footer />);
      expect(
        screen.getByText(/persistent key-value store with Memcached protocol compatibility/i)
      ).toBeInTheDocument();
    });

    it('mentions LSM-tree architecture', () => {
      render(<Footer />);
      expect(screen.getByText(/LSM-tree architecture/i)).toBeInTheDocument();
    });
  });

  describe('accessibility', () => {
    it('all external links have secure attributes', () => {
      render(<Footer />);
      const externalLinks = screen.getAllByRole('link').filter(
        (link) => link.getAttribute('target') === '_blank'
      );

      externalLinks.forEach((link) => {
        expect(link).toHaveAttribute('rel', 'noopener noreferrer');
      });
    });

    it('GitHub link has descriptive aria-label', () => {
      render(<Footer />);
      const githubLink = screen.getByLabelText(/View MirDB on GitHub \(opens in new tab\)/i);
      expect(githubLink).toBeInTheDocument();
    });

    it('footer sections have proper heading hierarchy', () => {
      render(<Footer />);
      const headings = screen.getAllByRole('heading', { level: 3 });
      expect(headings.length).toBe(3); // MirDB, Links, Acknowledgments
    });
  });

  describe('styling', () => {
    it('has surface background color class', () => {
      render(<Footer />);
      const footer = screen.getByRole('contentinfo');
      expect(footer).toHaveClass('bg-surface');
    });

    it('has border styling', () => {
      render(<Footer />);
      const footer = screen.getByRole('contentinfo');
      expect(footer).toHaveClass('border-t');
      expect(footer).toHaveClass('border-border');
    });

    it('has padding for layout', () => {
      render(<Footer />);
      const footer = screen.getByRole('contentinfo');
      expect(footer).toHaveClass('py-12');
    });
  });
});
