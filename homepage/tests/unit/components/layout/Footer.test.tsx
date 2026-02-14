import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { Footer } from '../../../../src/components/layout/Footer';
import { GITHUB_URL, COPYRIGHT_YEAR, PROJECT_NAME } from '../../../../src/utils/constants';

describe('Footer', () => {
  // Test Case 4: Footer renders with GitHub link and copyright text
  describe('rendering', () => {
    it('renders the footer element', () => {
      render(<Footer />);
      const footer = screen.getByRole('contentinfo');
      expect(footer).toBeInTheDocument();
    });

    it('renders copyright text with current year', () => {
      render(<Footer />);
      expect(screen.getByText(new RegExp(`© ${COPYRIGHT_YEAR}`))).toBeInTheDocument();
    });

    it('renders project name in copyright', () => {
      render(<Footer />);
      expect(screen.getByText(new RegExp(PROJECT_NAME))).toBeInTheDocument();
    });

    it('renders "All rights reserved" text', () => {
      render(<Footer />);
      expect(screen.getByText(/all rights reserved/i)).toBeInTheDocument();
    });
  });

  // GitHub link tests
  describe('GitHub link', () => {
    it('renders GitHub link', () => {
      render(<Footer />);
      const githubLink = screen.getByRole('link', { name: /github/i });
      expect(githubLink).toBeInTheDocument();
    });

    it('GitHub link has correct href', () => {
      render(<Footer />);
      const githubLink = screen.getByRole('link', { name: /github/i });
      expect(githubLink).toHaveAttribute('href', GITHUB_URL);
    });

    // Test Case 7: External GitHub link opens in new tab with rel='noopener noreferrer'
    it('GitHub link opens in new tab', () => {
      render(<Footer />);
      const githubLink = screen.getByRole('link', { name: /github/i });
      expect(githubLink).toHaveAttribute('target', '_blank');
    });

    it('GitHub link has rel="noopener noreferrer" for security', () => {
      render(<Footer />);
      const githubLink = screen.getByRole('link', { name: /github/i });
      expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
    });

    it('GitHub link has accessible aria-label', () => {
      render(<Footer />);
      const githubLink = screen.getByRole('link', { name: /github/i });
      expect(githubLink).toHaveAttribute('aria-label');
    });
  });

  // Accessibility tests
  describe('accessibility', () => {
    it('uses semantic footer element', () => {
      render(<Footer />);
      const footer = screen.getByRole('contentinfo');
      expect(footer.tagName.toLowerCase()).toBe('footer');
    });

    it('GitHub icon is hidden from screen readers', () => {
      render(<Footer />);
      const svg = screen.getByRole('link', { name: /github/i }).querySelector('svg');
      expect(svg).toHaveAttribute('aria-hidden', 'true');
    });
  });

  // Styling tests
  describe('styling', () => {
    it('has background color class', () => {
      render(<Footer />);
      const footer = screen.getByRole('contentinfo');
      expect(footer).toHaveClass('bg-gray-100');
    });

    it('has border styling', () => {
      render(<Footer />);
      const footer = screen.getByRole('contentinfo');
      expect(footer).toHaveClass('border-t');
    });
  });
});
