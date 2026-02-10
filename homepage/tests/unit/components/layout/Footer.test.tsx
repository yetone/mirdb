/**
 * Footer Component Unit Tests
 * Owner: Scenario 10 - Footer and Project Metadata
 *
 * Tests for:
 * - Rendering footer with project metadata
 * - CircleCI badge display and link
 * - GitHub repository link
 * - Proper semantic structure
 * - Accessibility attributes
 */

import React from 'react';
import { render, screen } from '@testing-library/react';
import { describe, it, expect } from 'vitest';
import { Footer } from '@/components/layout/Footer';

describe('Footer Component', () => {
  describe('Test Case 1: Footer is rendered with project metadata', () => {
    it('renders as a footer element with contentinfo role', () => {
      render(<Footer />);
      const footer = screen.getByRole('contentinfo');
      expect(footer).toBeInTheDocument();
    });

    it('displays the project name', () => {
      render(<Footer />);
      // MirDB appears multiple times, use getAllByText
      const matches = screen.getAllByText(/MirDB/);
      expect(matches.length).toBeGreaterThan(0);
    });

    it('displays the project description', () => {
      render(<Footer />);
      expect(screen.getByText(/Persistent Key-Value Store with Memcached Protocol/)).toBeInTheDocument();
    });

    it('displays the technical stack information', () => {
      render(<Footer />);
      expect(screen.getByText(/Built with Rust and Tokio/)).toBeInTheDocument();
    });

    it('displays copyright information with current year', () => {
      render(<Footer />);
      const currentYear = new Date().getFullYear();
      expect(screen.getByText(new RegExp(`© ${currentYear}`))).toBeInTheDocument();
    });

    it('displays license information', () => {
      render(<Footer />);
      expect(screen.getByText(/MIT License/)).toBeInTheDocument();
    });

    it('has aria-label for the footer', () => {
      render(<Footer />);
      const footer = screen.getByRole('contentinfo', { name: /site footer/i });
      expect(footer).toBeInTheDocument();
    });
  });

  describe('Test Case 2: CircleCI badge is displayed with link to CI dashboard', () => {
    it('renders CircleCI badge image', () => {
      render(<Footer />);
      const badge = screen.getByTestId('circleci-badge');
      expect(badge).toBeInTheDocument();
      expect(badge).toHaveAttribute('src', 'https://circleci.com/gh/yetone/mirdb.svg?style=svg');
    });

    it('badge has proper alt text', () => {
      render(<Footer />);
      const badge = screen.getByAltText('CircleCI build status');
      expect(badge).toBeInTheDocument();
    });

    it('badge is wrapped in a link to CircleCI dashboard', () => {
      render(<Footer />);
      const badgeLink = screen.getByTestId('circleci-badge-link');
      expect(badgeLink).toBeInTheDocument();
      expect(badgeLink).toHaveAttribute('href', 'https://circleci.com/gh/yetone/mirdb');
    });

    it('badge link opens in new tab', () => {
      render(<Footer />);
      const badgeLink = screen.getByTestId('circleci-badge-link');
      expect(badgeLink).toHaveAttribute('target', '_blank');
      expect(badgeLink).toHaveAttribute('rel', 'noopener noreferrer');
    });

    it('badge link has accessible label', () => {
      render(<Footer />);
      const badgeLink = screen.getByLabelText('View CircleCI build status');
      expect(badgeLink).toBeInTheDocument();
    });
  });

  describe('Test Case 3: GitHub repository link is present', () => {
    it('renders GitHub repository link', () => {
      render(<Footer />);
      const githubLink = screen.getByTestId('github-link');
      expect(githubLink).toBeInTheDocument();
    });

    it('GitHub link has correct href', () => {
      render(<Footer />);
      const githubLink = screen.getByTestId('github-link');
      expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    });

    it('GitHub link displays appropriate text', () => {
      render(<Footer />);
      expect(screen.getByText('GitHub Repository')).toBeInTheDocument();
    });

    it('GitHub link opens in new tab', () => {
      render(<Footer />);
      const githubLink = screen.getByTestId('github-link');
      expect(githubLink).toHaveAttribute('target', '_blank');
      expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
    });

    it('GitHub link has accessible label', () => {
      render(<Footer />);
      const githubLink = screen.getByLabelText('View MirDB on GitHub');
      expect(githubLink).toBeInTheDocument();
    });
  });

  describe('Additional Footer Links', () => {
    it('renders link to report issues', () => {
      render(<Footer />);
      const issuesLink = screen.getByText('Report Issues');
      expect(issuesLink).toBeInTheDocument();
      expect(issuesLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb/issues');
    });

    it('renders link to documentation', () => {
      render(<Footer />);
      const docsLink = screen.getByText('Documentation');
      expect(docsLink).toBeInTheDocument();
      expect(docsLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb#readme');
    });
  });

  describe('Accessibility', () => {
    it('has proper semantic structure with contentinfo role', () => {
      render(<Footer />);
      const footer = screen.getByRole('contentinfo');
      expect(footer).toBeInTheDocument();
    });

    it('contains a navigation section for links', () => {
      render(<Footer />);
      const nav = screen.getByRole('navigation', { name: /footer navigation/i });
      expect(nav).toBeInTheDocument();
    });

    it('all external links have proper security attributes', () => {
      render(<Footer />);
      const links = screen.getAllByRole('link');
      links.forEach((link) => {
        if (link.getAttribute('target') === '_blank') {
          expect(link).toHaveAttribute('rel', 'noopener noreferrer');
        }
      });
    });
  });

  describe('Visual Styling', () => {
    it('has background color from CSS variables', () => {
      render(<Footer />);
      const footer = screen.getByRole('contentinfo');
      expect(footer).toHaveStyle({ backgroundColor: 'var(--bg-secondary)' });
    });

    it('has top border for visual separation', () => {
      render(<Footer />);
      const footer = screen.getByRole('contentinfo');
      expect(footer).toHaveStyle({ borderTop: '1px solid var(--border-color)' });
    });
  });
});
