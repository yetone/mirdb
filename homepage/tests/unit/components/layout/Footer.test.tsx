/**
 * Unit tests for Footer component.
 * Owner: Scenario 7 - Footer Section
 */

import React from 'react';
import { render, screen } from '@testing-library/react';
import { Footer } from '@/components/layout/Footer';

describe('Footer Component', () => {
  // Test Case 1: Footer displays 'MIT License' text with link to license file
  describe('TC1: MIT License display', () => {
    it('should display "MIT License" text', () => {
      render(<Footer />);

      const licenseLink = screen.getByRole('link', { name: /MIT License/i });
      expect(licenseLink).toBeInTheDocument();
    });

    it('should link MIT License to the LICENSE file on GitHub', () => {
      render(<Footer />);

      const licenseLink = screen.getByRole('link', { name: /MIT License/i });
      expect(licenseLink).toHaveAttribute(
        'href',
        'https://github.com/yetone/mirdb/blob/master/LICENSE'
      );
    });

    it('should open license link in a new tab', () => {
      render(<Footer />);

      const licenseLink = screen.getByRole('link', { name: /MIT License/i });
      expect(licenseLink).toHaveAttribute('target', '_blank');
      expect(licenseLink).toHaveAttribute('rel', 'noopener noreferrer');
    });

    it('should have accessible label for license link', () => {
      render(<Footer />);

      const licenseLink = screen.getByLabelText('View MIT License');
      expect(licenseLink).toBeInTheDocument();
    });
  });

  // Test Case 2: Copyright notice includes current year (2026)
  describe('TC2: Copyright notice with current year', () => {
    it('should display copyright notice with current year', () => {
      render(<Footer />);

      const currentYear = new Date().getFullYear();
      const copyrightText = screen.getByText(
        new RegExp(`©\\s*${currentYear}.*MirDB`)
      );
      expect(copyrightText).toBeInTheDocument();
    });

    it('should include "All rights reserved" in copyright notice', () => {
      render(<Footer />);

      const copyrightText = screen.getByText(/All rights reserved/i);
      expect(copyrightText).toBeInTheDocument();
    });

    it('should display copyright in a paragraph element', () => {
      render(<Footer />);

      const currentYear = new Date().getFullYear();
      const copyrightText = screen.getByText(
        new RegExp(`©\\s*${currentYear}.*MirDB`)
      );
      expect(copyrightText.tagName.toLowerCase()).toBe('p');
    });
  });

  // Test Case 3: GitHub links (repository, issues, discussions) are present
  describe('TC3: GitHub links presence', () => {
    it('should display Repository link', () => {
      render(<Footer />);

      const repoLink = screen.getByRole('link', { name: 'Repository' });
      expect(repoLink).toBeInTheDocument();
      expect(repoLink).toHaveAttribute(
        'href',
        'https://github.com/yetone/mirdb'
      );
    });

    it('should display Issues link', () => {
      render(<Footer />);

      const issuesLink = screen.getByRole('link', { name: 'Issues' });
      expect(issuesLink).toBeInTheDocument();
      expect(issuesLink).toHaveAttribute(
        'href',
        'https://github.com/yetone/mirdb/issues'
      );
    });

    it('should display Discussions link', () => {
      render(<Footer />);

      const discussionsLink = screen.getByRole('link', { name: 'Discussions' });
      expect(discussionsLink).toBeInTheDocument();
      expect(discussionsLink).toHaveAttribute(
        'href',
        'https://github.com/yetone/mirdb/discussions'
      );
    });

    it('should open all GitHub links in new tabs', () => {
      render(<Footer />);

      const repoLink = screen.getByRole('link', { name: 'Repository' });
      const issuesLink = screen.getByRole('link', { name: 'Issues' });
      const discussionsLink = screen.getByRole('link', { name: 'Discussions' });

      [repoLink, issuesLink, discussionsLink].forEach((link) => {
        expect(link).toHaveAttribute('target', '_blank');
        expect(link).toHaveAttribute('rel', 'noopener noreferrer');
      });
    });
  });

  // Additional structural and accessibility tests
  describe('Footer structure and accessibility', () => {
    it('should render as a footer element', () => {
      render(<Footer />);

      const footer = screen.getByRole('contentinfo');
      expect(footer).toBeInTheDocument();
    });

    it('should have navigation landmark for footer links', () => {
      render(<Footer />);

      const nav = screen.getByRole('navigation', { name: /footer navigation/i });
      expect(nav).toBeInTheDocument();
    });

    it('should render GitHub links in a list structure', () => {
      render(<Footer />);

      const listItems = screen.getAllByRole('listitem');
      expect(listItems).toHaveLength(3);
    });
  });
});
