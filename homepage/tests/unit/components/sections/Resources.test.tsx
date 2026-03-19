/**
 * Unit tests for Resources component.
 * Owner: Scenario 6 - Resources and Links Section
 */

import React from 'react';
import { render, screen } from '@testing-library/react';
import { Resources } from '@/components/sections/Resources';

describe('Resources Component', () => {
  // Test Case 1: Six resource cards are displayed in a grid layout
  describe('TC1: Renders six resource cards in a grid layout', () => {
    it('should render exactly six resource cards', () => {
      render(<Resources />);

      const resourceCards = screen.getAllByTestId(/^resource-card-/);
      expect(resourceCards).toHaveLength(6);
    });

    it('should render all six resource titles', () => {
      render(<Resources />);

      expect(screen.getByText('Documentation')).toBeInTheDocument();
      expect(screen.getByText('API Reference')).toBeInTheDocument();
      expect(screen.getByText('Examples')).toBeInTheDocument();
      expect(screen.getByText('Contributing Guide')).toBeInTheDocument();
      expect(screen.getByText('Issue Tracker')).toBeInTheDocument();
      expect(screen.getByText('License')).toBeInTheDocument();
    });

    it('should render cards with correct test IDs', () => {
      render(<Resources />);

      expect(screen.getByTestId('resource-card-documentation')).toBeInTheDocument();
      expect(screen.getByTestId('resource-card-api-reference')).toBeInTheDocument();
      expect(screen.getByTestId('resource-card-examples')).toBeInTheDocument();
      expect(screen.getByTestId('resource-card-contributing-guide')).toBeInTheDocument();
      expect(screen.getByTestId('resource-card-issue-tracker')).toBeInTheDocument();
      expect(screen.getByTestId('resource-card-license')).toBeInTheDocument();
    });
  });

  // Test Case 2: Card displays 'Documentation' title with link to docs
  describe('TC2: Documentation card with link to docs', () => {
    it('should display Documentation card with correct title', () => {
      render(<Resources />);

      const docTitle = screen.getByText('Documentation');
      expect(docTitle).toBeInTheDocument();
    });

    it('should have a link for Documentation resource', () => {
      render(<Resources />);

      const docLink = screen.getByTestId('resource-link-documentation');
      expect(docLink).toHaveAttribute('href');
      expect(docLink.getAttribute('href')).toContain('github.com/yetone/mirdb');
    });

    it('should display Documentation description', () => {
      render(<Resources />);

      expect(
        screen.getByText(/Learn how to install, configure, and use MirDB effectively/)
      ).toBeInTheDocument();
    });
  });

  // Test Case 3: Card displays 'Contributing Guide' with link to CONTRIBUTING.md
  describe('TC3: Contributing Guide card with link', () => {
    it('should display Contributing Guide card with correct title', () => {
      render(<Resources />);

      const contributingTitle = screen.getByText('Contributing Guide');
      expect(contributingTitle).toBeInTheDocument();
    });

    it('should have a link for Contributing Guide resource pointing to CONTRIBUTING.md', () => {
      render(<Resources />);

      const contributingLink = screen.getByTestId('resource-link-contributing-guide');
      expect(contributingLink).toHaveAttribute('href');
      expect(contributingLink.getAttribute('href')).toContain('CONTRIBUTING.md');
    });

    it('should display Contributing Guide description', () => {
      render(<Resources />);

      expect(
        screen.getByText(/Contribute to MirDB development/)
      ).toBeInTheDocument();
    });
  });

  // Test Case 4: External links have target='_blank' and rel='noopener noreferrer'
  describe('TC4: External link attributes for security', () => {
    it('should have target="_blank" on all external links', () => {
      render(<Resources />);

      const resourceLinks = screen.getAllByTestId(/^resource-link-/);
      resourceLinks.forEach((link) => {
        expect(link).toHaveAttribute('target', '_blank');
      });
    });

    it('should have rel="noopener noreferrer" on all external links', () => {
      render(<Resources />);

      const resourceLinks = screen.getAllByTestId(/^resource-link-/);
      resourceLinks.forEach((link) => {
        expect(link).toHaveAttribute('rel', 'noopener noreferrer');
      });
    });

    it('should have proper security attributes on Documentation link specifically', () => {
      render(<Resources />);

      const docLink = screen.getByTestId('resource-link-documentation');
      expect(docLink).toHaveAttribute('target', '_blank');
      expect(docLink).toHaveAttribute('rel', 'noopener noreferrer');
    });
  });

  // Additional tests for section structure and accessibility
  describe('Section structure and accessibility', () => {
    it('should render a section with id="resources"', () => {
      render(<Resources />);

      const section = document.getElementById('resources');
      expect(section).toBeInTheDocument();
    });

    it('should have proper heading structure', () => {
      render(<Resources />);

      expect(
        screen.getByRole('heading', { name: 'Resources', level: 2 })
      ).toBeInTheDocument();
    });

    it('should have proper aria-labelledby on the section', () => {
      render(<Resources />);

      const section = document.getElementById('resources');
      expect(section).toHaveAttribute('aria-labelledby', 'resources-heading');
    });

    it('should render icons for each resource card', () => {
      render(<Resources />);

      const resourceCards = screen.getAllByTestId(/^resource-card-/);
      resourceCards.forEach((card) => {
        // Each card should have an SVG icon
        const svgIcon = card.querySelector('svg');
        expect(svgIcon).toBeInTheDocument();
      });
    });

    it('should have icons marked as aria-hidden', () => {
      render(<Resources />);

      const resourceCards = screen.getAllByTestId(/^resource-card-/);
      resourceCards.forEach((card) => {
        const svgIcon = card.querySelector('svg');
        expect(svgIcon).toHaveAttribute('aria-hidden', 'true');
      });
    });

    it('should have screen reader text indicating links open in new tab', () => {
      render(<Resources />);

      const srTexts = screen.getAllByText(/opens in new tab/);
      expect(srTexts).toHaveLength(6);
    });
  });

  // Test all resource links have valid hrefs
  describe('All resource links have valid URLs', () => {
    it('should have API Reference link pointing to docs/api.md', () => {
      render(<Resources />);

      const apiLink = screen.getByTestId('resource-link-api-reference');
      expect(apiLink.getAttribute('href')).toContain('docs/api.md');
    });

    it('should have Examples link pointing to examples directory', () => {
      render(<Resources />);

      const examplesLink = screen.getByTestId('resource-link-examples');
      expect(examplesLink.getAttribute('href')).toContain('examples');
    });

    it('should have Issue Tracker link pointing to issues', () => {
      render(<Resources />);

      const issuesLink = screen.getByTestId('resource-link-issue-tracker');
      expect(issuesLink.getAttribute('href')).toContain('issues');
    });

    it('should have License link pointing to LICENSE file', () => {
      render(<Resources />);

      const licenseLink = screen.getByTestId('resource-link-license');
      expect(licenseLink.getAttribute('href')).toContain('LICENSE');
    });
  });
});
