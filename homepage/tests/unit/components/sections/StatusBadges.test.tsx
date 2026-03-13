/**
 * Status Badges Section Component Tests
 * Owner: Scenario 6 - Status Badges Display
 *
 * Tests for REQ-6: The homepage shall display the current status with badges for CI/CD build status.
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { StatusBadges } from '../../../../src/components/sections/StatusBadges';

describe('StatusBadges Section', () => {
  describe('Test Case 1: CircleCI badge image is rendered', () => {
    it('renders the CircleCI badge image', () => {
      render(<StatusBadges />);

      const badgeImage = screen.getByRole('img');
      expect(badgeImage).toBeInTheDocument();
      expect(badgeImage).toHaveAttribute(
        'src',
        expect.stringContaining('circleci.com')
      );
    });

    it('renders badge with shield style', () => {
      render(<StatusBadges />);

      const badgeImage = screen.getByRole('img');
      expect(badgeImage).toHaveAttribute(
        'src',
        expect.stringContaining('style=shield')
      );
    });
  });

  describe('Test Case 2: Badge has proper alt text for accessibility', () => {
    it('has descriptive alt text for the badge image', () => {
      render(<StatusBadges />);

      const badgeImage = screen.getByRole('img');
      expect(badgeImage).toHaveAttribute('alt');
      const altText = badgeImage.getAttribute('alt');
      expect(altText).toBeTruthy();
      expect(altText!.toLowerCase()).toContain('circleci');
    });

    it('alt text describes the badge purpose', () => {
      render(<StatusBadges />);

      const badgeImage = screen.getByAltText(/circleci/i);
      expect(badgeImage).toBeInTheDocument();
    });
  });

  describe('Test Case 3: Badge is wrapped in a link to CircleCI dashboard', () => {
    it('badge image is inside a link element', () => {
      render(<StatusBadges />);

      const link = screen.getByRole('link');
      expect(link).toBeInTheDocument();

      const badgeImage = screen.getByRole('img');
      expect(link).toContainElement(badgeImage);
    });

    it('link points to CircleCI dashboard', () => {
      render(<StatusBadges />);

      const link = screen.getByRole('link');
      expect(link).toHaveAttribute(
        'href',
        'https://circleci.com/gh/yetone/mirdb'
      );
    });
  });

  describe('Test Case 4: Link opens in new tab with noopener noreferrer', () => {
    it('link has target="_blank" attribute', () => {
      render(<StatusBadges />);

      const link = screen.getByRole('link');
      expect(link).toHaveAttribute('target', '_blank');
    });

    it('link has rel="noopener noreferrer" for security', () => {
      render(<StatusBadges />);

      const link = screen.getByRole('link');
      expect(link).toHaveAttribute('rel', 'noopener noreferrer');
    });

    it('link has both security attributes set correctly', () => {
      render(<StatusBadges />);

      const link = screen.getByRole('link');
      const relAttr = link.getAttribute('rel');
      expect(relAttr).toContain('noopener');
      expect(relAttr).toContain('noreferrer');
    });
  });

  describe('Component structure', () => {
    it('renders within a section element', () => {
      render(<StatusBadges />);

      const section = screen.getByTestId('status-badges');
      expect(section.tagName.toLowerCase()).toBe('section');
    });

    it('has a heading for the section', () => {
      render(<StatusBadges />);

      const heading = screen.getByRole('heading');
      expect(heading).toBeInTheDocument();
    });
  });
});
