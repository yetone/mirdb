/**
 * Unit tests for ProjectStatus component.
 * Owner: Scenario 4 - Project Status Section
 */

import React from 'react';
import { render, screen } from '@testing-library/react';
import { ProjectStatus } from '@/components/sections/ProjectStatus';

describe('ProjectStatus Component', () => {
  // Test Case 1: List of implemented features displayed with checkmark icons
  describe('TC1: Renders implemented features with checkmark icons', () => {
    it('should render all implemented features', () => {
      render(<ProjectStatus />);

      expect(screen.getByText('Async networking with Tokio')).toBeInTheDocument();
      expect(screen.getByText('Memtable with skip list')).toBeInTheDocument();
      expect(screen.getByText('Minor compaction')).toBeInTheDocument();
      expect(screen.getByText('Major compaction')).toBeInTheDocument();
    });

    it('should render implemented features with checkmark icons', () => {
      render(<ProjectStatus />);

      const implementedFeaturesCard = screen.getByTestId('implemented-features-card');
      const checkIcons = implementedFeaturesCard.querySelectorAll('svg');

      // One icon in the header + 4 icons for each feature
      expect(checkIcons.length).toBe(5);
    });

    it('should have screen reader text indicating implemented status', () => {
      render(<ProjectStatus />);

      const implementedLabels = screen.getAllByText('(implemented)');
      expect(implementedLabels.length).toBe(4);

      // Check that they are sr-only (screen reader only)
      implementedLabels.forEach((label) => {
        expect(label).toHaveClass('sr-only');
      });
    });

    it('should render four implemented feature items', () => {
      render(<ProjectStatus />);

      const implementedFeatures = screen.getAllByTestId(/^implemented-feature-/);
      expect(implementedFeatures).toHaveLength(4);
    });
  });

  // Test Case 2: Planned feature 'Raft consensus' displayed with 'Coming Soon' badge
  describe('TC2: Renders planned feature with Coming Soon badge', () => {
    it('should render Raft consensus as a planned feature', () => {
      render(<ProjectStatus />);

      expect(screen.getByText('Raft consensus')).toBeInTheDocument();
    });

    it('should display Coming Soon badge for planned features', () => {
      render(<ProjectStatus />);

      const comingSoonBadge = screen.getByTestId('coming-soon-badge');
      expect(comingSoonBadge).toBeInTheDocument();
      expect(comingSoonBadge).toHaveTextContent('Coming Soon');
    });

    it('should render planned features in a separate card', () => {
      render(<ProjectStatus />);

      const plannedFeaturesCard = screen.getByTestId('planned-features-card');
      expect(plannedFeaturesCard).toBeInTheDocument();

      // Check that Raft consensus is within the planned features card
      expect(plannedFeaturesCard).toHaveTextContent('Raft consensus');
    });

    it('should display clock icon for planned features', () => {
      render(<ProjectStatus />);

      const plannedFeaturesCard = screen.getByTestId('planned-features-card');
      const clockIcons = plannedFeaturesCard.querySelectorAll('svg');

      // One icon in the header + 1 icon for the planned feature
      expect(clockIcons.length).toBe(2);
    });
  });

  // Test Case 3: CircleCI badge image is displayed with link to CI pipeline
  describe('TC3: Renders CircleCI badge with link to CI pipeline', () => {
    it('should render CircleCI badge image', () => {
      render(<ProjectStatus />);

      const badgeImage = screen.getByTestId('ci-badge-image');
      expect(badgeImage).toBeInTheDocument();
      expect(badgeImage.tagName).toBe('IMG');
      expect(badgeImage).toHaveAttribute('src', 'https://circleci.com/gh/yetone/mirdb.svg?style=svg');
    });

    it('should render CI badge with link to CircleCI pipeline', () => {
      render(<ProjectStatus />);

      const badgeLink = screen.getByTestId('ci-badge-link');
      expect(badgeLink).toBeInTheDocument();
      expect(badgeLink).toHaveAttribute('href', 'https://circleci.com/gh/yetone/mirdb');
    });

    it('should have proper alt text for accessibility', () => {
      render(<ProjectStatus />);

      const badgeImage = screen.getByTestId('ci-badge-image');
      expect(badgeImage).toHaveAttribute('alt', 'CircleCI Build Status');
    });

    it('should have aria-label for the CI badge link', () => {
      render(<ProjectStatus />);

      const badgeLink = screen.getByTestId('ci-badge-link');
      expect(badgeLink).toHaveAttribute('aria-label', 'View CircleCI build status (opens in new tab)');
    });

    it('should open CI badge link in new tab with security attributes', () => {
      render(<ProjectStatus />);

      const badgeLink = screen.getByTestId('ci-badge-link');
      expect(badgeLink).toHaveAttribute('target', '_blank');
      expect(badgeLink).toHaveAttribute('rel', 'noopener noreferrer');
    });
  });

  // Section structure and accessibility
  describe('Section structure and accessibility', () => {
    it('should render a section with id="status"', () => {
      render(<ProjectStatus />);

      const section = document.getElementById('status');
      expect(section).toBeInTheDocument();
    });

    it('should have proper heading structure', () => {
      render(<ProjectStatus />);

      expect(
        screen.getByRole('heading', { name: 'Project Status', level: 2 })
      ).toBeInTheDocument();
    });

    it('should have proper aria-labelledby on the section', () => {
      render(<ProjectStatus />);

      const section = document.getElementById('status');
      expect(section).toHaveAttribute('aria-labelledby', 'status-heading');
    });

    it('should have accessible lists for implemented and planned features', () => {
      render(<ProjectStatus />);

      const implementedList = screen.getByRole('list', { name: 'List of implemented features' });
      const plannedList = screen.getByRole('list', { name: 'List of planned features' });

      expect(implementedList).toBeInTheDocument();
      expect(plannedList).toBeInTheDocument();
    });
  });

  // Visual distinction between implemented and planned features
  describe('Visual distinction between implemented and planned features', () => {
    it('should render implemented features with green checkmarks', () => {
      render(<ProjectStatus />);

      const implementedFeaturesCard = screen.getByTestId('implemented-features-card');
      const checkIcons = implementedFeaturesCard.querySelectorAll('svg');

      // Check that icons have green styling
      checkIcons.forEach((icon) => {
        expect(icon.closest('svg')?.classList.contains('text-green-500') ||
               icon.parentElement?.querySelector('.text-green-500')).toBeTruthy();
      });
    });

    it('should render Coming Soon badge with amber styling', () => {
      render(<ProjectStatus />);

      const comingSoonBadge = screen.getByTestId('coming-soon-badge');
      expect(comingSoonBadge).toHaveClass('bg-amber-100', 'text-amber-800');
    });
  });
});
