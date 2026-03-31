/**
 * Status Section Component Tests
 * Owner: Scenario 5 - Status and Roadmap Display
 *
 * Tests for verifying:
 * - Implemented features display with checkmarks
 * - Planned features display with distinct indicators
 * - Visual progress indicator
 * - Timeline/roadmap visualization
 */

import { describe, it, expect } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { StatusSection } from '../../../src/components/sections/StatusSection';
import type { RoadmapItem } from '../../../src/types';

const mockRoadmapItems: RoadmapItem[] = [
  {
    id: 'memcached',
    title: 'Memcached Protocol',
    description: 'Full compatibility with the Memcached text protocol.',
    status: 'completed',
    quarter: 'Q1 2024',
  },
  {
    id: 'persistence',
    title: 'Persistence & SSTable',
    description: 'Durable storage using LSM-tree architecture.',
    status: 'completed',
    quarter: 'Q2 2024',
  },
  {
    id: 'compaction',
    title: 'Compaction',
    description: 'Automatic background compaction.',
    status: 'completed',
    quarter: 'Q3 2024',
  },
  {
    id: 'raft',
    title: 'Raft Consensus',
    description: 'Distributed consensus for high availability.',
    status: 'planned',
    quarter: 'Q1 2025',
  },
];

describe('StatusSection', () => {
  describe('Test Case 1: Section displays list of implemented features with checkmarks', () => {
    it('renders the status section with implemented features list', () => {
      render(<StatusSection items={mockRoadmapItems} />);

      // Check section is rendered
      const section = screen.getByTestId('status-section');
      expect(section).toBeInTheDocument();

      // Check heading is present
      expect(screen.getByText('Project Status & Roadmap')).toBeInTheDocument();

      // Check implemented features list exists
      const implementedList = screen.getByTestId('implemented-features-list');
      expect(implementedList).toBeInTheDocument();

      // Verify implemented features have checkmarks (3 implemented features)
      const implementedItems = within(implementedList).getAllByRole('listitem');
      expect(implementedItems.length).toBe(3);
    });
  });

  describe('Test Case 2: Memcached protocol feature shows as implemented', () => {
    it('displays Memcached Protocol with implemented status', () => {
      render(<StatusSection items={mockRoadmapItems} />);

      // Check Memcached Protocol is in the implemented list
      const implementedMemcached = screen.getByTestId('implemented-memcached');
      expect(implementedMemcached).toBeInTheDocument();
      expect(implementedMemcached).toHaveTextContent('Memcached Protocol');

      // Check the badge shows "Implemented"
      const statusBadge = screen.getByTestId('status-memcached');
      expect(statusBadge).toHaveTextContent('Implemented');
    });
  });

  describe('Test Case 3: Persistence/SSTable feature shows as implemented', () => {
    it('displays Persistence & SSTable with implemented status', () => {
      render(<StatusSection items={mockRoadmapItems} />);

      // Check Persistence is in the implemented list
      const implementedPersistence = screen.getByTestId('implemented-persistence');
      expect(implementedPersistence).toBeInTheDocument();
      expect(implementedPersistence).toHaveTextContent('Persistence & SSTable');

      // Check the badge shows "Implemented"
      const statusBadge = screen.getByTestId('status-persistence');
      expect(statusBadge).toHaveTextContent('Implemented');
    });
  });

  describe('Test Case 4: Compaction features show as implemented', () => {
    it('displays Compaction with implemented status', () => {
      render(<StatusSection items={mockRoadmapItems} />);

      // Check Compaction is in the implemented list
      const implementedCompaction = screen.getByTestId('implemented-compaction');
      expect(implementedCompaction).toBeInTheDocument();
      expect(implementedCompaction).toHaveTextContent('Compaction');

      // Check the badge shows "Implemented"
      const statusBadge = screen.getByTestId('status-compaction');
      expect(statusBadge).toHaveTextContent('Implemented');
    });
  });

  describe('Test Case 5: Raft consensus feature shows as planned/upcoming', () => {
    it('displays Raft Consensus with planned status', () => {
      render(<StatusSection items={mockRoadmapItems} />);

      // Check Raft is in the planned list
      const plannedRaft = screen.getByTestId('planned-raft');
      expect(plannedRaft).toBeInTheDocument();
      expect(plannedRaft).toHaveTextContent('Raft Consensus');

      // Check the badge shows "Planned"
      const statusBadge = screen.getByTestId('status-raft');
      expect(statusBadge).toHaveTextContent('Planned');
    });

    it('Raft feature is NOT in the implemented list', () => {
      render(<StatusSection items={mockRoadmapItems} />);

      const implementedList = screen.getByTestId('implemented-features-list');
      expect(within(implementedList).queryByText('Raft Consensus')).not.toBeInTheDocument();
    });
  });

  describe('Test Case 6: Visual progress indicator is present', () => {
    it('renders progress indicator with correct values', () => {
      render(<StatusSection items={mockRoadmapItems} />);

      // Check progress indicator exists
      const progressIndicator = screen.getByTestId('progress-indicator');
      expect(progressIndicator).toBeInTheDocument();

      // Check progress bar exists
      const progressBarFill = screen.getByTestId('progress-bar-fill');
      expect(progressBarFill).toBeInTheDocument();

      // Check progress percentage (3 of 4 = 75%)
      const progressPercent = screen.getByTestId('progress-percent');
      expect(progressPercent).toHaveTextContent('75%');

      // Check progress bar has accessible role
      const progressBar = screen.getByRole('progressbar');
      expect(progressBar).toHaveAttribute('aria-valuenow', '75');
    });

    it('renders roadmap timeline', () => {
      render(<StatusSection items={mockRoadmapItems} />);

      // Check timeline exists
      const timeline = screen.getByTestId('roadmap-timeline');
      expect(timeline).toBeInTheDocument();

      // Check all roadmap items are in the timeline
      expect(screen.getByTestId('roadmap-item-memcached')).toBeInTheDocument();
      expect(screen.getByTestId('roadmap-item-persistence')).toBeInTheDocument();
      expect(screen.getByTestId('roadmap-item-compaction')).toBeInTheDocument();
      expect(screen.getByTestId('roadmap-item-raft')).toBeInTheDocument();
    });

    it('renders timeline icons with correct colors', () => {
      render(<StatusSection items={mockRoadmapItems} />);

      // Check implemented items have green icons
      const memcachedIcon = screen.getByTestId('timeline-icon-memcached');
      expect(memcachedIcon).toHaveClass('bg-green-500');

      // Check planned items have yellow icons
      const raftIcon = screen.getByTestId('timeline-icon-raft');
      expect(raftIcon).toHaveClass('bg-yellow-500');
    });
  });

  describe('Test Case 7: Implemented and planned features are visually distinct', () => {
    it('implemented features have different visual styling from planned', () => {
      render(<StatusSection items={mockRoadmapItems} />);

      // Get implemented badge
      const implementedBadge = screen.getByTestId('status-memcached');
      // Get planned badge
      const plannedBadge = screen.getByTestId('status-raft');

      // Badges should have different text
      expect(implementedBadge).toHaveTextContent('Implemented');
      expect(plannedBadge).toHaveTextContent('Planned');
    });

    it('feature checklist separates implemented and planned features', () => {
      render(<StatusSection items={mockRoadmapItems} />);

      const checklist = screen.getByTestId('feature-checklist');
      expect(checklist).toBeInTheDocument();

      // Check both lists exist
      const implementedList = screen.getByTestId('implemented-features-list');
      const plannedList = screen.getByTestId('planned-features-list');

      expect(implementedList).toBeInTheDocument();
      expect(plannedList).toBeInTheDocument();

      // Verify separation - implemented has 3, planned has 1
      expect(within(implementedList).getAllByRole('listitem').length).toBe(3);
      expect(within(plannedList).getAllByRole('listitem').length).toBe(1);
    });

    it('implemented features section has checkmark heading', () => {
      render(<StatusSection items={mockRoadmapItems} />);

      expect(screen.getByText('Implemented Features')).toBeInTheDocument();
    });

    it('planned features section has target/upcoming heading', () => {
      render(<StatusSection items={mockRoadmapItems} />);

      expect(screen.getByText('Upcoming Features')).toBeInTheDocument();
    });
  });

  describe('Default items rendering', () => {
    it('renders default roadmap items when no props are provided', () => {
      render(<StatusSection />);

      // Check section is rendered
      expect(screen.getByTestId('status-section')).toBeInTheDocument();

      // Check default items are rendered (with default data) - using getAllByText since
      // items appear in both the checklist and timeline
      expect(screen.getAllByText('Memcached Protocol').length).toBeGreaterThan(0);
      expect(screen.getAllByText('Persistence & SSTable').length).toBeGreaterThan(0);
      expect(screen.getAllByText('Compaction').length).toBeGreaterThan(0);
      expect(screen.getAllByText('Raft Consensus').length).toBeGreaterThan(0);
    });
  });

  describe('Accessibility', () => {
    it('has proper aria labels and semantic structure', () => {
      render(<StatusSection items={mockRoadmapItems} />);

      // Check section has aria-labelledby
      const section = screen.getByTestId('status-section');
      expect(section).toHaveAttribute('aria-labelledby', 'status-heading');

      // Check progress bar has aria attributes
      const progressBar = screen.getByRole('progressbar');
      expect(progressBar).toHaveAttribute('aria-valuemin', '0');
      expect(progressBar).toHaveAttribute('aria-valuemax', '100');
      expect(progressBar).toHaveAttribute('aria-label');
    });

    it('has proper heading hierarchy', () => {
      render(<StatusSection items={mockRoadmapItems} />);

      // Main heading
      const mainHeading = screen.getByRole('heading', { level: 2, name: 'Project Status & Roadmap' });
      expect(mainHeading).toBeInTheDocument();

      // Subheadings
      expect(screen.getByRole('heading', { level: 3, name: 'Implemented Features' })).toBeInTheDocument();
      expect(screen.getByRole('heading', { level: 3, name: 'Upcoming Features' })).toBeInTheDocument();
      expect(screen.getByRole('heading', { level: 3, name: 'Development Timeline' })).toBeInTheDocument();
    });
  });
});
