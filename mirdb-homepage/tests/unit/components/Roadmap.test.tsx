/**
 * Unit tests for Roadmap section component.
 * Owner: Scenario 8 - Roadmap Section
 *
 * Tests cover:
 * - Rendering roadmap section with completed and planned features
 * - Verifying completed features show checkmarks (Tokio networking, memtable, minor/major compaction)
 * - Verifying planned features show Raft consensus
 * - Visual distinction between completed and planned features
 */

import { describe, it, expect } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { Roadmap } from '../../../src/components/Roadmap/Roadmap';
import roadmapData from '../../../src/content/roadmap.json';
import type { RoadmapItem } from '../../../src/types/index';

const roadmapItems = roadmapData as RoadmapItem[];

describe('Roadmap', () => {
  describe('Test Case 1: Render Roadmap component', () => {
    it('renders roadmap section with completed and planned features', () => {
      render(<Roadmap />);

      // Check that the roadmap section is rendered
      const section = screen.getByTestId('roadmap-section');
      expect(section).toBeInTheDocument();

      // Check section heading
      expect(screen.getByRole('heading', { level: 2 })).toHaveTextContent('Roadmap');

      // Check that completed features section exists
      const completedSection = screen.getByTestId('completed-features');
      expect(completedSection).toBeInTheDocument();

      // Check that planned features section exists
      const plannedSection = screen.getByTestId('planned-features');
      expect(plannedSection).toBeInTheDocument();
    });

    it('renders all roadmap items from data', () => {
      render(<Roadmap />);

      // Check all completed items are rendered
      const completedItems = screen.getAllByTestId('roadmap-item-completed');
      const expectedCompletedCount = roadmapItems.filter(item => item.status === 'completed').length;
      expect(completedItems).toHaveLength(expectedCompletedCount);

      // Check all planned items are rendered
      const plannedItems = screen.getAllByTestId('roadmap-item-planned');
      const expectedPlannedCount = roadmapItems.filter(item => item.status === 'planned').length;
      expect(plannedItems).toHaveLength(expectedPlannedCount);
    });
  });

  describe('Test Case 2: Check completed features list', () => {
    it('shows checkmarks for: Tokio networking, memtable, minor compaction, major compaction', () => {
      render(<Roadmap />);

      const completedSection = screen.getByTestId('completed-features');
      const completedItems = within(completedSection).getAllByTestId('roadmap-item-completed');

      // Verify we have 4 completed items
      expect(completedItems).toHaveLength(4);

      // Check for Tokio networking
      expect(screen.getByText('Tokio networking')).toBeInTheDocument();

      // Check for Memtable implementation
      expect(screen.getByText('Memtable implementation')).toBeInTheDocument();

      // Check for Minor compaction
      expect(screen.getByText('Minor compaction')).toBeInTheDocument();

      // Check for Major compaction
      expect(screen.getByText('Major compaction')).toBeInTheDocument();

      // Each completed item should have a checkmark icon (completed-icon)
      const checkmarkIcons = within(completedSection).getAllByTestId('completed-icon');
      expect(checkmarkIcons).toHaveLength(4);
    });

    it('displays descriptions for each completed feature', () => {
      render(<Roadmap />);

      // Check Tokio networking description
      expect(screen.getByText(/Async I\/O networking layer built on Tokio runtime/i)).toBeInTheDocument();

      // Check Memtable description
      expect(screen.getByText(/In-memory skip-list based memtable/i)).toBeInTheDocument();

      // Check Minor compaction description
      expect(screen.getByText(/Automatic flush of immutable memtables to SSTable files/i)).toBeInTheDocument();

      // Check Major compaction description
      expect(screen.getByText(/Background merge of SSTables across levels/i)).toBeInTheDocument();
    });
  });

  describe('Test Case 3: Check planned features list', () => {
    it('shows Raft consensus as planned/upcoming feature', () => {
      render(<Roadmap />);

      const plannedSection = screen.getByTestId('planned-features');
      const plannedItems = within(plannedSection).getAllByTestId('roadmap-item-planned');

      // Verify we have 1 planned item
      expect(plannedItems).toHaveLength(1);

      // Check for Raft consensus title
      expect(screen.getByText('Raft consensus')).toBeInTheDocument();

      // Check for Raft consensus description
      expect(screen.getByText(/Distributed consensus protocol for high availability/i)).toBeInTheDocument();
    });

    it('displays planned icon for Raft consensus', () => {
      render(<Roadmap />);

      const plannedSection = screen.getByTestId('planned-features');
      const plannedIcons = within(plannedSection).getAllByTestId('planned-icon');
      expect(plannedIcons).toHaveLength(1);
    });
  });

  describe('Test Case 4: Verify visual distinction', () => {
    it('completed features are visually distinct from planned features', () => {
      render(<Roadmap />);

      // Completed section has "Completed" heading with success color
      const completedHeading = screen.getByText('Completed');
      expect(completedHeading).toBeInTheDocument();
      expect(completedHeading).toHaveClass('text-success');

      // Planned section has "Planned" heading with warning color
      const plannedHeading = screen.getByText('Planned');
      expect(plannedHeading).toBeInTheDocument();
      expect(plannedHeading).toHaveClass('text-warning');
    });

    it('completed items have solid border while planned items have dashed border', () => {
      render(<Roadmap />);

      // Check completed items have solid border (default, no border-dashed class)
      const completedItems = screen.getAllByTestId('roadmap-item-completed');
      completedItems.forEach(item => {
        expect(item).not.toHaveClass('border-dashed');
      });

      // Check planned items have dashed border
      const plannedItems = screen.getAllByTestId('roadmap-item-planned');
      plannedItems.forEach(item => {
        expect(item).toHaveClass('border-dashed');
      });
    });

    it('completed items use checkmark icon while planned items use clock icon', () => {
      render(<Roadmap />);

      const completedSection = screen.getByTestId('completed-features');
      const plannedSection = screen.getByTestId('planned-features');

      // Completed items should have checkmark icons (completed-icon)
      const completedIcons = within(completedSection).getAllByTestId('completed-icon');
      expect(completedIcons.length).toBeGreaterThan(0);

      // Planned items should have planned/clock icons (planned-icon)
      const plannedIcons = within(plannedSection).getAllByTestId('planned-icon');
      expect(plannedIcons.length).toBeGreaterThan(0);
    });

    it('completed items icons are success color and planned items icons are warning color', () => {
      render(<Roadmap />);

      // Check completed items have success colored icons
      const completedItems = screen.getAllByTestId('roadmap-item-completed');
      completedItems.forEach(item => {
        const iconWrapper = item.querySelector('[aria-hidden="true"]');
        expect(iconWrapper).toHaveClass('text-success');
      });

      // Check planned items have warning colored icons
      const plannedItems = screen.getAllByTestId('roadmap-item-planned');
      plannedItems.forEach(item => {
        const iconWrapper = item.querySelector('[aria-hidden="true"]');
        expect(iconWrapper).toHaveClass('text-warning');
      });
    });
  });

  describe('Accessibility', () => {
    it('has accessible list structure with aria labels', () => {
      render(<Roadmap />);

      // Check for completed features list with aria-label
      const completedList = screen.getByRole('list', { name: 'Completed features' });
      expect(completedList).toBeInTheDocument();

      // Check for planned features list with aria-label
      const plannedList = screen.getByRole('list', { name: 'Planned features' });
      expect(plannedList).toBeInTheDocument();
    });

    it('has proper heading hierarchy', () => {
      render(<Roadmap />);

      // Main section heading should be h2
      const mainHeading = screen.getByRole('heading', { level: 2 });
      expect(mainHeading).toHaveTextContent('Roadmap');

      // Sub-section headings should be h3
      const subHeadings = screen.getAllByRole('heading', { level: 3 });
      expect(subHeadings).toHaveLength(2);
      expect(subHeadings[0]).toHaveTextContent('Completed');
      expect(subHeadings[1]).toHaveTextContent('Planned');
    });

    it('icons are hidden from assistive technology', () => {
      render(<Roadmap />);

      // All SVG icons within items should be aria-hidden
      const completedItems = screen.getAllByTestId('roadmap-item-completed');
      completedItems.forEach(item => {
        const iconWrapper = item.querySelector('[aria-hidden="true"]');
        expect(iconWrapper).toBeInTheDocument();
      });

      const plannedItems = screen.getAllByTestId('roadmap-item-planned');
      plannedItems.forEach(item => {
        const iconWrapper = item.querySelector('[aria-hidden="true"]');
        expect(iconWrapper).toBeInTheDocument();
      });
    });
  });

  describe('Custom items prop', () => {
    it('renders custom roadmap items when provided', () => {
      const customItems: RoadmapItem[] = [
        {
          title: 'Custom Completed Feature',
          description: 'A custom completed feature',
          status: 'completed',
        },
        {
          title: 'Custom Planned Feature',
          description: 'A custom planned feature',
          status: 'planned',
        },
      ];

      render(<Roadmap items={customItems} />);

      expect(screen.getByText('Custom Completed Feature')).toBeInTheDocument();
      expect(screen.getByText('Custom Planned Feature')).toBeInTheDocument();

      const completedItems = screen.getAllByTestId('roadmap-item-completed');
      expect(completedItems).toHaveLength(1);

      const plannedItems = screen.getAllByTestId('roadmap-item-planned');
      expect(plannedItems).toHaveLength(1);
    });
  });
});
