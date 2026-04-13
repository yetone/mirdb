/**
 * Roadmap Section Unit Tests
 * Owner: Scenario 7 - Planned Features Section
 *
 * Tests:
 * - RoadmapItem renders different states for completed vs planned
 * - RoadmapItem renders status indicator (checkmark for completed, circle for planned)
 * - Roadmap section renders completed and planned features separately
 * - Visual distinction between completed and planned items
 */
import { describe, it, expect } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { RoadmapItem } from './RoadmapItem';
import { Roadmap } from './Roadmap';
import type { RoadmapItem as RoadmapItemType } from '../../types';

describe('RoadmapItem', () => {
  it('renders completed item with checkmark indicator', () => {
    const completedItem: RoadmapItemType = {
      title: 'Tokio-based async networking',
      status: 'completed',
    };

    render(<RoadmapItem item={completedItem} />);

    const item = screen.getByTestId('roadmap-item');
    expect(item).toHaveAttribute('data-status', 'completed');
    expect(item).toHaveClass('roadmap-item--completed');

    // Check for completed badge
    expect(screen.getByText('Completed')).toBeInTheDocument();
    expect(screen.getByText('Tokio-based async networking')).toBeInTheDocument();
  });

  it('renders planned item with planned indicator', () => {
    const plannedItem: RoadmapItemType = {
      title: 'Raft consensus',
      status: 'planned',
      description: 'Distributed operation support',
    };

    render(<RoadmapItem item={plannedItem} />);

    const item = screen.getByTestId('roadmap-item');
    expect(item).toHaveAttribute('data-status', 'planned');
    expect(item).toHaveClass('roadmap-item--planned');

    // Check for planned badge
    expect(screen.getByText('Planned')).toBeInTheDocument();
    expect(screen.getByText('Raft consensus')).toBeInTheDocument();
    expect(screen.getByText('Distributed operation support')).toBeInTheDocument();
  });

  it('renders different status indicators for completed vs planned', () => {
    const completedItem: RoadmapItemType = {
      title: 'Completed Feature',
      status: 'completed',
    };

    const { rerender, container } = render(<RoadmapItem item={completedItem} />);

    // Completed should have check icon
    const completedIndicator = container.querySelector('.roadmap-status-indicator--completed');
    expect(completedIndicator).toBeInTheDocument();
    expect(container.querySelector('.roadmap-icon--check')).toBeInTheDocument();

    // Rerender with planned item
    const plannedItem: RoadmapItemType = {
      title: 'Planned Feature',
      status: 'planned',
    };

    rerender(<RoadmapItem item={plannedItem} />);

    // Planned should have circle icon
    const plannedIndicator = container.querySelector('.roadmap-status-indicator--planned');
    expect(plannedIndicator).toBeInTheDocument();
    expect(container.querySelector('.roadmap-icon--planned')).toBeInTheDocument();
  });

  it('renders optional description when provided', () => {
    const itemWithDescription: RoadmapItemType = {
      title: 'Feature with Description',
      status: 'planned',
      description: 'This is a detailed description',
    };

    render(<RoadmapItem item={itemWithDescription} />);

    expect(screen.getByText('This is a detailed description')).toBeInTheDocument();
  });

  it('does not render description when not provided', () => {
    const itemWithoutDescription: RoadmapItemType = {
      title: 'Feature without Description',
      status: 'completed',
    };

    const { container } = render(<RoadmapItem item={itemWithoutDescription} />);

    const description = container.querySelector('.roadmap-item-description');
    expect(description).toBeNull();
  });
});

describe('Roadmap', () => {
  it('renders the roadmap section with heading', () => {
    render(<Roadmap />);

    expect(screen.getByRole('heading', { name: /project status/i })).toBeInTheDocument();
    expect(screen.getByText(/track our progress/i)).toBeInTheDocument();
  });

  it('renders completed features in separate column', () => {
    render(<Roadmap />);

    const completedSection = screen.getByTestId('completed-features');
    expect(completedSection).toBeInTheDocument();

    // Should contain completed items
    const completedItems = within(completedSection).getAllByTestId('roadmap-item');
    expect(completedItems.length).toBeGreaterThan(0);

    // All items in completed section should be completed
    completedItems.forEach(item => {
      expect(item).toHaveAttribute('data-status', 'completed');
    });
  });

  it('renders planned features in separate column', () => {
    render(<Roadmap />);

    const plannedSection = screen.getByTestId('planned-features');
    expect(plannedSection).toBeInTheDocument();

    // Should contain planned items
    const plannedItems = within(plannedSection).getAllByTestId('roadmap-item');
    expect(plannedItems.length).toBeGreaterThan(0);

    // All items in planned section should be planned
    plannedItems.forEach(item => {
      expect(item).toHaveAttribute('data-status', 'planned');
    });
  });

  it('displays Raft consensus as a planned feature', () => {
    render(<Roadmap />);

    const plannedSection = screen.getByTestId('planned-features');
    expect(within(plannedSection).getByText(/Raft consensus/i)).toBeInTheDocument();
  });

  it('displays completed features with appropriate indicators', () => {
    render(<Roadmap />);

    const completedSection = screen.getByTestId('completed-features');

    // Check for specific completed features
    expect(within(completedSection).getByText(/Tokio-based async networking/i)).toBeInTheDocument();
    expect(within(completedSection).getByText(/Memtable with skip list/i)).toBeInTheDocument();
    expect(within(completedSection).getByText(/Minor compaction/i)).toBeInTheDocument();
    expect(within(completedSection).getByText(/Major compaction/i)).toBeInTheDocument();
  });

  it('has visual distinction between completed and planned columns', () => {
    const { container } = render(<Roadmap />);

    const completedColumn = container.querySelector('.roadmap-column--completed');
    const plannedColumn = container.querySelector('.roadmap-column--planned');

    expect(completedColumn).toBeInTheDocument();
    expect(plannedColumn).toBeInTheDocument();

    // Both columns should exist and be distinct
    expect(completedColumn).not.toBe(plannedColumn);
  });

  it('has proper accessibility attributes', () => {
    const { container } = render(<Roadmap />);

    const section = container.querySelector('#roadmap');
    expect(section).toHaveAttribute('aria-labelledby', 'roadmap-heading');

    // Check for list elements
    const lists = screen.getAllByRole('list');
    expect(lists.length).toBeGreaterThanOrEqual(2);
  });

  it('renders column headings for completed and planned', () => {
    render(<Roadmap />);

    expect(screen.getByRole('heading', { name: /completed/i })).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: /planned/i })).toBeInTheDocument();
  });
});
