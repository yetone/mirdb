import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { Roadmap } from '../../../../src/components/sections/Roadmap';
import { roadmapItems } from '../../../../src/data/roadmap';

describe('Roadmap Component', () => {
  // Test Case 1: Component renders with 'Roadmap' or 'Status' heading
  it('renders with section heading "Roadmap"', () => {
    render(<Roadmap />);
    const heading = screen.getByRole('heading', { level: 2, name: /roadmap/i });
    expect(heading).toBeInTheDocument();
    expect(heading).toHaveTextContent('Roadmap');
  });

  // Test Case 2: Query for 'tokio' item - Tokio is listed and marked as completed
  it('displays Tokio item marked as completed', () => {
    render(<Roadmap />);
    const tokioItem = screen.getByRole('heading', { level: 3, name: /tokio/i });
    expect(tokioItem).toBeInTheDocument();

    // Find the parent roadmap item
    const roadmapItem = tokioItem.closest('[data-testid="roadmap-item"]');
    expect(roadmapItem).toBeInTheDocument();

    // Check for completed indicator
    const completedIndicator = roadmapItem?.querySelector('[data-testid="completed-indicator"]');
    expect(completedIndicator).toBeInTheDocument();

    // Check for checkmark icon
    const checkmark = roadmapItem?.querySelector('[data-testid="checkmark-icon"]');
    expect(checkmark).toBeInTheDocument();

    // Check for Completed badge
    const badge = roadmapItem?.querySelector('[data-testid="status-badge"]');
    expect(badge).toHaveTextContent('Completed');
  });

  // Test Case 3: Query for 'memtable' item - Memtable is listed and marked as completed
  it('displays Memtable item marked as completed', () => {
    render(<Roadmap />);
    const memtableItem = screen.getByRole('heading', { level: 3, name: /memtable/i });
    expect(memtableItem).toBeInTheDocument();

    const roadmapItem = memtableItem.closest('[data-testid="roadmap-item"]');
    expect(roadmapItem).toBeInTheDocument();

    const completedIndicator = roadmapItem?.querySelector('[data-testid="completed-indicator"]');
    expect(completedIndicator).toBeInTheDocument();

    const checkmark = roadmapItem?.querySelector('[data-testid="checkmark-icon"]');
    expect(checkmark).toBeInTheDocument();

    const badge = roadmapItem?.querySelector('[data-testid="status-badge"]');
    expect(badge).toHaveTextContent('Completed');
  });

  // Test Case 4: Query for 'minor compaction' item - Minor compaction is listed and marked as completed
  it('displays Minor Compaction item marked as completed', () => {
    render(<Roadmap />);
    const minorCompactionItem = screen.getByText(/minor compaction/i);
    expect(minorCompactionItem).toBeInTheDocument();

    const roadmapItem = minorCompactionItem.closest('[data-testid="roadmap-item"]');
    expect(roadmapItem).toBeInTheDocument();

    const completedIndicator = roadmapItem?.querySelector('[data-testid="completed-indicator"]');
    expect(completedIndicator).toBeInTheDocument();

    const checkmark = roadmapItem?.querySelector('[data-testid="checkmark-icon"]');
    expect(checkmark).toBeInTheDocument();

    const badge = roadmapItem?.querySelector('[data-testid="status-badge"]');
    expect(badge).toHaveTextContent('Completed');
  });

  // Test Case 5: Query for 'major compaction' item - Major compaction is listed and marked as completed
  it('displays Major Compaction item marked as completed', () => {
    render(<Roadmap />);
    const majorCompactionItem = screen.getByText(/major compaction/i);
    expect(majorCompactionItem).toBeInTheDocument();

    const roadmapItem = majorCompactionItem.closest('[data-testid="roadmap-item"]');
    expect(roadmapItem).toBeInTheDocument();

    const completedIndicator = roadmapItem?.querySelector('[data-testid="completed-indicator"]');
    expect(completedIndicator).toBeInTheDocument();

    const checkmark = roadmapItem?.querySelector('[data-testid="checkmark-icon"]');
    expect(checkmark).toBeInTheDocument();

    const badge = roadmapItem?.querySelector('[data-testid="status-badge"]');
    expect(badge).toHaveTextContent('Completed');
  });

  // Test Case 6: Query for 'Raft consensus' item - Raft consensus is listed and marked as planned/in-progress
  it('displays Raft Consensus item marked as planned', () => {
    render(<Roadmap />);
    const raftItem = screen.getByText(/raft consensus/i);
    expect(raftItem).toBeInTheDocument();

    const roadmapItem = raftItem.closest('[data-testid="roadmap-item"]');
    expect(roadmapItem).toBeInTheDocument();

    // Check for planned indicator (NOT completed)
    const plannedIndicator = roadmapItem?.querySelector('[data-testid="planned-indicator"]');
    expect(plannedIndicator).toBeInTheDocument();

    // Check for planned icon
    const plannedIcon = roadmapItem?.querySelector('[data-testid="planned-icon"]');
    expect(plannedIcon).toBeInTheDocument();

    // Should NOT have checkmark
    const checkmark = roadmapItem?.querySelector('[data-testid="checkmark-icon"]');
    expect(checkmark).toBeNull();

    // Check for Planned badge
    const badge = roadmapItem?.querySelector('[data-testid="status-badge"]');
    expect(badge).toHaveTextContent('Planned');
  });

  // Test Case 7: Check visual distinction between completed and planned
  it('has visual distinction between completed and planned items', () => {
    render(<Roadmap />);

    // Get all roadmap items
    const items = screen.getAllByTestId('roadmap-item');
    expect(items.length).toBeGreaterThan(0);

    // Check for completed indicators with green styling
    const completedIndicators = screen.getAllByTestId('completed-indicator');
    completedIndicators.forEach((indicator) => {
      expect(indicator).toHaveClass('bg-green-100');
    });

    // Check for planned indicators with blue styling
    const plannedIndicators = screen.getAllByTestId('planned-indicator');
    plannedIndicators.forEach((indicator) => {
      expect(indicator).toHaveClass('bg-blue-100');
    });

    // Check for checkmarks in completed items
    const checkmarks = screen.getAllByTestId('checkmark-icon');
    checkmarks.forEach((checkmark) => {
      expect(checkmark).toHaveClass('text-green-600');
    });

    // Check for different icon in planned items
    const plannedIcons = screen.getAllByTestId('planned-icon');
    plannedIcons.forEach((icon) => {
      expect(icon).toHaveClass('text-blue-600');
    });
  });

  // Test Case 8: Count completed items - At least 4 items are marked as completed
  it('has at least 4 completed items', () => {
    render(<Roadmap />);

    // Count completed indicators in DOM
    const completedIndicators = screen.getAllByTestId('completed-indicator');
    expect(completedIndicators.length).toBeGreaterThanOrEqual(4);

    // Also verify the data source
    const completedItemsInData = roadmapItems.filter((item) => item.completed);
    expect(completedItemsInData.length).toBeGreaterThanOrEqual(4);
  });

  // Additional test: Section has proper accessibility attributes
  it('has proper accessibility attributes', () => {
    render(<Roadmap />);
    const section = document.querySelector('section#roadmap');
    expect(section).toBeInTheDocument();
    expect(section).toHaveAttribute('aria-labelledby', 'roadmap-heading');
  });

  // Additional test: All roadmap items render
  it('renders all roadmap items from data', () => {
    render(<Roadmap />);
    const items = screen.getAllByTestId('roadmap-item');
    expect(items).toHaveLength(roadmapItems.length);
  });

  // Additional test: Each item has a name and status badge
  it('each item has a name and status badge', () => {
    render(<Roadmap />);
    const names = screen.getAllByTestId('roadmap-item-name');
    const badges = screen.getAllByTestId('status-badge');

    expect(names.length).toBe(roadmapItems.length);
    expect(badges.length).toBe(roadmapItems.length);
  });
});
