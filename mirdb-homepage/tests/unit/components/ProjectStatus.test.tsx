import { describe, it, expect } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { ProjectStatus } from '../../../src/components/ProjectStatus';
import { StatusItem } from '../../../src/components/ProjectStatus/StatusItem';
import type { ProjectStatusItem } from '../../../src/types';

describe('ProjectStatus Component', () => {
  // Test Case 1: Component renders with list of project status items
  it('renders component with list of project status items', () => {
    render(<ProjectStatus />);

    const statusList = screen.getByTestId('project-status-list');
    expect(statusList).toBeInTheDocument();

    const statusItems = screen.getAllByTestId('status-item');
    expect(statusItems.length).toBeGreaterThan(0);
  });

  // Test Case 2: 'tokio with memcached protocol' is displayed with completed status
  it("displays 'tokio with memcached protocol' with completed status", () => {
    render(<ProjectStatus />);

    const tokioItem = screen.getByText('tokio with memcached protocol');
    expect(tokioItem).toBeInTheDocument();

    const listItem = tokioItem.closest('li');
    expect(listItem).toHaveClass('completed');
  });

  // Test Case 3: 'memtable with skiplist' is displayed with completed status
  it("displays 'memtable with skiplist' with completed status", () => {
    render(<ProjectStatus />);

    const memtableItem = screen.getByText('memtable with skiplist');
    expect(memtableItem).toBeInTheDocument();

    const listItem = memtableItem.closest('li');
    expect(listItem).toHaveClass('completed');
  });

  // Test Case 4: 'minor compaction' is displayed with completed status
  it("displays 'minor compaction' with completed status", () => {
    render(<ProjectStatus />);

    const minorCompactionItem = screen.getByText('minor compaction');
    expect(minorCompactionItem).toBeInTheDocument();

    const listItem = minorCompactionItem.closest('li');
    expect(listItem).toHaveClass('completed');
  });

  // Test Case 5: 'major compaction' is displayed with completed status
  it("displays 'major compaction' with completed status", () => {
    render(<ProjectStatus />);

    const majorCompactionItem = screen.getByText('major compaction');
    expect(majorCompactionItem).toBeInTheDocument();

    const listItem = majorCompactionItem.closest('li');
    expect(listItem).toHaveClass('completed');
  });

  // Test Case 6: 'raft' is displayed with planned/in-progress status
  it("displays 'raft' with planned/in-progress status", () => {
    render(<ProjectStatus />);

    const raftItem = screen.getByText('raft');
    expect(raftItem).toBeInTheDocument();

    const listItem = raftItem.closest('li');
    expect(listItem).toHaveClass('planned');
  });

  // Additional test: Project Status section has correct heading
  it('displays Project Status section heading', () => {
    render(<ProjectStatus />);

    expect(screen.getByRole('heading', { name: /Project Status/i })).toBeInTheDocument();
  });

  // Test: Project Status section is accessible
  it('has accessible project status section with region landmark', () => {
    render(<ProjectStatus />);

    const section = screen.getByRole('region', { name: /project status/i });
    expect(section).toBeInTheDocument();
  });
});

describe('StatusItem Component', () => {
  // Test Case 7: Item shows checkmark icon and completed styling when completed=true
  it('shows checkmark icon and completed styling when completed=true', () => {
    const completedItem: ProjectStatusItem = {
      title: 'Completed Feature',
      completed: true,
    };

    render(<StatusItem item={completedItem} />);

    const listItem = screen.getByTestId('status-item');
    expect(listItem).toHaveClass('completed');

    const checkmark = screen.getByRole('img', { name: /completed/i });
    expect(checkmark).toBeInTheDocument();
    expect(checkmark).toHaveTextContent('✓');
  });

  // Test Case 8: Item shows pending icon and planned styling when completed=false
  it('shows pending icon and planned styling when completed=false', () => {
    const plannedItem: ProjectStatusItem = {
      title: 'Planned Feature',
      completed: false,
    };

    render(<StatusItem item={plannedItem} />);

    const listItem = screen.getByTestId('status-item');
    expect(listItem).toHaveClass('planned');

    const pendingIcon = screen.getByRole('img', { name: /planned/i });
    expect(pendingIcon).toBeInTheDocument();
    expect(pendingIcon).toHaveTextContent('○');
  });

  // Test: StatusItem renders title correctly
  it('renders item title correctly', () => {
    const testItem: ProjectStatusItem = {
      title: 'Test Feature Title',
      completed: true,
    };

    render(<StatusItem item={testItem} />);

    expect(screen.getByText('Test Feature Title')).toBeInTheDocument();
  });

  // Test: StatusItem is a list item
  it('renders as a list item element', () => {
    const testItem: ProjectStatusItem = {
      title: 'Test Item',
      completed: false,
    };

    render(<StatusItem item={testItem} />);

    const listItem = screen.getByTestId('status-item');
    expect(listItem.tagName.toLowerCase()).toBe('li');
  });
});
