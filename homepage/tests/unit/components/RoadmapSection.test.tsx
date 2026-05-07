import { render, screen } from '@testing-library/react';
import RoadmapSection from '@/components/RoadmapSection';
import { ROADMAP_ITEMS } from '@/lib/constants';

describe('RoadmapSection', () => {
  it('renders the roadmap section with heading', () => {
    render(<RoadmapSection />);
    expect(screen.getByTestId('roadmap-section')).toBeInTheDocument();
    expect(screen.getByRole('heading', { level: 2, name: /roadmap/i })).toBeInTheDocument();
  });

  it('displays implemented and planned column headings', () => {
    render(<RoadmapSection />);
    expect(screen.getByRole('heading', { name: /implemented/i })).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: /planned/i })).toBeInTheDocument();
  });

  it('shows all 4 implemented features with checkmarks', () => {
    render(<RoadmapSection />);
    const implementedItems = screen.getAllByTestId('implemented-item');
    expect(implementedItems).toHaveLength(4);

    const implementedTitles = ROADMAP_ITEMS.filter((i) => i.status === 'implemented').map(
      (i) => i.title
    );
    for (const title of implementedTitles) {
      expect(screen.getByText(title)).toBeInTheDocument();
    }
  });

  it('shows at least 1 planned feature with pending icon', () => {
    render(<RoadmapSection />);
    const plannedItems = screen.getAllByTestId('planned-item');
    expect(plannedItems.length).toBeGreaterThanOrEqual(1);

    const plannedTitles = ROADMAP_ITEMS.filter((i) => i.status === 'planned').map(
      (i) => i.title
    );
    for (const title of plannedTitles) {
      expect(screen.getByText(title)).toBeInTheDocument();
    }
  });

  it('displays Raft consensus as a planned feature', () => {
    render(<RoadmapSection />);
    expect(
      screen.getByText(/raft consensus for distributed operation/i)
    ).toBeInTheDocument();
  });

  it('has distinct visual styling for implemented vs planned items', () => {
    render(<RoadmapSection />);

    const implementedList = screen.getByTestId('implemented-list');
    const plannedList = screen.getByTestId('planned-list');

    // Implemented items should have green styling classes
    const firstImplemented = implementedList.querySelector('[data-testid="implemented-item"]');
    expect(firstImplemented).toHaveClass('bg-green-50', 'dark:bg-green-900/20');

    // Planned items should have amber styling classes
    const firstPlanned = plannedList.querySelector('[data-testid="planned-item"]');
    expect(firstPlanned).toHaveClass('bg-amber-50', 'dark:bg-amber-900/20');
  });

  it('renders checkmark icons for implemented features', () => {
    render(<RoadmapSection />);
    const implementedItems = screen.getAllByTestId('implemented-item');
    for (const item of implementedItems) {
      const icon = item.querySelector('.bg-green-500');
      expect(icon).toBeInTheDocument();
    }
  });

  it('renders clock/pending icons for planned features', () => {
    render(<RoadmapSection />);
    const plannedItems = screen.getAllByTestId('planned-item');
    for (const item of plannedItems) {
      const icon = item.querySelector('.bg-amber-500');
      expect(icon).toBeInTheDocument();
    }
  });
});
