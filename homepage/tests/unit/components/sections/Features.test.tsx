import { describe, it, expect } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { Features } from '../../../../src/components/sections/Features';
import { features } from '../../../../src/data/features';

describe('Features Component', () => {
  // Test Case 1: Component renders with section heading 'Features'
  it('renders with section heading "Features"', () => {
    render(<Features />);
    const heading = screen.getByRole('heading', { level: 2, name: /features/i });
    expect(heading).toBeInTheDocument();
    expect(heading).toHaveTextContent('Features');
  });

  // Test Case 2: Query for 'Memcached Protocol Compatible' feature
  it('displays "Memcached Protocol Compatible" feature with icon, title, and description', () => {
    render(<Features />);
    const title = screen.getByRole('heading', { level: 3, name: /memcached protocol compatible/i });
    expect(title).toBeInTheDocument();

    // Get the parent card
    const card = title.closest('[data-testid="feature-card"]');
    expect(card).toBeInTheDocument();

    // Check for icon (emoji)
    const icon = within(card as HTMLElement).getByRole('img', { name: /memcached protocol compatible icon/i });
    expect(icon).toBeInTheDocument();

    // Check for description
    const description = within(card as HTMLElement).getByText(/drop-in replacement for memcached/i);
    expect(description).toBeInTheDocument();
  });

  // Test Case 3: Query for 'Persistent Storage' feature
  it('displays "Persistent Storage" feature with icon, title, and description', () => {
    render(<Features />);
    const title = screen.getByRole('heading', { level: 3, name: /persistent storage/i });
    expect(title).toBeInTheDocument();

    const card = title.closest('[data-testid="feature-card"]');
    expect(card).toBeInTheDocument();

    const icon = within(card as HTMLElement).getByRole('img', { name: /persistent storage icon/i });
    expect(icon).toBeInTheDocument();

    const description = within(card as HTMLElement).getByText(/your data survives restarts/i);
    expect(description).toBeInTheDocument();
  });

  // Test Case 4: Query for 'LSM Tree Architecture' feature
  it('displays "LSM Tree Architecture" feature with icon, title, and description', () => {
    render(<Features />);
    const title = screen.getByRole('heading', { level: 3, name: /lsm tree architecture/i });
    expect(title).toBeInTheDocument();

    const card = title.closest('[data-testid="feature-card"]');
    expect(card).toBeInTheDocument();

    const icon = within(card as HTMLElement).getByRole('img', { name: /lsm tree architecture icon/i });
    expect(icon).toBeInTheDocument();

    const description = within(card as HTMLElement).getByText(/log-structured merge trees/i);
    expect(description).toBeInTheDocument();
  });

  // Test Case 5: Query for 'Async/Tokio Networking' feature
  it('displays "Async/Tokio Networking" feature with icon, title, and description', () => {
    render(<Features />);
    const title = screen.getByRole('heading', { level: 3, name: /async\/tokio networking/i });
    expect(title).toBeInTheDocument();

    const card = title.closest('[data-testid="feature-card"]');
    expect(card).toBeInTheDocument();

    const icon = within(card as HTMLElement).getByRole('img', { name: /async\/tokio networking icon/i });
    expect(icon).toBeInTheDocument();

    const description = within(card as HTMLElement).getByText(/high-performance async networking/i);
    expect(description).toBeInTheDocument();
  });

  // Test Case 6: Query for 'Skip List Memtable' feature
  it('displays "Skip List Memtable" feature with icon, title, and description', () => {
    render(<Features />);
    const title = screen.getByRole('heading', { level: 3, name: /skip list memtable/i });
    expect(title).toBeInTheDocument();

    const card = title.closest('[data-testid="feature-card"]');
    expect(card).toBeInTheDocument();

    const icon = within(card as HTMLElement).getByRole('img', { name: /skip list memtable icon/i });
    expect(icon).toBeInTheDocument();

    const description = within(card as HTMLElement).getByText(/fast in-memory indexing/i);
    expect(description).toBeInTheDocument();
  });

  // Test Case 7: Query for 'Automatic Compaction' feature
  it('displays "Automatic Compaction" feature with icon, title, and description', () => {
    render(<Features />);
    const title = screen.getByRole('heading', { level: 3, name: /automatic compaction/i });
    expect(title).toBeInTheDocument();

    const card = title.closest('[data-testid="feature-card"]');
    expect(card).toBeInTheDocument();

    const icon = within(card as HTMLElement).getByRole('img', { name: /automatic compaction icon/i });
    expect(icon).toBeInTheDocument();

    const description = within(card as HTMLElement).getByText(/background compaction automatically/i);
    expect(description).toBeInTheDocument();
  });

  // Test Case 8: Exactly 6 feature cards are rendered
  it('renders exactly 6 feature cards', () => {
    render(<Features />);
    const cards = screen.getAllByTestId('feature-card');
    expect(cards).toHaveLength(6);

    // Also verify the data source has 6 features
    expect(features).toHaveLength(6);
  });

  // Test Case 9: Cards have subtle hover lift effect or highlight
  it('applies hover effect classes to feature cards', () => {
    render(<Features />);
    const cards = screen.getAllByTestId('feature-card');

    cards.forEach((card) => {
      // Find the parent Card component which has the hover classes
      const cardWrapper = card.closest('.hover\\:shadow-lg');
      expect(cardWrapper).toBeInTheDocument();

      // Check for the transition class
      const transitionElement = card.closest('.transition-all');
      expect(transitionElement).toBeInTheDocument();

      // Check for the hover lift class
      const liftElement = card.closest('.hover\\:-translate-y-1');
      expect(liftElement).toBeInTheDocument();
    });
  });

  // Additional test: Section has proper accessibility attributes
  it('has proper accessibility attributes', () => {
    render(<Features />);
    const section = document.querySelector('section#features');
    expect(section).toBeInTheDocument();
    expect(section).toHaveAttribute('aria-labelledby', 'features-heading');
  });

  // Additional test: Grid uses responsive classes for layout
  it('uses responsive grid classes for layout', () => {
    render(<Features />);

    // Find the grid container
    const section = document.querySelector('section#features');
    expect(section).toBeInTheDocument();

    const gridContainer = section?.querySelector('.grid');
    expect(gridContainer).toBeInTheDocument();

    // Check for responsive column classes
    expect(gridContainer).toHaveClass('grid-cols-1');  // Mobile: 1 column
    expect(gridContainer).toHaveClass('md:grid-cols-2');  // Tablet: 2 columns
    expect(gridContainer).toHaveClass('lg:grid-cols-3');  // Desktop: 3 columns
  });
});
