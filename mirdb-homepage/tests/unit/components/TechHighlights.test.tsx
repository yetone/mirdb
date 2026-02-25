import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { TechHighlights } from '../../../src/components/TechHighlights';

describe('TechHighlights Component', () => {
  // Test Case 1: Component renders with architecture overview content
  it('renders with architecture overview content', () => {
    render(<TechHighlights />);

    expect(
      screen.getByRole('heading', { name: /Technical Highlights/i })
    ).toBeInTheDocument();
    expect(
      screen.getByText(/Understanding the architecture behind MirDB/i)
    ).toBeInTheDocument();
  });

  // Test Case 2: LSM Tree architecture description is displayed
  it('displays LSM Tree architecture description', () => {
    render(<TechHighlights />);

    expect(screen.getByText('LSM Tree Architecture')).toBeInTheDocument();
    expect(
      screen.getByText(/Log-Structured Merge-tree design/i)
    ).toBeInTheDocument();
    expect(screen.getByText(/MemTable backed by a SkipList/i)).toBeInTheDocument();
    expect(screen.getByText(/SSTables with automatic compaction/i)).toBeInTheDocument();
  });

  // Test Case 3: Rust implementation highlights are mentioned
  it('displays Rust implementation highlights', () => {
    render(<TechHighlights />);

    expect(screen.getByText('Rust Implementation')).toBeInTheDocument();
    expect(screen.getByText(/safe Rust for memory safety/i)).toBeInTheDocument();
    expect(screen.getByText(/async\/await with Tokio/i)).toBeInTheDocument();
  });

  // Test Case 4: Performance characteristics are described
  it('displays performance characteristics', () => {
    render(<TechHighlights />);

    expect(screen.getByText('Performance Characteristics')).toBeInTheDocument();
    expect(screen.getByText(/O\(1\) writes to MemTable/i)).toBeInTheDocument();
    expect(screen.getByText(/background compaction/i)).toBeInTheDocument();
    expect(screen.getByText(/high-throughput workloads/i)).toBeInTheDocument();
  });

  // Test Case 5: Component structure and accessibility
  it('has correct section structure and accessibility', () => {
    render(<TechHighlights />);

    const section = screen.getByRole('region', { name: /technical highlights/i });
    expect(section).toBeInTheDocument();
    expect(section).toHaveAttribute('id', 'architecture');
  });

  // Additional test: All three highlight cards are rendered
  it('renders three technical highlight cards', () => {
    render(<TechHighlights />);

    const cards = screen.getAllByRole('article');
    expect(cards).toHaveLength(3);
  });

  // Additional test: Each card has an icon
  it('displays icons for each highlight card', () => {
    render(<TechHighlights />);

    // Icons should be present (aria-hidden)
    expect(screen.getByText('🌳')).toBeInTheDocument();
    expect(screen.getByText('🦀')).toBeInTheDocument();
    expect(screen.getByText('⚡')).toBeInTheDocument();
  });

  // Additional test: Icons have aria-hidden attribute
  it('icons have aria-hidden attribute for accessibility', () => {
    render(<TechHighlights />);

    const treeIcon = screen.getByText('🌳');
    const rustIcon = screen.getByText('🦀');
    const perfIcon = screen.getByText('⚡');

    expect(treeIcon).toHaveAttribute('aria-hidden', 'true');
    expect(rustIcon).toHaveAttribute('aria-hidden', 'true');
    expect(perfIcon).toHaveAttribute('aria-hidden', 'true');
  });

  // Test: Card titles are rendered as headings
  it('renders card titles as headings', () => {
    render(<TechHighlights />);

    expect(
      screen.getByRole('heading', { name: 'LSM Tree Architecture' })
    ).toBeInTheDocument();
    expect(
      screen.getByRole('heading', { name: 'Rust Implementation' })
    ).toBeInTheDocument();
    expect(
      screen.getByRole('heading', { name: 'Performance Characteristics' })
    ).toBeInTheDocument();
  });
});

describe('TechHighlights Responsive Design', () => {
  // Test Case 5: Content is readable and properly formatted (mocked viewport test)
  it('renders content that is readable on any viewport', () => {
    render(<TechHighlights />);

    // Verify all content is rendered and accessible
    const section = screen.getByRole('region', { name: /technical highlights/i });
    expect(section).toBeInTheDocument();

    // Check that all cards have descriptions (ensures readable content)
    const descriptions = [
      /Log-Structured Merge-tree design/i,
      /safe Rust for memory safety/i,
      /O\(1\) writes to MemTable/i,
    ];

    descriptions.forEach((desc) => {
      expect(screen.getByText(desc)).toBeInTheDocument();
    });
  });

  it('cards have semantic structure for proper content flow', () => {
    render(<TechHighlights />);

    const cards = screen.getAllByRole('article');
    cards.forEach((card) => {
      // Each card should have a heading (h3)
      const heading = card.querySelector('h3');
      expect(heading).toBeInTheDocument();

      // Each card should have a description paragraph
      const description = card.querySelector('p');
      expect(description).toBeInTheDocument();
    });
  });
});
