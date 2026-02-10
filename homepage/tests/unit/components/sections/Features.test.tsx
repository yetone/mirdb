/**
 * Unit tests for Features section component
 * Owner: Scenario 2 - Features Section Display
 */

import { describe, it, expect } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { Features } from '../../../../src/components/sections/Features';
import { features } from '../../../../src/data/features';

describe('Features', () => {
  it('renders four feature cards', () => {
    render(<Features />);
    const grid = screen.getByTestId('features-grid');
    const cards = within(grid).getAllByRole('listitem');
    expect(cards).toHaveLength(4);
  });

  it('renders the section with correct id for anchor navigation', () => {
    render(<Features />);
    const section = document.getElementById('features');
    expect(section).toBeInTheDocument();
  });

  it('renders the section title', () => {
    render(<Features />);
    expect(screen.getByText('Core Features')).toBeInTheDocument();
  });

  it('renders the section subtitle describing MirDB', () => {
    render(<Features />);
    expect(
      screen.getByText(/MirDB combines the simplicity of memcached protocol/i)
    ).toBeInTheDocument();
  });

  it('displays Tokio with Memcached Protocol feature with icon and description', () => {
    render(<Features />);
    expect(screen.getByText('Tokio with Memcached Protocol')).toBeInTheDocument();
    expect(
      screen.getByText(/Built on Tokio for high-performance async networking/i)
    ).toBeInTheDocument();
    expect(screen.getByTestId('feature-card-tokio-memcached')).toBeInTheDocument();
  });

  it('displays Memtable with Skiplist feature with icon and description', () => {
    render(<Features />);
    expect(screen.getByText('Memtable with Skiplist')).toBeInTheDocument();
    expect(
      screen.getByText(/In-memory data structure using skip lists/i)
    ).toBeInTheDocument();
    expect(screen.getByTestId('feature-card-memtable-skiplist')).toBeInTheDocument();
  });

  it('displays Minor Compaction feature with icon and description', () => {
    render(<Features />);
    expect(screen.getByText('Minor Compaction')).toBeInTheDocument();
    expect(
      screen.getByText(/Automatically flushes memtable data to disk/i)
    ).toBeInTheDocument();
    expect(screen.getByTestId('feature-card-minor-compaction')).toBeInTheDocument();
  });

  it('displays Major Compaction feature with icon and description', () => {
    render(<Features />);
    expect(screen.getByText('Major Compaction')).toBeInTheDocument();
    expect(
      screen.getByText(/LSM-tree level compaction merges and compacts/i)
    ).toBeInTheDocument();
    expect(screen.getByTestId('feature-card-major-compaction')).toBeInTheDocument();
  });

  it('renders features in a grid layout', () => {
    render(<Features />);
    const grid = screen.getByTestId('features-grid');
    expect(grid).toHaveStyle({ display: 'grid' });
  });

  it('uses responsive grid with auto-fit columns', () => {
    render(<Features />);
    const grid = screen.getByTestId('features-grid');
    expect(grid).toHaveStyle({
      gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))',
    });
  });

  it('has proper ARIA labeling for accessibility', () => {
    render(<Features />);
    const section = screen.getByRole('region', { name: /core features/i });
    expect(section).toBeInTheDocument();
  });

  it('uses role=list for the features grid', () => {
    render(<Features />);
    const grid = screen.getByRole('list');
    expect(grid).toBeInTheDocument();
  });

  it('renders all features from the data file', () => {
    render(<Features />);
    features.forEach((feature) => {
      expect(screen.getByText(feature.title)).toBeInTheDocument();
    });
  });

  it('renders each feature with its description', () => {
    render(<Features />);
    features.forEach((feature) => {
      expect(screen.getByText(feature.description)).toBeInTheDocument();
    });
  });
});
