/**
 * FeatureCard Component Unit Tests.
 * Owner: Scenario 2 - Features Showcase Section
 *
 * Tests:
 * - Component renders with correct props
 * - Icon, title, and description display correctly
 * - Features component renders feature data as cards
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { FeatureCard } from '../../../src/components/ui/FeatureCard';
import { Features } from '../../../src/components/sections/Features';
import type { Feature } from '../../../src/types';

describe('FeatureCard Component', () => {
  const mockFeature: Feature = {
    title: 'Test Feature',
    description: 'This is a test feature description',
    icon: 'lightning',
  };

  it('renders the feature title correctly', () => {
    render(<FeatureCard {...mockFeature} />);

    const title = screen.getByRole('heading', { level: 3 });
    expect(title.textContent).toBe('Test Feature');
  });

  it('renders the feature description correctly', () => {
    render(<FeatureCard {...mockFeature} />);

    expect(screen.getByText('This is a test feature description')).toBeTruthy();
  });

  it('renders with the correct data-testid', () => {
    render(<FeatureCard {...mockFeature} testId="my-feature-card" />);

    const card = screen.getByTestId('my-feature-card');
    expect(card).toBeTruthy();
  });

  it('uses article element for semantic markup', () => {
    const { container } = render(<FeatureCard {...mockFeature} />);

    const article = container.querySelector('article');
    expect(article).toBeTruthy();
  });

  it('renders with default data-testid when not provided', () => {
    render(<FeatureCard {...mockFeature} />);

    const card = screen.getByTestId('feature-card');
    expect(card).toBeTruthy();
  });

  it('renders SVG icon', () => {
    const { container } = render(<FeatureCard {...mockFeature} />);

    const svg = container.querySelector('svg');
    expect(svg).toBeTruthy();
  });
});

describe('Features Component', () => {
  const mockFeatures: Feature[] = [
    {
      title: 'Tokio Async Runtime',
      description: 'High-performance async I/O',
      icon: 'lightning',
    },
    {
      title: 'Memtable with Skiplist',
      description: 'Fast in-memory data structure',
      icon: 'layers',
    },
    {
      title: 'Minor Compaction',
      description: 'Efficient disk writes',
      icon: 'compress',
    },
    {
      title: 'Major Compaction',
      description: 'Optimized reads',
      icon: 'merge',
    },
  ];

  it('TC5: Features component accepts array of features and renders cards', () => {
    render(<Features featureData={mockFeatures} />);

    // Verify all feature cards are rendered
    const cards = screen.getAllByTestId(/feature-card-\d+/);
    expect(cards.length).toBe(4);
  });

  it('renders the features section with correct id', () => {
    const { container } = render(<Features featureData={mockFeatures} />);

    const section = container.querySelector('#features');
    expect(section).toBeTruthy();
  });

  it('renders heading with Key Features text', () => {
    render(<Features featureData={mockFeatures} />);

    const heading = screen.getByRole('heading', { level: 2 });
    expect(heading.textContent).toBe('Key Features');
  });

  it('renders features in a grid', () => {
    render(<Features featureData={mockFeatures} />);

    const grid = screen.getByTestId('features-grid');
    expect(grid).toBeTruthy();
  });

  it('uses semantic section element', () => {
    const { container } = render(<Features featureData={mockFeatures} />);

    const section = container.querySelector('section');
    expect(section).toBeTruthy();
  });

  it('renders each feature with correct title', () => {
    render(<Features featureData={mockFeatures} />);

    expect(screen.getByText('Tokio Async Runtime')).toBeTruthy();
    expect(screen.getByText('Memtable with Skiplist')).toBeTruthy();
    expect(screen.getByText('Minor Compaction')).toBeTruthy();
    expect(screen.getByText('Major Compaction')).toBeTruthy();
  });

  it('uses default features when no featureData provided', () => {
    render(<Features />);

    // Should render at least 4 features (the default data)
    const cards = screen.getAllByTestId(/feature-card-\d+/);
    expect(cards.length).toBeGreaterThanOrEqual(4);
  });
});
