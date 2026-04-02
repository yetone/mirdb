/**
 * Unit tests for ComparisonTable component.
 * Owner: Scenario 6 - Comparison Section
 */
import React from 'react';
import { render, screen } from '../../setup/test-utils';
import { ComparisonTable } from '@/components/ui/ComparisonTable';
import {
  COMPARISON_FEATURES,
  COMPARISON_PRODUCTS,
} from '@/data/comparison';

describe('ComparisonTable', () => {
  const mockFeatures = [
    { name: 'persistence', description: 'Data Persistence' },
    { name: 'protocol', description: 'Memcached Protocol' },
  ];

  const mockProducts = [
    {
      name: 'MirDB',
      description: 'Persistent key-value store',
      highlighted: true,
      features: { persistence: true, protocol: true },
    },
    {
      name: 'Memcached',
      description: 'Memory caching system',
      highlighted: false,
      features: { persistence: false, protocol: true },
    },
    {
      name: 'Redis',
      description: 'In-memory data store',
      highlighted: false,
      features: { persistence: true, protocol: false },
    },
  ];

  it('renders comparison table with all products and features', () => {
    render(<ComparisonTable features={mockFeatures} products={mockProducts} />);

    // Check table exists
    expect(screen.getByTestId('comparison-table')).toBeInTheDocument();
    expect(screen.getByRole('table')).toBeInTheDocument();
  });

  it('renders all product headers', () => {
    render(<ComparisonTable features={mockFeatures} products={mockProducts} />);

    expect(screen.getByTestId('product-header-mirdb')).toHaveTextContent('MirDB');
    expect(screen.getByTestId('product-header-memcached')).toHaveTextContent('Memcached');
    expect(screen.getByTestId('product-header-redis')).toHaveTextContent('Redis');
  });

  it('renders all feature rows', () => {
    render(<ComparisonTable features={mockFeatures} products={mockProducts} />);

    expect(screen.getByTestId('feature-row-persistence')).toHaveTextContent('Data Persistence');
    expect(screen.getByTestId('feature-row-protocol')).toHaveTextContent('Memcached Protocol');
  });

  it('renders check icons for true values', () => {
    render(<ComparisonTable features={mockFeatures} products={mockProducts} />);

    // MirDB has persistence
    const mirdbPersistence = screen.getByTestId('cell-mirdb-persistence');
    expect(mirdbPersistence.querySelector('[data-testid="check-icon"]')).toBeInTheDocument();
  });

  it('renders cross icons for false values', () => {
    render(<ComparisonTable features={mockFeatures} products={mockProducts} />);

    // Memcached does not have persistence
    const memcachedPersistence = screen.getByTestId('cell-memcached-persistence');
    expect(memcachedPersistence.querySelector('[data-testid="cross-icon"]')).toBeInTheDocument();
  });

  it('renders text values for non-boolean features', () => {
    const productsWithText = [
      {
        name: 'MirDB',
        description: 'Test',
        features: { clustering: 'Planned' },
      },
    ];
    const featuresWithText = [
      { name: 'clustering', description: 'Clustering Support' },
    ];

    render(<ComparisonTable features={featuresWithText} products={productsWithText} />);

    expect(screen.getByTestId('feature-text')).toHaveTextContent('Planned');
  });

  it('highlights MirDB column', () => {
    render(<ComparisonTable features={mockFeatures} products={mockProducts} />);

    const mirdbHeader = screen.getByTestId('product-header-mirdb');
    expect(mirdbHeader).toHaveClass('highlightedHeader');
  });

  it('renders with actual comparison data', () => {
    render(
      <ComparisonTable
        features={COMPARISON_FEATURES}
        products={COMPARISON_PRODUCTS}
      />
    );

    // Verify all products are present
    expect(screen.getByTestId('product-header-mirdb')).toBeInTheDocument();
    expect(screen.getByTestId('product-header-memcached')).toBeInTheDocument();
    expect(screen.getByTestId('product-header-redis')).toBeInTheDocument();

    // Verify key features are present
    expect(screen.getByTestId('feature-row-persistence')).toBeInTheDocument();
    expect(screen.getByTestId('feature-row-protocol')).toBeInTheDocument();
  });

  it('has correct aria attributes for accessibility', () => {
    render(<ComparisonTable features={mockFeatures} products={mockProducts} />);

    // Check icons have aria-labels
    const checkIcons = screen.getAllByTestId('check-icon');
    checkIcons.forEach((icon) => {
      expect(icon).toHaveAttribute('aria-label', 'Yes');
    });

    const crossIcons = screen.getAllByTestId('cross-icon');
    crossIcons.forEach((icon) => {
      expect(icon).toHaveAttribute('aria-label', 'No');
    });
  });

  it('renders Feature column header', () => {
    render(<ComparisonTable features={mockFeatures} products={mockProducts} />);

    expect(screen.getByText('Feature')).toBeInTheDocument();
  });
});
