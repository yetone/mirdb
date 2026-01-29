/**
 * Unit tests for FeaturesGrid component.
 * Owner: Scenario 2 - Features Grid Implementation
 *
 * Tests cover:
 * - Rendering 6 feature cards
 * - Verifying each card's title and description
 * - Grid layout structure
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { FeaturesGrid, FeatureCard, Icon } from '../../../src/components/Features/FeaturesGrid';
import featuresData from '../../../src/content/features.json';
import type { Feature } from '../../../src/types/index';

const features: Feature[] = featuresData;

describe('FeaturesGrid', () => {
  describe('Test Case 1: Render FeaturesGrid component', () => {
    it('renders 6 feature cards with icons, titles, and descriptions', () => {
      render(<FeaturesGrid />);

      // Check that the features grid is rendered
      const grid = screen.getByTestId('features-grid');
      expect(grid).toBeInTheDocument();

      // Check that all 6 feature cards are rendered
      const featureCards = screen.getAllByTestId('feature-card');
      expect(featureCards).toHaveLength(6);

      // Check that all 6 titles are rendered
      const titles = screen.getAllByTestId('feature-title');
      expect(titles).toHaveLength(6);

      // Check that all 6 descriptions are rendered
      const descriptions = screen.getAllByTestId('feature-description');
      expect(descriptions).toHaveLength(6);

      // Check that all 6 icons are rendered (using data-testid since SVGs have aria-hidden)
      const iconNames = ['memcached', 'storage', 'performance', 'config', 'compaction', 'async'];
      iconNames.forEach(name => {
        expect(screen.getByTestId(`icon-${name}`)).toBeInTheDocument();
      });
    });
  });

  describe('Test Case 2: Check Memcached Compatible card', () => {
    it("displays 'Memcached Compatible' title with drop-in replacement description", () => {
      render(<FeaturesGrid />);

      // Check title
      expect(screen.getByText('Memcached Compatible')).toBeInTheDocument();

      // Check description contains key phrases
      const memcachedCard = features.find(f => f.id === 'memcached-compatible');
      expect(memcachedCard).toBeDefined();
      expect(screen.getByText(memcachedCard!.description)).toBeInTheDocument();

      // Verify description mentions "drop-in replacement"
      expect(memcachedCard!.description.toLowerCase()).toContain('drop-in replacement');
    });
  });

  describe('Test Case 3: Check Persistent Storage card', () => {
    it("displays 'Persistent Storage' title with LSM-tree architecture description", () => {
      render(<FeaturesGrid />);

      // Check title
      expect(screen.getByText('Persistent Storage')).toBeInTheDocument();

      // Check description
      const storageCard = features.find(f => f.id === 'persistent-storage');
      expect(storageCard).toBeDefined();
      expect(screen.getByText(storageCard!.description)).toBeInTheDocument();

      // Verify description mentions "LSM-tree"
      expect(storageCard!.description.toLowerCase()).toContain('lsm-tree');
    });
  });

  describe('Test Case 4: Check High Performance card', () => {
    it("displays 'High Performance' title with skip-list memtable description", () => {
      render(<FeaturesGrid />);

      // Check title
      expect(screen.getByText('High Performance')).toBeInTheDocument();

      // Check description
      const perfCard = features.find(f => f.id === 'high-performance');
      expect(perfCard).toBeDefined();
      expect(screen.getByText(perfCard!.description)).toBeInTheDocument();

      // Verify description mentions "skip-list memtable"
      expect(perfCard!.description.toLowerCase()).toContain('skip-list memtable');
    });
  });

  describe('Test Case 5: Check Configurable card', () => {
    it("displays 'Configurable' title with TOML-based configuration description", () => {
      render(<FeaturesGrid />);

      // Check title
      expect(screen.getByText('Configurable')).toBeInTheDocument();

      // Check description
      const configCard = features.find(f => f.id === 'configurable');
      expect(configCard).toBeDefined();
      expect(screen.getByText(configCard!.description)).toBeInTheDocument();

      // Verify description mentions "TOML"
      expect(configCard!.description.toLowerCase()).toContain('toml');
    });
  });

  describe('Test Case 6: Check Compaction card', () => {
    it("displays 'Compaction' title with automatic minor/major compaction description", () => {
      render(<FeaturesGrid />);

      // Check title
      expect(screen.getByText('Compaction')).toBeInTheDocument();

      // Check description
      const compactCard = features.find(f => f.id === 'compaction');
      expect(compactCard).toBeDefined();
      expect(screen.getByText(compactCard!.description)).toBeInTheDocument();

      // Verify description mentions "minor and major compaction"
      expect(compactCard!.description.toLowerCase()).toContain('minor and major compaction');
    });
  });

  describe('Test Case 7: Check Async I/O card', () => {
    it("displays 'Async I/O' title with Tokio for high concurrency description", () => {
      render(<FeaturesGrid />);

      // Check title
      expect(screen.getByText('Async I/O')).toBeInTheDocument();

      // Check description
      const asyncCard = features.find(f => f.id === 'async-io');
      expect(asyncCard).toBeDefined();
      expect(screen.getByText(asyncCard!.description)).toBeInTheDocument();

      // Verify description mentions "Tokio" and "concurrency"
      expect(asyncCard!.description.toLowerCase()).toContain('tokio');
      expect(asyncCard!.description.toLowerCase()).toContain('concurrency');
    });
  });
});

describe('FeatureCard', () => {
  it('renders a single feature card with icon, title, and description', () => {
    const feature = features[0];
    render(
      <FeatureCard
        icon={feature.icon}
        title={feature.title}
        description={feature.description}
      />
    );

    expect(screen.getByTestId('feature-card')).toBeInTheDocument();
    expect(screen.getByTestId('feature-title')).toHaveTextContent(feature.title);
    expect(screen.getByTestId('feature-description')).toHaveTextContent(feature.description);
    expect(screen.getByTestId(`icon-${feature.icon}`)).toBeInTheDocument();
  });
});

describe('Icon', () => {
  it('renders an icon with the correct size class', () => {
    render(<Icon name="memcached" size="lg" />);
    const icon = screen.getByTestId('icon-memcached');
    expect(icon).toBeInTheDocument();
    expect(icon).toHaveClass('w-8', 'h-8');
  });

  it('renders with default medium size when size is not specified', () => {
    render(<Icon name="storage" />);
    const icon = screen.getByTestId('icon-storage');
    expect(icon).toBeInTheDocument();
    expect(icon).toHaveClass('w-6', 'h-6');
  });

  it('renders with aria-hidden for accessibility', () => {
    render(<Icon name="performance" />);
    const icon = screen.getByTestId('icon-performance');
    expect(icon).toHaveAttribute('aria-hidden', 'true');
  });
});

describe('Grid Layout Structure', () => {
  it('has a responsive grid container', () => {
    render(<FeaturesGrid />);

    const container = screen.getByTestId('features-container');
    expect(container).toBeInTheDocument();
    expect(container).toHaveClass('grid', 'grid-cols-1', 'gap-6');
  });

  it('displays section heading', () => {
    render(<FeaturesGrid />);

    expect(screen.getByRole('heading', { level: 2 })).toHaveTextContent('Why MirDB?');
  });
});
