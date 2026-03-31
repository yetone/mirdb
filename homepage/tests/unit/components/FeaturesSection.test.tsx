/**
 * Unit and Integration tests for FeaturesSection Component
 * Owner: Scenario 2 - Features Section Display
 *
 * Tests cover:
 * - Feature cards rendering
 * - Status badges (implemented/planned)
 * - Responsive grid layout
 * - Specific feature content verification
 * - Hover effects
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { FeaturesSection } from '../../../src/components/sections/FeaturesSection';
import { features } from '../../../src/constants/features';
import type { Feature } from '../../../src/types';

describe('FeaturesSection', () => {
  // Test Case 1: Render FeaturesSection with feature data
  it('renders all feature cards in the DOM', () => {
    render(<FeaturesSection features={features} />);

    // Check that the features section is rendered
    expect(screen.getByRole('region', { name: /features/i })).toBeInTheDocument();

    // Check that all feature cards are rendered
    features.forEach((feature) => {
      expect(screen.getByTestId(`feature-card-${feature.id}`)).toBeInTheDocument();
      expect(screen.getByText(feature.name)).toBeInTheDocument();
    });
  });

  // Test Case 2: Render FeaturesSection with implemented feature
  it('displays implemented status badge with checkmark for implemented features', () => {
    const implementedFeatures: Feature[] = [
      {
        id: 'test-implemented',
        name: 'Test Implemented Feature',
        description: 'A test feature that is implemented',
        status: 'implemented',
      },
    ];

    render(<FeaturesSection features={implementedFeatures} />);

    const statusBadge = screen.getByTestId('status-badge-test-implemented');
    expect(statusBadge).toBeInTheDocument();
    expect(statusBadge).toHaveTextContent('Implemented');

    // Check for checkmark icon (the badge should have success variant styling)
    expect(statusBadge).toHaveClass('bg-green-100');
  });

  // Test Case 3: Render FeaturesSection with planned feature (Raft)
  it('displays upcoming status badge for planned features', () => {
    const plannedFeatures: Feature[] = [
      {
        id: 'raft-consensus',
        name: 'Raft Consensus',
        description: 'Distributed consensus protocol',
        status: 'planned',
      },
    ];

    render(<FeaturesSection features={plannedFeatures} />);

    const statusBadge = screen.getByTestId('status-badge-raft-consensus');
    expect(statusBadge).toBeInTheDocument();
    expect(statusBadge).toHaveTextContent('Upcoming');

    // Check for warning variant styling (upcoming/planned)
    expect(statusBadge).toHaveClass('bg-yellow-100');
  });

  // Test Case 4: Render FeaturesSection on desktop viewport (1200px)
  it('displays features in 3-column grid layout on desktop', () => {
    render(<FeaturesSection features={features} />);

    const grid = screen.getByTestId('features-grid');
    expect(grid).toBeInTheDocument();

    // Check that the grid has the correct Tailwind classes for 3 columns on large screens
    expect(grid).toHaveClass('lg:grid-cols-3');
    expect(grid).toHaveClass('grid');
  });

  // Test Case 5: Render FeaturesSection on mobile viewport (375px)
  it('displays features in single-column layout on mobile', () => {
    render(<FeaturesSection features={features} />);

    const grid = screen.getByTestId('features-grid');
    expect(grid).toBeInTheDocument();

    // Check that the grid has the correct Tailwind class for single column by default
    expect(grid).toHaveClass('grid-cols-1');
  });

  // Test Case 6: Verify Memcached Protocol feature
  it('displays Memcached Protocol feature with protocol compatibility description', () => {
    render(<FeaturesSection features={features} />);

    const memcachedFeature = screen.getByTestId('feature-card-memcached-protocol');
    expect(memcachedFeature).toBeInTheDocument();

    // Check for protocol compatibility in description
    expect(screen.getByText(/memcached text protocol/i)).toBeInTheDocument();
    expect(screen.getByText(/existing memcached clients/i)).toBeInTheDocument();
  });

  // Test Case 7: Verify Persistence feature
  it('displays Persistence feature with SSTable storage description', () => {
    render(<FeaturesSection features={features} />);

    const persistenceFeature = screen.getByTestId('feature-card-persistence');
    expect(persistenceFeature).toBeInTheDocument();

    // Check for SSTable mention in description - use within to scope to the persistence card
    // Also verify that the description contains both SSTable and persistence keywords
    expect(persistenceFeature.textContent).toMatch(/SSTables/i);
    expect(persistenceFeature.textContent).toMatch(/persists data to disk/i);
  });

  // Test Case 8: Hover over feature card - verify hover classes exist
  it('displays hover effect classes on feature cards', () => {
    render(<FeaturesSection features={features} />);

    const featureCard = screen.getByTestId('feature-card-memcached-protocol');
    expect(featureCard).toBeInTheDocument();

    // Check that the card has hover effect classes
    expect(featureCard).toHaveClass('hover:shadow-lg');
    expect(featureCard).toHaveClass('hover:-translate-y-1');
  });

  // Additional test: Verify all features have correct status badges
  it('displays correct status badges for all features', () => {
    render(<FeaturesSection features={features} />);

    // Count implemented and planned features
    const implementedCount = features.filter((f) => f.status === 'implemented').length;
    const plannedCount = features.filter((f) => f.status === 'planned').length;

    // Check implemented badges
    const implementedBadges = screen.getAllByText('Implemented');
    expect(implementedBadges).toHaveLength(implementedCount);

    // Check upcoming badges
    const upcomingBadges = screen.getAllByText('Upcoming');
    expect(upcomingBadges).toHaveLength(plannedCount);
  });

  // Additional test: Verify section heading
  it('displays Features heading', () => {
    render(<FeaturesSection features={features} />);

    expect(screen.getByRole('heading', { name: 'Features' })).toBeInTheDocument();
  });
});
