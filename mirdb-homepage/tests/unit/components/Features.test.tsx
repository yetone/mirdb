import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { Features } from '../../../src/components/Features';
import { FeatureCard } from '../../../src/components/Features/FeatureCard';
import { FEATURES } from '../../../src/utils/constants';
import type { Feature } from '../../../src/types';

describe('Features Component', () => {
  // Test Case 1: Four feature cards are rendered
  it('renders four feature cards', () => {
    render(<Features />);

    const featureCards = screen.getAllByRole('article');
    expect(featureCards).toHaveLength(4);
  });

  // Test Case 2: Persistent Storage feature card displays title and description
  it('displays Persistent Storage feature card with title and description', () => {
    render(<Features />);

    expect(screen.getByText('Persistent Storage')).toBeInTheDocument();
    expect(screen.getByText(/Data persists reliably across restarts/)).toBeInTheDocument();
  });

  // Test Case 3: Memcached Protocol Compatibility feature card displays title and description
  it('displays Memcached Protocol Compatibility feature card with title and description', () => {
    render(<Features />);

    expect(screen.getByText('Memcached Protocol Compatibility')).toBeInTheDocument();
    expect(screen.getByText(/Drop-in replacement for Memcached/)).toBeInTheDocument();
  });

  // Test Case 4: LSM Tree Architecture feature card displays title and description
  it('displays LSM Tree Architecture feature card with title and description', () => {
    render(<Features />);

    expect(screen.getByText('LSM Tree Architecture')).toBeInTheDocument();
    expect(screen.getByText(/High-performance Log-Structured Merge-tree/)).toBeInTheDocument();
  });

  // Test Case 5: Raft Support (upcoming) feature card displays title and description with 'planned' indicator
  it('displays Raft Support feature card with planned indicator', () => {
    render(<Features />);

    expect(screen.getByText('Raft Support')).toBeInTheDocument();
    expect(screen.getByText(/Distributed consensus for high availability/)).toBeInTheDocument();
    expect(screen.getByText('Planned')).toBeInTheDocument();
  });

  // Test Case 6: FeatureCard receives correct props and renders icon, title, and description
  it('FeatureCard receives correct props and renders icon, title, and description', () => {
    const testFeature: Feature = {
      title: 'Test Feature',
      description: 'This is a test feature description',
      icon: '🧪',
      status: 'completed',
    };

    render(<FeatureCard feature={testFeature} />);

    expect(screen.getByText('🧪')).toBeInTheDocument();
    expect(screen.getByText('Test Feature')).toBeInTheDocument();
    expect(screen.getByText('This is a test feature description')).toBeInTheDocument();
  });

  // Additional test: FeatureCard with planned status shows indicator
  it('FeatureCard with planned status shows Planned indicator', () => {
    const plannedFeature: Feature = {
      title: 'Planned Feature',
      description: 'A feature that is planned',
      icon: '🔮',
      status: 'planned',
    };

    render(<FeatureCard feature={plannedFeature} />);

    expect(screen.getByText('Planned')).toBeInTheDocument();
  });

  // Test: Features section has correct heading
  it('displays Features section heading', () => {
    render(<Features />);

    expect(screen.getByRole('heading', { name: /Key Features/i })).toBeInTheDocument();
  });

  // Test: Features grid is accessible
  it('has accessible features grid with section landmark', () => {
    render(<Features />);

    const section = screen.getByRole('region', { name: /features/i });
    expect(section).toBeInTheDocument();
  });
});

describe('FeatureCard Component', () => {
  const mockFeature: Feature = {
    title: 'Mock Feature',
    description: 'Mock description for testing',
    icon: '✨',
    status: 'completed',
  };

  it('renders with correct semantic structure', () => {
    render(<FeatureCard feature={mockFeature} />);

    const article = screen.getByRole('article');
    expect(article).toBeInTheDocument();
  });

  it('renders icon in accessible span', () => {
    render(<FeatureCard feature={mockFeature} />);

    const icon = screen.getByText('✨');
    expect(icon).toBeInTheDocument();
    expect(icon).toHaveAttribute('aria-hidden', 'true');
  });

  it('renders title as heading', () => {
    render(<FeatureCard feature={mockFeature} />);

    const heading = screen.getByRole('heading', { name: 'Mock Feature' });
    expect(heading).toBeInTheDocument();
  });

  it('renders description text', () => {
    render(<FeatureCard feature={mockFeature} />);

    expect(screen.getByText('Mock description for testing')).toBeInTheDocument();
  });

  it('does not show Planned indicator for completed features', () => {
    render(<FeatureCard feature={mockFeature} />);

    expect(screen.queryByText('Planned')).not.toBeInTheDocument();
  });
});
