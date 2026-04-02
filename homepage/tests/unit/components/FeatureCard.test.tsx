/**
 * Unit tests for FeatureCard component.
 * Owner: Scenario 3 - Features Section Display
 */
import React from 'react';
import { render, screen } from '../../setup/test-utils';
import { FeatureCard } from '@/components/ui/FeatureCard';

describe('FeatureCard', () => {
  const mockProps = {
    icon: '🔌',
    title: 'Test Feature',
    description: 'This is a test feature description.',
  };

  it('renders icon, title, and description correctly', () => {
    render(<FeatureCard {...mockProps} />);

    // Check icon is rendered
    expect(screen.getByText('🔌')).toBeInTheDocument();

    // Check title is rendered
    expect(screen.getByRole('heading', { name: 'Test Feature' })).toBeInTheDocument();

    // Check description is rendered
    expect(screen.getByText('This is a test feature description.')).toBeInTheDocument();
  });

  it('renders as an article element for semantic HTML', () => {
    render(<FeatureCard {...mockProps} />);
    expect(screen.getByRole('article')).toBeInTheDocument();
  });

  it('has the correct test id for targeting', () => {
    render(<FeatureCard {...mockProps} />);
    expect(screen.getByTestId('feature-card')).toBeInTheDocument();
  });

  it('renders different props correctly', () => {
    const differentProps = {
      icon: '💾',
      title: 'Persistence',
      description: 'Stores data on disk.',
    };

    render(<FeatureCard {...differentProps} />);

    expect(screen.getByText('💾')).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: 'Persistence' })).toBeInTheDocument();
    expect(screen.getByText('Stores data on disk.')).toBeInTheDocument();
  });

  it('marks icon as aria-hidden for accessibility', () => {
    const { container } = render(<FeatureCard {...mockProps} />);
    const iconElement = container.querySelector('[aria-hidden="true"]');
    expect(iconElement).toBeInTheDocument();
    expect(iconElement).toHaveTextContent('🔌');
  });
});
