/**
 * Unit tests for FeatureCard component
 * Owner: Scenario 2 - Features Section Display
 */

import { describe, it, expect } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { FeatureCard } from '../../../../src/components/ui/FeatureCard';
import type { Feature } from '../../../../src/types';

const mockFeature: Feature = {
  id: 'test-feature',
  title: 'Test Feature Title',
  description: 'This is a test feature description.',
  icon: <svg data-testid="mock-icon" />,
};

describe('FeatureCard', () => {
  it('renders the feature title', () => {
    render(<FeatureCard feature={mockFeature} />);
    expect(screen.getByText('Test Feature Title')).toBeInTheDocument();
  });

  it('renders the feature description', () => {
    render(<FeatureCard feature={mockFeature} />);
    expect(screen.getByText('This is a test feature description.')).toBeInTheDocument();
  });

  it('renders the feature icon', () => {
    render(<FeatureCard feature={mockFeature} />);
    expect(screen.getByTestId('mock-icon')).toBeInTheDocument();
  });

  it('has the correct test id based on feature id', () => {
    render(<FeatureCard feature={mockFeature} />);
    expect(screen.getByTestId('feature-card-test-feature')).toBeInTheDocument();
  });

  it('renders as an article element for semantic HTML', () => {
    render(<FeatureCard feature={mockFeature} />);
    const card = screen.getByTestId('feature-card-test-feature');
    expect(card.tagName).toBe('ARTICLE');
  });

  it('displays title in an h3 heading', () => {
    render(<FeatureCard feature={mockFeature} />);
    const heading = screen.getByRole('heading', { level: 3 });
    expect(heading).toHaveTextContent('Test Feature Title');
  });

  it('applies hover styles on mouse enter and leave', () => {
    render(<FeatureCard feature={mockFeature} />);
    const card = screen.getByTestId('feature-card-test-feature');

    // Initial state - no transform
    expect(card).toHaveStyle({ transform: 'translateY(0)' });

    // Hover state
    fireEvent.mouseEnter(card);
    expect(card).toHaveStyle({ transform: 'translateY(-4px)' });

    // Leave state
    fireEvent.mouseLeave(card);
    expect(card).toHaveStyle({ transform: 'translateY(0)' });
  });
});
