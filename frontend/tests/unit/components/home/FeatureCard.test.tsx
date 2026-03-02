/**
 * FeatureCard Component Tests.
 * Owner: Scenario 6 - Features Section with Analytics Showcase
 *
 * Tests the individual feature card component:
 * - Renders title, description, and visualization
 * - Applies custom className
 * - Has proper accessibility attributes
 */
import { render, screen } from '@testing-library/react';
import { describe, it, expect, vi } from 'vitest';
import { FeatureCard } from '../../../../src/components/home/FeatureCard';

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <div {...props}>{children}</div>
    ),
  },
  useScroll: () => ({ scrollYProgress: { current: 0 } }),
  useTransform: () => 0,
}));

describe('FeatureCard', () => {
  const defaultProps = {
    title: 'Test Feature',
    description: 'This is a test description for the feature card.',
    visualization: <div data-testid="test-viz">Mock Visualization</div>,
  };

  it('renders title, description, and visualization', () => {
    render(<FeatureCard {...defaultProps} />);

    // Verify title
    const title = screen.getByTestId('feature-card-title');
    expect(title).toBeInTheDocument();
    expect(title).toHaveTextContent('Test Feature');

    // Verify description
    const description = screen.getByTestId('feature-card-description');
    expect(description).toBeInTheDocument();
    expect(description).toHaveTextContent('This is a test description for the feature card.');

    // Verify visualization
    const visualization = screen.getByTestId('test-viz');
    expect(visualization).toBeInTheDocument();
  });

  it('renders the card container with proper styling', () => {
    render(<FeatureCard {...defaultProps} />);

    const card = screen.getByTestId('feature-card');
    expect(card).toBeInTheDocument();
    expect(card).toHaveClass('card');
  });

  it('applies custom className when provided', () => {
    render(<FeatureCard {...defaultProps} className="custom-test-class" />);

    const card = screen.getByTestId('feature-card');
    expect(card).toHaveClass('custom-test-class');
  });

  it('contains visualization container with data-testid', () => {
    render(<FeatureCard {...defaultProps} />);

    const vizContainer = screen.getByTestId('feature-visualization');
    expect(vizContainer).toBeInTheDocument();
  });

  it('renders with index prop for staggered animations', () => {
    render(<FeatureCard {...defaultProps} index={2} />);

    const card = screen.getByTestId('feature-card');
    expect(card).toBeInTheDocument();
  });

  it('renders title as h3 element', () => {
    render(<FeatureCard {...defaultProps} />);

    const title = screen.getByTestId('feature-card-title');
    expect(title.tagName.toLowerCase()).toBe('h3');
  });

  it('renders description as p element', () => {
    render(<FeatureCard {...defaultProps} />);

    const description = screen.getByTestId('feature-card-description');
    expect(description.tagName.toLowerCase()).toBe('p');
  });
});
