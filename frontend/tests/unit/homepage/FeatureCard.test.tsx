/**
 * Unit tests for FeatureCard component
 * Owner: Scenario 3 - Features Section Display
 *
 * Tests cover:
 * - Component rendering with props
 * - Title, description, and icon display
 * - Glass morphism styling
 *
 * Requirements: REQ-2, REQ-4
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { FeatureCard } from '../../../src/components/homepage/FeatureCard';

describe('FeatureCard', () => {
  const defaultProps = {
    title: 'Test Feature',
    description: 'This is a test feature description that explains what it does.',
    icon: <span data-testid="test-icon">Icon</span>,
  };

  // Test Case 6: Card displays title, description, and icon
  it('renders with all required props', () => {
    render(<FeatureCard {...defaultProps} />);
    expect(screen.getByTestId('feature-card')).toBeInTheDocument();
  });

  it('displays the title', () => {
    render(<FeatureCard {...defaultProps} />);
    const title = screen.getByTestId('feature-title');
    expect(title).toBeInTheDocument();
    expect(title).toHaveTextContent('Test Feature');
  });

  it('displays the description', () => {
    render(<FeatureCard {...defaultProps} />);
    const description = screen.getByTestId('feature-description');
    expect(description).toBeInTheDocument();
    expect(description).toHaveTextContent('This is a test feature description that explains what it does.');
  });

  it('displays the icon', () => {
    render(<FeatureCard {...defaultProps} />);
    const iconContainer = screen.getByTestId('feature-icon');
    expect(iconContainer).toBeInTheDocument();
    expect(screen.getByTestId('test-icon')).toBeInTheDocument();
  });

  // Test Case 7: Cards have glass morphism visual style
  it('uses GlassMorphismCard styling', () => {
    render(<FeatureCard {...defaultProps} />);
    const glassCard = screen.getByTestId('glass-morphism-card');
    expect(glassCard).toBeInTheDocument();
  });

  it('has proper heading level for title', () => {
    render(<FeatureCard {...defaultProps} />);
    const heading = screen.getByRole('heading', { level: 3 });
    expect(heading).toBeInTheDocument();
    expect(heading).toHaveTextContent('Test Feature');
  });

  // Test with different props
  it('renders with URL Shortening feature props', () => {
    const urlProps = {
      title: 'URL Shortening',
      description: 'Create short, memorable links instantly.',
      icon: <span data-testid="link-icon">Link</span>,
    };
    render(<FeatureCard {...urlProps} />);
    expect(screen.getByText('URL Shortening')).toBeInTheDocument();
    expect(screen.getByText('Create short, memorable links instantly.')).toBeInTheDocument();
  });

  it('renders with Analytics feature props', () => {
    const analyticsProps = {
      title: 'Click Analytics',
      description: 'Track every click with detailed insights.',
      icon: <span data-testid="chart-icon">Chart</span>,
    };
    render(<FeatureCard {...analyticsProps} />);
    expect(screen.getByText('Click Analytics')).toBeInTheDocument();
    expect(screen.getByText('Track every click with detailed insights.')).toBeInTheDocument();
  });

  it('renders with Link Management feature props', () => {
    const managementProps = {
      title: 'Link Management',
      description: 'Organize and manage all your shortened URLs.',
      icon: <span data-testid="folder-icon">Folder</span>,
    };
    render(<FeatureCard {...managementProps} />);
    expect(screen.getByText('Link Management')).toBeInTheDocument();
    expect(screen.getByText('Organize and manage all your shortened URLs.')).toBeInTheDocument();
  });

  it('icon container has proper styling classes', () => {
    render(<FeatureCard {...defaultProps} />);
    const iconContainer = screen.getByTestId('feature-icon');
    // Check that the icon container has gradient styling
    expect(iconContainer.className).toContain('rounded-full');
    expect(iconContainer.className).toContain('bg-gradient-to-br');
  });

  it('title has proper text styling', () => {
    render(<FeatureCard {...defaultProps} />);
    const title = screen.getByTestId('feature-title');
    expect(title.className).toContain('font-bold');
    expect(title.className).toContain('text-white');
  });

  it('description has proper text styling', () => {
    render(<FeatureCard {...defaultProps} />);
    const description = screen.getByTestId('feature-description');
    expect(description.className).toContain('text-gray-300');
  });

  it('renders empty icon when provided as empty element', () => {
    const emptyIconProps = {
      ...defaultProps,
      icon: <></>,
    };
    render(<FeatureCard {...emptyIconProps} />);
    expect(screen.getByTestId('feature-icon')).toBeInTheDocument();
  });

  it('handles long title text', () => {
    const longTitleProps = {
      ...defaultProps,
      title: 'This is a very long feature title that might wrap to multiple lines',
    };
    render(<FeatureCard {...longTitleProps} />);
    expect(screen.getByTestId('feature-title')).toHaveTextContent(longTitleProps.title);
  });

  it('handles long description text', () => {
    const longDescProps = {
      ...defaultProps,
      description: 'This is a very long feature description that explains in detail what this feature does and how it benefits the user. It may span multiple lines in the card layout.',
    };
    render(<FeatureCard {...longDescProps} />);
    expect(screen.getByTestId('feature-description')).toHaveTextContent(longDescProps.description);
  });
});
