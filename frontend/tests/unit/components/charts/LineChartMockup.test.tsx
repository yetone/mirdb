/**
 * LineChartMockup Component Tests.
 * Owner: Scenario 6 - Features Section with Analytics Showcase
 *
 * Tests the line chart mockup component:
 * - Renders as static SVG
 * - Has proper accessibility attributes
 * - Supports animate prop
 */
import { render, screen } from '@testing-library/react';
import { describe, it, expect, vi } from 'vitest';
import { LineChartMockup } from '../../../../src/components/charts/LineChartMockup';

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    path: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <path {...props}>{children}</path>
    ),
    circle: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <circle {...props}>{children}</circle>
    ),
  },
}));

describe('LineChartMockup', () => {
  it('renders as an SVG element', () => {
    render(<LineChartMockup />);

    const svg = screen.getByTestId('line-chart-mockup');
    expect(svg).toBeInTheDocument();
    expect(svg.tagName.toLowerCase()).toBe('svg');
  });

  it('has proper accessibility attributes', () => {
    render(<LineChartMockup />);

    const svg = screen.getByTestId('line-chart-mockup');
    expect(svg).toHaveAttribute('role', 'img');
    expect(svg).toHaveAttribute('aria-label');
  });

  it('applies custom className when provided', () => {
    render(<LineChartMockup className="custom-chart-class" />);

    const svg = screen.getByTestId('line-chart-mockup');
    expect(svg).toHaveClass('custom-chart-class');
  });

  it('renders with animate prop set to true by default', () => {
    render(<LineChartMockup />);

    const svg = screen.getByTestId('line-chart-mockup');
    expect(svg).toBeInTheDocument();
  });

  it('renders with animate prop set to false', () => {
    render(<LineChartMockup animate={false} />);

    const svg = screen.getByTestId('line-chart-mockup');
    expect(svg).toBeInTheDocument();
  });

  it('has proper viewBox attribute', () => {
    render(<LineChartMockup />);

    const svg = screen.getByTestId('line-chart-mockup');
    expect(svg).toHaveAttribute('viewBox');
  });

  it('is a static SVG (not a Recharts component)', () => {
    const { container } = render(<LineChartMockup />);

    // Verify no Recharts wrapper elements
    const rechartsContainers = container.querySelectorAll('.recharts-responsive-container');
    expect(rechartsContainers).toHaveLength(0);

    // Verify it's a native SVG
    const svg = screen.getByTestId('line-chart-mockup');
    expect(svg.tagName.toLowerCase()).toBe('svg');
  });
});
