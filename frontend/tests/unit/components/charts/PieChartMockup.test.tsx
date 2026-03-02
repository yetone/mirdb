/**
 * PieChartMockup Component Tests.
 * Owner: Scenario 6 - Features Section with Analytics Showcase
 *
 * Tests the pie chart mockup component:
 * - Renders as static SVG
 * - Has proper accessibility attributes
 * - Supports animate prop
 */
import { render, screen } from '@testing-library/react';
import { describe, it, expect, vi } from 'vitest';
import { PieChartMockup } from '../../../../src/components/charts/PieChartMockup';

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    path: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <path {...props}>{children}</path>
    ),
    g: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <g {...props}>{children}</g>
    ),
  },
}));

describe('PieChartMockup', () => {
  it('renders as an SVG element', () => {
    render(<PieChartMockup />);

    const svg = screen.getByTestId('pie-chart-mockup');
    expect(svg).toBeInTheDocument();
    expect(svg.tagName.toLowerCase()).toBe('svg');
  });

  it('has proper accessibility attributes', () => {
    render(<PieChartMockup />);

    const svg = screen.getByTestId('pie-chart-mockup');
    expect(svg).toHaveAttribute('role', 'img');
    expect(svg).toHaveAttribute('aria-label');
  });

  it('applies custom className when provided', () => {
    render(<PieChartMockup className="custom-pie-class" />);

    const svg = screen.getByTestId('pie-chart-mockup');
    expect(svg).toHaveClass('custom-pie-class');
  });

  it('renders with animate prop set to true by default', () => {
    render(<PieChartMockup />);

    const svg = screen.getByTestId('pie-chart-mockup');
    expect(svg).toBeInTheDocument();
  });

  it('renders with animate prop set to false', () => {
    render(<PieChartMockup animate={false} />);

    const svg = screen.getByTestId('pie-chart-mockup');
    expect(svg).toBeInTheDocument();
  });

  it('has proper viewBox attribute', () => {
    render(<PieChartMockup />);

    const svg = screen.getByTestId('pie-chart-mockup');
    expect(svg).toHaveAttribute('viewBox');
  });

  it('is a static SVG (not a Recharts component)', () => {
    const { container } = render(<PieChartMockup />);

    // Verify no Recharts wrapper elements
    const rechartsContainers = container.querySelectorAll('.recharts-responsive-container');
    expect(rechartsContainers).toHaveLength(0);

    // Verify it's a native SVG
    const svg = screen.getByTestId('pie-chart-mockup');
    expect(svg.tagName.toLowerCase()).toBe('svg');
  });
});
