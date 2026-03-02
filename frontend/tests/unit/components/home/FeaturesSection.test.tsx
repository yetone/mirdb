/**
 * FeaturesSection Component Tests.
 * Owner: Scenario 6 - Features Section with Analytics Showcase
 *
 * Tests the features section with analytics showcase:
 * - Section renders with heading and three feature cards
 * - Grid layout is applied
 * - Minimum height requirement
 * - Static SVG visualizations (not Recharts)
 */
import { render, screen, within } from '@testing-library/react';
import { describe, it, expect, vi } from 'vitest';
import { FeaturesSection } from '../../../../src/components/home/FeaturesSection';

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <div {...props}>{children}</div>
    ),
    path: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <path {...props}>{children}</path>
    ),
    circle: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <circle {...props}>{children}</circle>
    ),
    g: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <g {...props}>{children}</g>
    ),
    rect: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <rect {...props}>{children}</rect>
    ),
  },
  useScroll: () => ({ scrollYProgress: { current: 0 } }),
  useTransform: () => 0,
}));

describe('FeaturesSection', () => {
  it('renders section with heading and three feature cards in grid layout', () => {
    render(<FeaturesSection />);

    // Verify section exists
    const section = screen.getByTestId('features-section');
    expect(section).toBeInTheDocument();

    // Verify heading
    const heading = screen.getByTestId('features-heading');
    expect(heading).toBeInTheDocument();
    expect(heading).toHaveTextContent('Powerful Analytics at Your Fingertips');

    // Verify grid layout
    const grid = screen.getByTestId('features-grid');
    expect(grid).toBeInTheDocument();
    expect(grid).toHaveClass('grid');

    // Verify three feature cards
    const cards = screen.getAllByTestId('feature-card');
    expect(cards).toHaveLength(3);
  });

  it('displays Real-time Click Tracking card with line chart SVG', () => {
    render(<FeaturesSection />);

    // Find the card with the Real-time Click Tracking title
    const titles = screen.getAllByTestId('feature-card-title');
    const clickTrackingTitle = titles.find(
      (title) => title.textContent === 'Real-time Click Tracking'
    );
    expect(clickTrackingTitle).toBeInTheDocument();

    // Verify line chart mockup is present
    const lineChart = screen.getByTestId('line-chart-mockup');
    expect(lineChart).toBeInTheDocument();
    expect(lineChart.tagName.toLowerCase()).toBe('svg');
  });

  it('displays Geographic Insights card with map visualization', () => {
    render(<FeaturesSection />);

    // Find the card with the Geographic Insights title
    const titles = screen.getAllByTestId('feature-card-title');
    const geoTitle = titles.find(
      (title) => title.textContent === 'Geographic Insights'
    );
    expect(geoTitle).toBeInTheDocument();

    // Verify map mockup is present
    const mapChart = screen.getByTestId('map-mockup');
    expect(mapChart).toBeInTheDocument();
    expect(mapChart.tagName.toLowerCase()).toBe('svg');
  });

  it('displays Device & Browser Breakdown card with pie chart SVG', () => {
    render(<FeaturesSection />);

    // Find the card with the Device & Browser Breakdown title
    const titles = screen.getAllByTestId('feature-card-title');
    const deviceTitle = titles.find(
      (title) => title.textContent === 'Device & Browser Breakdown'
    );
    expect(deviceTitle).toBeInTheDocument();

    // Verify pie chart mockup is present
    const pieChart = screen.getByTestId('pie-chart-mockup');
    expect(pieChart).toBeInTheDocument();
    expect(pieChart.tagName.toLowerCase()).toBe('svg');
  });

  it('has minimum height of 500px', () => {
    render(<FeaturesSection />);

    const section = screen.getByTestId('features-section');
    expect(section).toHaveClass('min-h-[500px]');
  });

  it('renders visualizations as static SVG elements (not Recharts)', () => {
    render(<FeaturesSection />);

    // Get all SVG elements
    const lineChart = screen.getByTestId('line-chart-mockup');
    const mapChart = screen.getByTestId('map-mockup');
    const pieChart = screen.getByTestId('pie-chart-mockup');

    // Verify they are native SVG elements
    expect(lineChart.tagName.toLowerCase()).toBe('svg');
    expect(mapChart.tagName.toLowerCase()).toBe('svg');
    expect(pieChart.tagName.toLowerCase()).toBe('svg');

    // Verify they have proper accessibility attributes
    expect(lineChart).toHaveAttribute('role', 'img');
    expect(mapChart).toHaveAttribute('role', 'img');
    expect(pieChart).toHaveAttribute('role', 'img');

    // Verify they are not Recharts components (no ResponsiveContainer wrapper)
    const responsiveContainers = document.querySelectorAll('.recharts-responsive-container');
    expect(responsiveContainers).toHaveLength(0);
  });

  it('applies custom className when provided', () => {
    render(<FeaturesSection className="custom-class" />);

    const section = screen.getByTestId('features-section');
    expect(section).toHaveClass('custom-class');
  });

  it('has proper aria attributes for accessibility', () => {
    render(<FeaturesSection />);

    const section = screen.getByTestId('features-section');
    expect(section).toHaveAttribute('aria-labelledby', 'features-heading');

    const heading = screen.getByTestId('features-heading');
    expect(heading).toHaveAttribute('id', 'features-heading');
  });
});
