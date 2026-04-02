/**
 * Unit tests for PerformanceChart component.
 * Owner: Scenario 5 - Performance Section Display
 *
 * Test case 4: Component renders performance visualization correctly
 */
import React from 'react';
import { render, screen } from '../../setup/test-utils';
import { PerformanceChart, PerformanceMetric } from '@/components/ui/PerformanceChart';

describe('PerformanceChart', () => {
  const mockMetrics: PerformanceMetric[] = [
    {
      label: 'Read Operations',
      value: 150000,
      unit: 'ops/sec',
      maxValue: 200000,
    },
    {
      label: 'Write Operations',
      value: 100000,
      unit: 'ops/sec',
      maxValue: 200000,
    },
    {
      label: 'Latency',
      value: 0.5,
      unit: 'ms',
      maxValue: 2,
    },
  ];

  it('renders performance chart container', () => {
    render(<PerformanceChart metrics={mockMetrics} />);
    expect(screen.getByTestId('performance-chart')).toBeInTheDocument();
  });

  it('renders all metrics', () => {
    render(<PerformanceChart metrics={mockMetrics} />);
    const metricElements = screen.getAllByTestId('performance-metric');
    expect(metricElements).toHaveLength(3);
  });

  it('renders metric labels correctly', () => {
    render(<PerformanceChart metrics={mockMetrics} />);
    expect(screen.getByText('Read Operations')).toBeInTheDocument();
    expect(screen.getByText('Write Operations')).toBeInTheDocument();
    expect(screen.getByText('Latency')).toBeInTheDocument();
  });

  it('renders metric values with units', () => {
    render(<PerformanceChart metrics={mockMetrics} />);
    const metricValues = screen.getAllByTestId('metric-value');

    expect(metricValues[0]).toHaveTextContent('150,000 ops/sec');
    expect(metricValues[1]).toHaveTextContent('100,000 ops/sec');
    expect(metricValues[2]).toHaveTextContent('0.5 ms');
  });

  it('renders metric bars', () => {
    render(<PerformanceChart metrics={mockMetrics} />);
    const bars = screen.getAllByTestId('metric-bar');
    expect(bars).toHaveLength(3);
  });

  it('renders metric bars with correct width percentage', () => {
    render(<PerformanceChart metrics={mockMetrics} />);
    const bars = screen.getAllByTestId('metric-bar');

    // Read Operations: 150000/200000 = 75%
    expect(bars[0]).toHaveStyle({ width: '75%' });

    // Write Operations: 100000/200000 = 50%
    expect(bars[1]).toHaveStyle({ width: '50%' });

    // Latency: 0.5/2 = 25%
    expect(bars[2]).toHaveStyle({ width: '25%' });
  });

  it('caps bar width at 100% when value exceeds maxValue', () => {
    const overMaxMetrics: PerformanceMetric[] = [
      {
        label: 'Test Metric',
        value: 300,
        unit: 'units',
        maxValue: 100,
      },
    ];

    render(<PerformanceChart metrics={overMaxMetrics} />);
    const bar = screen.getByTestId('metric-bar');
    expect(bar).toHaveStyle({ width: '100%' });
  });

  it('renders title when provided', () => {
    render(<PerformanceChart metrics={mockMetrics} title="Benchmark Results" />);
    expect(screen.getByText('Benchmark Results')).toBeInTheDocument();
  });

  it('does not render title when not provided', () => {
    render(<PerformanceChart metrics={mockMetrics} />);
    expect(screen.queryByRole('heading', { level: 3 })).not.toBeInTheDocument();
  });

  it('has correct accessibility role', () => {
    render(<PerformanceChart metrics={mockMetrics} title="Performance Metrics" />);
    const chart = screen.getByRole('figure');
    expect(chart).toHaveAttribute('aria-label', 'Performance Metrics');
  });

  it('has progressbar role on bar containers', () => {
    render(<PerformanceChart metrics={mockMetrics} />);
    const progressbars = screen.getAllByRole('progressbar');
    expect(progressbars).toHaveLength(3);

    // Check first progressbar has correct aria attributes
    expect(progressbars[0]).toHaveAttribute('aria-valuenow', '150000');
    expect(progressbars[0]).toHaveAttribute('aria-valuemin', '0');
    expect(progressbars[0]).toHaveAttribute('aria-valuemax', '200000');
  });

  it('formats large numbers with locale formatting', () => {
    const largeMetric: PerformanceMetric[] = [
      {
        label: 'Large Number',
        value: 1234567,
        unit: 'ops',
        maxValue: 2000000,
      },
    ];

    render(<PerformanceChart metrics={largeMetric} />);
    const metricValue = screen.getByTestId('metric-value');
    // Depending on locale, should have comma separators
    expect(metricValue.textContent).toMatch(/1,234,567|1\.234\.567/);
  });

  it('renders empty state gracefully', () => {
    render(<PerformanceChart metrics={[]} />);
    expect(screen.getByTestId('performance-chart')).toBeInTheDocument();
    expect(screen.queryAllByTestId('performance-metric')).toHaveLength(0);
  });

  it('renders single metric correctly', () => {
    const singleMetric: PerformanceMetric[] = [
      {
        label: 'Single Metric',
        value: 500,
        unit: 'items',
        maxValue: 1000,
      },
    ];

    render(<PerformanceChart metrics={singleMetric} />);
    expect(screen.getByText('Single Metric')).toBeInTheDocument();
    expect(screen.getByTestId('performance-metric')).toBeInTheDocument();
  });
});
