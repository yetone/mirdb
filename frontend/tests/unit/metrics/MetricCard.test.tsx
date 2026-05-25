/**
 * Unit tests for MetricCard component.
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import MetricCard from '../../../src/components/metrics/MetricCard';

describe('MetricCard', () => {
  it('renders with label and value', () => {
    render(<MetricCard label="Keys" value="12,345" testId="metric-keys" />);
    expect(screen.getByTestId('metric-keys')).toBeInTheDocument();
    expect(screen.getByTestId('metric-keys-label')).toHaveTextContent('Keys');
    expect(screen.getByTestId('metric-keys-value')).toHaveTextContent('12,345');
  });

  it('renders disk usage metric', () => {
    render(<MetricCard label="Disk Usage" value="450 MB" testId="metric-disk" />);
    expect(screen.getByTestId('metric-disk-label')).toHaveTextContent('Disk Usage');
    expect(screen.getByTestId('metric-disk-value')).toHaveTextContent('450 MB');
  });

  it('renders compactions metric', () => {
    render(<MetricCard label="Active Compactions" value="2 active" testId="metric-compactions" />);
    expect(screen.getByTestId('metric-compactions-label')).toHaveTextContent('Active Compactions');
    expect(screen.getByTestId('metric-compactions-value')).toHaveTextContent('2 active');
  });

  it('renders latency metric', () => {
    render(<MetricCard label="Avg Latency" value="1.2ms avg" testId="metric-latency" />);
    expect(screen.getByTestId('metric-latency-label')).toHaveTextContent('Avg Latency');
    expect(screen.getByTestId('metric-latency-value')).toHaveTextContent('1.2ms avg');
  });

  it('has CSS transition classes for smooth value changes', () => {
    render(<MetricCard label="Test" value="100" testId="metric-test" />);
    const valueEl = screen.getByTestId('metric-test-value');
    expect(valueEl).toHaveClass('transition-opacity');
    expect(valueEl).toHaveClass('duration-500');
  });
});
