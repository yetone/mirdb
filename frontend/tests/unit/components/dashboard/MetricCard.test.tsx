/**
 * Unit tests for MetricCard component
 *
 * Owner: Scenario 2 - Real-time System Metrics Dashboard
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MetricCard } from '../../../../src/components/dashboard/MetricCard';

describe('MetricCard', () => {
  it('should render with label and value', () => {
    render(<MetricCard label="Total Keys" value="12,345" testId="test-card" />);

    const card = screen.getByTestId('test-card');
    expect(card).toBeInTheDocument();
    expect(card).toHaveTextContent('Total Keys');
    expect(card).toHaveTextContent('12,345');
  });

  it('should render with secondary value', () => {
    render(
      <MetricCard
        label="Memory Usage"
        value="1.2 GB"
        secondaryValue="/ 2 GB"
        testId="memory-card"
      />
    );

    const card = screen.getByTestId('memory-card');
    expect(card).toHaveTextContent('1.2 GB');
    expect(card).toHaveTextContent('/ 2 GB');
  });

  it('should render with icon when provided', () => {
    render(
      <MetricCard
        label="Connections"
        value="42"
        icon="network"
        testId="connections-card"
      />
    );

    const card = screen.getByTestId('connections-card');
    expect(card).toHaveTextContent('network');
  });

  it('should apply custom className', () => {
    render(
      <MetricCard
        label="Test"
        value="100"
        className="custom-class"
        testId="custom-card"
      />
    );

    const card = screen.getByTestId('custom-card');
    expect(card).toHaveClass('metric-card');
    expect(card).toHaveClass('custom-class');
  });

  it('should render without secondary value', () => {
    render(<MetricCard label="Uptime" value="1d 2h" testId="uptime-card" />);

    const card = screen.getByTestId('uptime-card');
    expect(card).toHaveTextContent('Uptime');
    expect(card).toHaveTextContent('1d 2h');
    // Should not have the secondary value class content
    const secondary = card.querySelector('.metric-card__secondary');
    expect(secondary).toBeNull();
  });
});
