/**
 * Unit tests for MetricsDashboard component
 *
 * Owner: Scenario 2 - Real-time System Metrics Dashboard
 *
 * Test Case 2: Render MetricsDashboard component with mock data
 * Expected: Component displays formatted metrics:
 *   '12,345 keys', '1.2GB / 2GB memory', '94% hit rate', '42 connections'
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MetricsDashboard } from '../../../../src/components/dashboard/MetricsDashboard';
import type { MetricsResponse } from '../../../../src/types/api';

// Mock the useMetrics hook
vi.mock('../../../../src/hooks/useMetrics', () => ({
  useMetrics: vi.fn(() => ({
    data: null,
    loading: true,
    error: null,
    refresh: vi.fn(),
    lastUpdated: null,
  })),
}));

// Import after mocking
import { useMetrics } from '../../../../src/hooks/useMetrics';

const mockMetrics: MetricsResponse = {
  total_keys: 12345,
  memory_used_bytes: 1289748480, // ~1.2 GB
  memory_available_bytes: 2147483648, // 2 GB
  cache_hit_rate: 0.94,
  active_connections: 42,
  uptime_seconds: 86400,
  ls_levels: [],
};

describe('MetricsDashboard', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should render loading state initially', () => {
    vi.mocked(useMetrics).mockReturnValue({
      data: null,
      loading: true,
      error: null,
      refresh: vi.fn(),
      lastUpdated: null,
    });

    render(<MetricsDashboard />);

    expect(screen.getByTestId('metrics-dashboard-loading')).toBeInTheDocument();
    expect(screen.getByText('Loading metrics...')).toBeInTheDocument();
  });

  it('should render error state when fetch fails', () => {
    vi.mocked(useMetrics).mockReturnValue({
      data: null,
      loading: false,
      error: 'Network error',
      refresh: vi.fn(),
      lastUpdated: null,
    });

    render(<MetricsDashboard />);

    expect(screen.getByTestId('metrics-dashboard-error')).toBeInTheDocument();
    expect(screen.getByText(/Failed to load metrics/)).toBeInTheDocument();
    expect(screen.getByText('Retry')).toBeInTheDocument();
  });

  it('should display formatted metrics with mock data', () => {
    vi.mocked(useMetrics).mockReturnValue({
      data: mockMetrics,
      loading: false,
      error: null,
      refresh: vi.fn(),
      lastUpdated: new Date(),
    });

    render(<MetricsDashboard />);

    // Check that the dashboard is rendered
    expect(screen.getByTestId('metrics-dashboard')).toBeInTheDocument();

    // Test Case 2: Verify formatted metrics display
    // Check total keys: '12,345 keys'
    const keysCard = screen.getByTestId('metric-total-keys');
    expect(keysCard).toBeInTheDocument();
    expect(keysCard).toHaveTextContent('12,345');
    expect(keysCard).toHaveTextContent('keys');

    // Check memory: '1.2GB / 2GB memory' (1.20 GB / 2.00 GB)
    const memoryCard = screen.getByTestId('metric-memory');
    expect(memoryCard).toBeInTheDocument();
    expect(memoryCard).toHaveTextContent('1.20 GB');
    expect(memoryCard).toHaveTextContent('2.00 GB');

    // Check hit rate: '94% hit rate'
    const hitRateCard = screen.getByTestId('metric-hit-rate');
    expect(hitRateCard).toBeInTheDocument();
    expect(hitRateCard).toHaveTextContent('94%');
    expect(hitRateCard).toHaveTextContent('hit rate');

    // Check connections: '42 connections'
    const connectionsCard = screen.getByTestId('metric-connections');
    expect(connectionsCard).toBeInTheDocument();
    expect(connectionsCard).toHaveTextContent('42');
    expect(connectionsCard).toHaveTextContent('connections');
  });

  it('should display uptime metric', () => {
    vi.mocked(useMetrics).mockReturnValue({
      data: mockMetrics,
      loading: false,
      error: null,
      refresh: vi.fn(),
      lastUpdated: new Date(),
    });

    render(<MetricsDashboard />);

    // Check uptime: 86400 seconds = 1 day
    const uptimeCard = screen.getByTestId('metric-uptime');
    expect(uptimeCard).toBeInTheDocument();
    expect(uptimeCard).toHaveTextContent('1d');
  });

  it('should use initialData prop when provided', () => {
    vi.mocked(useMetrics).mockReturnValue({
      data: null,
      loading: false,
      error: null,
      refresh: vi.fn(),
      lastUpdated: null,
    });

    const customMetrics: MetricsResponse = {
      ...mockMetrics,
      total_keys: 99999,
    };

    render(<MetricsDashboard initialData={customMetrics} />);

    expect(screen.getByTestId('metrics-dashboard')).toBeInTheDocument();
    expect(screen.getByTestId('metric-total-keys')).toHaveTextContent('99,999');
  });

  it('should show stale warning when error occurs but data exists', () => {
    vi.mocked(useMetrics).mockReturnValue({
      data: mockMetrics,
      loading: false,
      error: 'Failed to refresh',
      refresh: vi.fn(),
      lastUpdated: new Date(),
    });

    render(<MetricsDashboard />);

    // Should still show the dashboard with data
    expect(screen.getByTestId('metrics-dashboard')).toBeInTheDocument();

    // But also show the stale warning
    expect(screen.getByText(/Warning: Failed to refresh metrics/)).toBeInTheDocument();
  });

  it('should display last updated time', () => {
    const now = new Date('2024-01-15T10:30:00');

    vi.mocked(useMetrics).mockReturnValue({
      data: mockMetrics,
      loading: false,
      error: null,
      refresh: vi.fn(),
      lastUpdated: now,
    });

    render(<MetricsDashboard />);

    expect(screen.getByText(/Last updated:/)).toBeInTheDocument();
  });
});
