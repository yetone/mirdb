/**
 * Unit tests for MetricsDashboard component.
 * Covers REQ-6 (Real-time Metrics Dashboard).
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor, act } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import MetricsDashboard from '../../../src/components/metrics/MetricsDashboard';
import { METRICS_POLL_INTERVAL_MS } from '../../../src/utils/constants';

describe('MetricsDashboard', () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('renders the section container', () => {
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({
        total_keys_in_memory: 12345,
        total_disk_storage_mb: 450,
        active_compactions: 2,
        request_latency_ms: 1.2,
      }),
    } as Response);

    render(<MetricsDashboard />);
    expect(screen.getByTestId('metrics-dashboard')).toBeInTheDocument();
  });

  it('renders the section title', async () => {
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({
        total_keys_in_memory: 12345,
        total_disk_storage_mb: 450,
        active_compactions: 2,
        request_latency_ms: 1.2,
      }),
    } as Response);

    render(<MetricsDashboard />);
    await waitFor(() =>
      expect(screen.getByText('Real-time Metrics')).toBeInTheDocument()
    );
  });

  it('displays all 4 metrics with mock data', async () => {
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({
        total_keys_in_memory: 12345,
        total_disk_storage_mb: 450,
        active_compactions: 2,
        request_latency_ms: 1.2,
      }),
    } as Response);

    render(<MetricsDashboard />);

    await waitFor(() =>
      expect(screen.getByTestId('metric-keys')).toBeInTheDocument()
    );

    expect(screen.getByTestId('metric-keys-value')).toHaveTextContent('12,345');
    expect(screen.getByTestId('metric-disk-value')).toHaveTextContent('450 MB');
    expect(screen.getByTestId('metric-compactions-value')).toHaveTextContent('2 active');
    expect(screen.getByTestId('metric-latency-value')).toHaveTextContent('1.2ms avg');
  });

  it('formats numbers with commas', async () => {
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({
        total_keys_in_memory: 1234567,
        total_disk_storage_mb: 450,
        active_compactions: 0,
        request_latency_ms: 1.0,
      }),
    } as Response);

    render(<MetricsDashboard />);

    await waitFor(() =>
      expect(screen.getByTestId('metric-keys-value')).toHaveTextContent('1,234,567')
    );
  });

  it('shows loading state initially', () => {
    global.fetch = vi.fn().mockImplementation(
      () =>
        new Promise((resolve) =>
          setTimeout(() =>
            resolve({
              ok: true,
              status: 200,
              json: async () => ({
                total_keys_in_memory: 100,
                total_disk_storage_mb: 50,
                active_compactions: 0,
                request_latency_ms: 1.0,
              }),
            }), 1000
          )
        )
    );

    render(<MetricsDashboard />);
    expect(screen.getByTestId('metrics-loading')).toBeInTheDocument();
  });

  it('shows live pulse indicator when data is loaded', async () => {
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({
        total_keys_in_memory: 100,
        total_disk_storage_mb: 50,
        active_compactions: 0,
        request_latency_ms: 1.0,
      }),
    } as Response);

    render(<MetricsDashboard />);

    await waitFor(() =>
      expect(screen.getByTestId('metrics-pulse')).toBeInTheDocument()
    );
    expect(screen.getByTestId('metrics-pulse')).toHaveClass('bg-green-500');
    expect(screen.getByTestId('metrics-pulse')).toHaveClass('animate-pulse');
  });

  it('uses responsive grid layout classes', async () => {
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({
        total_keys_in_memory: 100,
        total_disk_storage_mb: 50,
        active_compactions: 0,
        request_latency_ms: 1.0,
      }),
    } as Response);

    render(<MetricsDashboard />);

    await waitFor(() =>
      expect(screen.getByTestId('metrics-grid')).toBeInTheDocument()
    );

    const grid = screen.getByTestId('metrics-grid');
    expect(grid).toHaveClass('grid-cols-1');
    expect(grid).toHaveClass('sm:grid-cols-2');
    expect(grid).toHaveClass('lg:grid-cols-4');
  });

  it('shows error state when API fails', async () => {
    global.fetch = vi.fn().mockRejectedValue(
      new Error('Server unavailable')
    );

    render(<MetricsDashboard />);

    await waitFor(() =>
      expect(screen.getByTestId('metrics-error')).toBeInTheDocument()
    );
    expect(screen.getByText('Server unavailable')).toBeInTheDocument();
  });

  it('updates metric values at 2-second intervals', async () => {
    global.fetch = vi.fn()
      .mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => ({
          total_keys_in_memory: 100,
          total_disk_storage_mb: 50,
          active_compactions: 0,
          request_latency_ms: 1.0,
        }),
      } as Response)
      .mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => ({
          total_keys_in_memory: 150,
          total_disk_storage_mb: 55,
          active_compactions: 1,
          request_latency_ms: 1.5,
        }),
      } as Response);

    render(<MetricsDashboard />);

    await waitFor(() =>
      expect(screen.getByTestId('metric-keys-value')).toHaveTextContent('100')
    );

    act(() => {
      vi.advanceTimersByTime(METRICS_POLL_INTERVAL_MS);
    });

    await waitFor(() =>
      expect(screen.getByTestId('metric-keys-value')).toHaveTextContent('150')
    );
    expect(screen.getByTestId('metric-compactions-value')).toHaveTextContent('1 active');
    expect(screen.getByTestId('metric-latency-value')).toHaveTextContent('1.5ms avg');
  });

  it('does not show loading state on refresh updates', async () => {
    global.fetch = vi.fn()
      .mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => ({
          total_keys_in_memory: 100,
          total_disk_storage_mb: 50,
          active_compactions: 0,
          request_latency_ms: 1.0,
        }),
      } as Response)
      .mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => ({
          total_keys_in_memory: 200,
          total_disk_storage_mb: 60,
          active_compactions: 0,
          request_latency_ms: 1.2,
        }),
      } as Response);

    render(<MetricsDashboard />);

    await waitFor(() =>
      expect(screen.queryByTestId('metrics-loading')).not.toBeInTheDocument()
    );

    act(() => {
      vi.advanceTimersByTime(METRICS_POLL_INTERVAL_MS);
    });

    await waitFor(() =>
      expect(screen.getByTestId('metric-keys-value')).toHaveTextContent('200')
    );
    expect(screen.queryByTestId('metrics-loading')).not.toBeInTheDocument();
  });

  it('shows active compactions > 0 during simulated compaction', async () => {
    global.fetch = vi.fn()
      .mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => ({
          total_keys_in_memory: 1000,
          total_disk_storage_mb: 100,
          active_compactions: 0,
          request_latency_ms: 1.0,
        }),
      } as Response)
      .mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => ({
          total_keys_in_memory: 2000,
          total_disk_storage_mb: 150,
          active_compactions: 3,
          request_latency_ms: 2.5,
        }),
      } as Response)
      .mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => ({
          total_keys_in_memory: 2000,
          total_disk_storage_mb: 120,
          active_compactions: 0,
          request_latency_ms: 1.2,
        }),
      } as Response);

    render(<MetricsDashboard />);

    await waitFor(() =>
      expect(screen.getByTestId('metric-compactions-value')).toHaveTextContent('0 active')
    );

    act(() => {
      vi.advanceTimersByTime(METRICS_POLL_INTERVAL_MS);
    });
    await waitFor(() =>
      expect(screen.getByTestId('metric-compactions-value')).toHaveTextContent('3 active')
    );

    act(() => {
      vi.advanceTimersByTime(METRICS_POLL_INTERVAL_MS);
    });
    await waitFor(() =>
      expect(screen.getByTestId('metric-compactions-value')).toHaveTextContent('0 active')
    );
  });

  it('allows retry from error state', async () => {
    global.fetch = vi.fn()
      .mockRejectedValueOnce(new Error('Server error'))
      .mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => ({
          total_keys_in_memory: 100,
          total_disk_storage_mb: 50,
          active_compactions: 0,
          request_latency_ms: 1.0,
        }),
      } as Response);

    render(<MetricsDashboard />);

    await waitFor(() =>
      expect(screen.getByTestId('metrics-error')).toBeInTheDocument()
    );

    const user = userEvent.setup({ delay: null });
    const retryButton = screen.getByRole('button', { name: /retry/i });
    await user.click(retryButton);

    await waitFor(() =>
      expect(screen.getByTestId('metric-keys')).toBeInTheDocument()
    );
    expect(screen.queryByTestId('metrics-error')).not.toBeInTheDocument();
  });

  it('shows singular "active" when compactions count is 1', async () => {
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({
        total_keys_in_memory: 100,
        total_disk_storage_mb: 50,
        active_compactions: 1,
        request_latency_ms: 1.0,
      }),
    } as Response);

    render(<MetricsDashboard />);

    await waitFor(() =>
      expect(screen.getByTestId('metric-compactions-value')).toHaveTextContent('1 active')
    );
  });

  it('formats zero values correctly', async () => {
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({
        total_keys_in_memory: 0,
        total_disk_storage_mb: 0,
        active_compactions: 0,
        request_latency_ms: 0,
      }),
    } as Response);

    render(<MetricsDashboard />);

    await waitFor(() => {
      expect(screen.getByTestId('metric-keys-value')).toHaveTextContent('0');
      expect(screen.getByTestId('metric-disk-value')).toHaveTextContent('0 MB');
      expect(screen.getByTestId('metric-compactions-value')).toHaveTextContent('0 active');
      expect(screen.getByTestId('metric-latency-value')).toHaveTextContent('0ms avg');
    });
  });
});
