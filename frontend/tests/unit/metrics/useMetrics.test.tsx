/**
 * Unit tests for useMetrics hook.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor, act } from '@testing-library/react';
import { useMetrics } from '../../../src/components/metrics/useMetrics';
import { METRICS_POLL_INTERVAL_MS } from '../../../src/utils/constants';

function UseMetricsWrapper() {
  const result = useMetrics();
  return (
    <div>
      <div data-testid="metrics-loading">{result.loading ? 'loading' : 'done'}</div>
      <div data-testid="metrics-error">{result.error || 'none'}</div>
      <div data-testid="metrics-keys">{result.metrics?.total_keys_in_memory ?? 'null'}</div>
      <div data-testid="metrics-disk">{result.metrics?.total_disk_storage_mb ?? 'null'}</div>
      <div data-testid="metrics-compactions">{result.metrics?.active_compactions ?? 'null'}</div>
      <div data-testid="metrics-latency">{result.metrics?.request_latency_ms ?? 'null'}</div>
      <button data-testid="refresh-btn" onClick={result.refresh}>Refresh</button>
    </div>
  );
}

describe('useMetrics', () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('starts in loading state', () => {
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({
        total_keys_in_memory: 100,
        total_disk_storage_mb: 50,
        active_compactions: 0,
        request_latency_ms: 1.5,
      }),
    } as Response);

    render(<UseMetricsWrapper />);
    expect(screen.getByTestId('metrics-loading')).toHaveTextContent('loading');
  });

  it('fetches metrics on mount', async () => {
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

    render(<UseMetricsWrapper />);

    await waitFor(() => {
      expect(screen.getByTestId('metrics-loading')).toHaveTextContent('done');
    });

    expect(screen.getByTestId('metrics-keys')).toHaveTextContent('12345');
    expect(screen.getByTestId('metrics-disk')).toHaveTextContent('450');
    expect(screen.getByTestId('metrics-compactions')).toHaveTextContent('2');
    expect(screen.getByTestId('metrics-latency')).toHaveTextContent('1.2');
    expect(screen.getByTestId('metrics-error')).toHaveTextContent('none');
    expect(global.fetch).toHaveBeenCalledTimes(1);
  });

  it('polls metrics at 2-second intervals', async () => {
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
          total_disk_storage_mb: 55,
          active_compactions: 1,
          request_latency_ms: 1.5,
        }),
      } as Response);

    render(<UseMetricsWrapper />);

    await waitFor(() => {
      expect(screen.getByTestId('metrics-loading')).toHaveTextContent('done');
    });
    expect(screen.getByTestId('metrics-keys')).toHaveTextContent('100');

    act(() => {
      vi.advanceTimersByTime(METRICS_POLL_INTERVAL_MS);
    });

    await waitFor(() => {
      expect(screen.getByTestId('metrics-keys')).toHaveTextContent('200');
    });

    expect(global.fetch).toHaveBeenCalledTimes(2);
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
          total_keys_in_memory: 150,
          total_disk_storage_mb: 55,
          active_compactions: 0,
          request_latency_ms: 1.1,
        }),
      } as Response);

    render(<UseMetricsWrapper />);

    await waitFor(() => {
      expect(screen.getByTestId('metrics-loading')).toHaveTextContent('done');
    });

    act(() => {
      vi.advanceTimersByTime(METRICS_POLL_INTERVAL_MS);
    });

    await waitFor(() => {
      expect(screen.getByTestId('metrics-keys')).toHaveTextContent('150');
    });

    // Loading should still be done, not loading again
    expect(screen.getByTestId('metrics-loading')).toHaveTextContent('done');
  });

  it('sets error when fetch fails', async () => {
    global.fetch = vi.fn().mockRejectedValue(new Error('Network error'));

    render(<UseMetricsWrapper />);

    await waitFor(() => {
      expect(screen.getByTestId('metrics-loading')).toHaveTextContent('done');
    });

    expect(screen.getByTestId('metrics-error')).toHaveTextContent('Network error');
    expect(screen.getByTestId('metrics-keys')).toHaveTextContent('null');
  });

  it('manual refresh triggers fetch', async () => {
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
          total_keys_in_memory: 300,
          total_disk_storage_mb: 60,
          active_compactions: 1,
          request_latency_ms: 2.0,
        }),
      } as Response);

    render(<UseMetricsWrapper />);

    await waitFor(() => {
      expect(screen.getByTestId('metrics-loading')).toHaveTextContent('done');
    });
    expect(screen.getByTestId('metrics-keys')).toHaveTextContent('100');

    act(() => {
      screen.getByTestId('refresh-btn').click();
    });

    await waitFor(() => {
      expect(screen.getByTestId('metrics-keys')).toHaveTextContent('300');
    });

    expect(global.fetch).toHaveBeenCalledTimes(2);
  });

  it('clears interval on unmount', async () => {
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

    const { unmount } = render(<UseMetricsWrapper />);

    await waitFor(() => {
      expect(global.fetch).toHaveBeenCalledTimes(1);
    });

    unmount();

    act(() => {
      vi.advanceTimersByTime(METRICS_POLL_INTERVAL_MS * 3);
    });

    // Should not have polled again after unmount
    expect(global.fetch).toHaveBeenCalledTimes(1);
  });

  it('handles multiple polling cycles', async () => {
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
          total_keys_in_memory: 110,
          total_disk_storage_mb: 52,
          active_compactions: 0,
          request_latency_ms: 1.1,
        }),
      } as Response)
      .mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => ({
          total_keys_in_memory: 120,
          total_disk_storage_mb: 55,
          active_compactions: 1,
          request_latency_ms: 1.2,
        }),
      } as Response);

    render(<UseMetricsWrapper />);

    await waitFor(() => {
      expect(screen.getByTestId('metrics-loading')).toHaveTextContent('done');
    });
    expect(screen.getByTestId('metrics-keys')).toHaveTextContent('100');

    act(() => {
      vi.advanceTimersByTime(METRICS_POLL_INTERVAL_MS);
    });
    await waitFor(() => {
      expect(screen.getByTestId('metrics-keys')).toHaveTextContent('110');
    });

    act(() => {
      vi.advanceTimersByTime(METRICS_POLL_INTERVAL_MS);
    });
    await waitFor(() => {
      expect(screen.getByTestId('metrics-keys')).toHaveTextContent('120');
    });

    expect(global.fetch).toHaveBeenCalledTimes(3);
  });
});
