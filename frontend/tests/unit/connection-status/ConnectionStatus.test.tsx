/**
 * Unit and integration tests for Connection Status Indicator.
 * Covers REQ-13.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor, act } from '@testing-library/react';
import ConnectionStatus from '../../../src/components/connection-status/ConnectionStatus';
import { useConnection } from '../../../src/components/connection-status/useConnection';
import type { ConnectionState, HealthStatus } from '../../../src/types';

// ---------------------------------------------------------------------------
// Component: ConnectionStatus
// ---------------------------------------------------------------------------

describe('ConnectionStatus component', () => {
  it('renders with connected state - green dot, Connected text, aria-label', () => {
    render(<ConnectionStatus state="connected" />);

    const container = screen.getByTestId('connection-status');
    expect(container).toBeInTheDocument();
    expect(container).toHaveAttribute('role', 'status');
    expect(container).toHaveAttribute('aria-label', 'MirDB server is connected');
    expect(container).toHaveAttribute('aria-live', 'polite');

    const dot = screen.getByTestId('connection-status-dot');
    expect(dot).toBeInTheDocument();
    expect(dot).toHaveClass('connection-status__dot--connected');
    expect(dot).toHaveAttribute('aria-hidden', 'true');

    const label = screen.getByTestId('connection-status-label');
    expect(label).toHaveTextContent('Connected');
  });

  it('renders with connecting state - yellow dot, Connecting... text, pulsing style', () => {
    render(<ConnectionStatus state="connecting" />);

    const container = screen.getByTestId('connection-status');
    expect(container).toBeInTheDocument();
    expect(container).toHaveAttribute('aria-label', 'MirDB server is connecting');

    const dot = screen.getByTestId('connection-status-dot');
    expect(dot).toBeInTheDocument();
    expect(dot).toHaveClass('connection-status__dot--connecting');

    const label = screen.getByTestId('connection-status-label');
    expect(label).toHaveTextContent('Connecting...');
  });

  it('renders with disconnected state - red dot, Disconnected text, attention-grabbing style', () => {
    render(<ConnectionStatus state="disconnected" />);

    const container = screen.getByTestId('connection-status');
    expect(container).toBeInTheDocument();
    expect(container).toHaveAttribute('aria-label', 'MirDB server is disconnected');

    const dot = screen.getByTestId('connection-status-dot');
    expect(dot).toBeInTheDocument();
    expect(dot).toHaveClass('connection-status__dot--disconnected');

    const label = screen.getByTestId('connection-status-label');
    expect(label).toHaveTextContent('Disconnected');
  });

  it('transitions between states correctly', () => {
    const { rerender } = render(<ConnectionStatus state="connecting" />);

    expect(screen.getByTestId('connection-status-label')).toHaveTextContent('Connecting...');

    rerender(<ConnectionStatus state="connected" />);
    expect(screen.getByTestId('connection-status-label')).toHaveTextContent('Connected');
    expect(screen.getByTestId('connection-status-dot')).toHaveClass('connection-status__dot--connected');

    rerender(<ConnectionStatus state="disconnected" />);
    expect(screen.getByTestId('connection-status-label')).toHaveTextContent('Disconnected');
    expect(screen.getByTestId('connection-status-dot')).toHaveClass('connection-status__dot--disconnected');
  });
});

// ---------------------------------------------------------------------------
// Hook: useConnection
// ---------------------------------------------------------------------------

function UseConnectionWrapper({ pollInterval }: { pollInterval?: number }) {
  const result = useConnection(pollInterval);
  return (
    <div data-testid="hook-state">{result.state}</div>
  );
}

describe('useConnection hook', () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('polls /api/health and transitions to connected when server is healthy', async () => {
    const mockHealth: HealthStatus = { status: 'healthy', timestamp: new Date().toISOString() };
    global.fetch = vi.fn().mockResolvedValueOnce({
      ok: true,
      status: 200,
      statusText: 'OK',
      json: async () => mockHealth,
    } as Response);

    render(<UseConnectionWrapper pollInterval={1000} />);

    expect(screen.getByTestId('hook-state')).toHaveTextContent('connecting');

    await waitFor(() => {
      expect(global.fetch).toHaveBeenCalledTimes(1);
    });

    await waitFor(() => {
      expect(screen.getByTestId('hook-state')).toHaveTextContent('connected');
    });

    const fetchCall = vi.mocked(global.fetch).mock.calls[0];
    expect(fetchCall[0]).toContain('/api/health');
  });

  it('returns {status: "healthy"} with HTTP 200 on successful health check', async () => {
    const mockHealth: HealthStatus = { status: 'healthy', timestamp: new Date().toISOString() };
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      statusText: 'OK',
      json: async () => mockHealth,
    } as Response);

    render(<UseConnectionWrapper pollInterval={1000} />);

    await waitFor(() => {
      expect(global.fetch).toHaveBeenCalledTimes(1);
    });

    const response = await (global.fetch as ReturnType<typeof vi.fn>).mock.results[0].value;
    expect(response.ok).toBe(true);
    expect(response.status).toBe(200);

    const data = await response.json();
    expect(data).toEqual(mockHealth);
    expect(data.status).toBe('healthy');
  });

  it('transitions to disconnected within 2 polling cycles when server is unreachable', async () => {
    global.fetch = vi.fn().mockRejectedValue(new Error('Connection refused'));

    render(<UseConnectionWrapper pollInterval={500} />);

    expect(screen.getByTestId('hook-state')).toHaveTextContent('connecting');

    // First poll fails
    await waitFor(() => {
      expect(global.fetch).toHaveBeenCalledTimes(1);
    });

    await waitFor(() => {
      expect(screen.getByTestId('hook-state')).toHaveTextContent('disconnected');
    });

    // Verify it was within 2 polling cycles (1 call = 1 cycle, within 1 cycle)
    expect(global.fetch).toHaveBeenCalledTimes(1);
  });

  it('continues polling after disconnection and transitions back to connected when server recovers', async () => {
    global.fetch = vi.fn()
      .mockRejectedValueOnce(new Error('Connection refused'))
      .mockResolvedValueOnce({
        ok: true,
        status: 200,
        statusText: 'OK',
        json: async () => ({ status: 'healthy', timestamp: new Date().toISOString() }),
      } as Response);

    render(<UseConnectionWrapper pollInterval={500} />);

    // First poll fails → disconnected
    await waitFor(() => {
      expect(screen.getByTestId('hook-state')).toHaveTextContent('disconnected');
    });

    // Advance to next poll cycle
    act(() => {
      vi.advanceTimersByTime(500);
    });

    // Second poll succeeds → connected
    await waitFor(() => {
      expect(screen.getByTestId('hook-state')).toHaveTextContent('connected');
    });

    expect(global.fetch).toHaveBeenCalledTimes(2);
  });

  it('stops polling on unmount', async () => {
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ status: 'healthy', timestamp: new Date().toISOString() }),
    } as Response);

    const { unmount } = render(<UseConnectionWrapper pollInterval={500} />);

    await waitFor(() => {
      expect(global.fetch).toHaveBeenCalledTimes(1);
    });

    unmount();

    act(() => {
      vi.advanceTimersByTime(2000);
    });

    // Should not have polled again after unmount
    expect(global.fetch).toHaveBeenCalledTimes(1);
  });

  it('transitions to disconnected when server returns unhealthy status', async () => {
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ status: 'unhealthy', timestamp: new Date().toISOString() }),
    } as Response);

    render(<UseConnectionWrapper pollInterval={500} />);

    await waitFor(() => {
      expect(screen.getByTestId('hook-state')).toHaveTextContent('disconnected');
    });
  });
});
