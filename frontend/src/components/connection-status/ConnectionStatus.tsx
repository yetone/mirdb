/**
 * Connection Status Indicator component.
 * Owner: Scenario 5 - Connection Status Indicator
 *
 * Displays a color-coded status indicator showing MirDB server reachability.
 * Covers REQ-13.
 */

import type { ConnectionState } from '../../types';

export interface ConnectionStatusProps {
  state: ConnectionState;
}

const STATE_CONFIG: Record<
  ConnectionState,
  {
    dotClass: string;
    label: string;
    ariaLabel: string;
    testId: string;
  }
> = {
  connected: {
    dotClass: 'connection-status__dot--connected',
    label: 'Connected',
    ariaLabel: 'MirDB server is connected',
    testId: 'connection-status-connected',
  },
  connecting: {
    dotClass: 'connection-status__dot--connecting',
    label: 'Connecting...',
    ariaLabel: 'MirDB server is connecting',
    testId: 'connection-status-connecting',
  },
  disconnected: {
    dotClass: 'connection-status__dot--disconnected',
    label: 'Disconnected',
    ariaLabel: 'MirDB server is disconnected',
    testId: 'connection-status-disconnected',
  },
};

export default function ConnectionStatus({ state }: ConnectionStatusProps) {
  const config = STATE_CONFIG[state];

  return (
    <div
      className="connection-status"
      data-testid="connection-status"
      role="status"
      aria-label={config.ariaLabel}
      aria-live="polite"
    >
      <span
        className={`connection-status__dot ${config.dotClass}`}
        data-testid="connection-status-dot"
        aria-hidden="true"
      />
      <span className="connection-status__label" data-testid="connection-status-label">
        {config.label}
      </span>
    </div>
  );
}
