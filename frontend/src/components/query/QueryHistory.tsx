/**
 * Query history component showing previously executed commands
 *
 * Owner: Scenario 6 - Interactive Query Builder
 *
 * Features:
 * - List of previously executed commands
 * - Click to re-execute a command
 * - Shows success/failure status
 * - Timestamp for each query
 */

import React from 'react';
import type { QueryHistoryItem } from '../../types/api';

export interface QueryHistoryProps {
  /** List of history items */
  history: QueryHistoryItem[];
  /** Callback when a history item is clicked for re-execution */
  onReExecute: (command: string) => void;
  /** Maximum items to display */
  maxItems?: number;
  /** Custom class name */
  className?: string;
}

/**
 * Component displaying query history with clickable items for re-execution.
 */
export function QueryHistory({
  history,
  onReExecute,
  maxItems = 10,
  className = '',
}: QueryHistoryProps): React.ReactElement {
  const displayedHistory = history.slice(0, maxItems);

  if (history.length === 0) {
    return (
      <div
        className={`query-history query-history--empty ${className}`}
        data-testid="query-history"
        style={{
          padding: '1rem',
          textAlign: 'center',
          color: 'var(--text-secondary, #6b7280)',
          fontSize: '0.875rem',
        }}
      >
        No command history yet. Execute a command to see it here.
      </div>
    );
  }

  return (
    <div
      className={`query-history ${className}`}
      data-testid="query-history"
      style={{
        display: 'flex',
        flexDirection: 'column',
        gap: '0.5rem',
      }}
    >
      <h3
        style={{
          margin: '0 0 0.5rem 0',
          fontSize: '0.875rem',
          fontWeight: 600,
          color: 'var(--text-primary, #1f2937)',
        }}
      >
        History ({history.length})
      </h3>
      <ul
        style={{
          listStyle: 'none',
          margin: 0,
          padding: 0,
          display: 'flex',
          flexDirection: 'column',
          gap: '0.25rem',
        }}
        data-testid="query-history-list"
        role="list"
        aria-label="Query history"
      >
        {displayedHistory.map((item) => (
          <li key={item.id}>
            <button
              onClick={() => onReExecute(item.command)}
              data-testid={`history-item-${item.id}`}
              title={`Re-execute: ${item.command}`}
              style={{
                width: '100%',
                padding: '0.5rem 0.75rem',
                textAlign: 'left',
                backgroundColor: item.success
                  ? 'var(--bg-success-light, #f0fdf4)'
                  : 'var(--bg-error-light, #fef2f2)',
                border: '1px solid',
                borderColor: item.success
                  ? 'var(--border-success, #bbf7d0)'
                  : 'var(--border-error, #fecaca)',
                borderRadius: '4px',
                cursor: 'pointer',
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                gap: '0.5rem',
              }}
              aria-label={`Re-execute command: ${item.command}`}
            >
              <code
                style={{
                  fontFamily: 'monospace',
                  fontSize: '0.8125rem',
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  whiteSpace: 'nowrap',
                  flex: 1,
                }}
              >
                {item.command}
              </code>
              <span
                style={{
                  fontSize: '0.75rem',
                  color: 'var(--text-secondary, #6b7280)',
                  whiteSpace: 'nowrap',
                }}
              >
                {formatTimestamp(item.timestamp)}
              </span>
            </button>
          </li>
        ))}
      </ul>
      {history.length > maxItems && (
        <p
          style={{
            margin: '0.5rem 0 0 0',
            fontSize: '0.75rem',
            color: 'var(--text-secondary, #6b7280)',
            textAlign: 'center',
          }}
        >
          Showing {maxItems} of {history.length} commands
        </p>
      )}
    </div>
  );
}

function formatTimestamp(date: Date): string {
  const now = new Date();
  const diff = now.getTime() - date.getTime();
  const seconds = Math.floor(diff / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);

  if (seconds < 60) {
    return 'just now';
  } else if (minutes < 60) {
    return `${minutes}m ago`;
  } else if (hours < 24) {
    return `${hours}h ago`;
  } else {
    return date.toLocaleDateString();
  }
}

export default QueryHistory;
