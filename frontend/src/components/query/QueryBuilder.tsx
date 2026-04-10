/**
 * Query builder component
 *
 * Owner: Scenario 6 - Interactive Query Builder
 *
 * Features:
 * - Command input field
 * - Execute button
 * - Response display area
 * - Query history with re-execute
 */

import React, { useState, useCallback } from 'react';
import { QueryInput } from './QueryInput';
import { QueryHistory } from './QueryHistory';
import { QueryResponse } from './QueryResponse';
import { executeQuery } from '../../api/query';
import type { QueryResponse as QueryResponseType, QueryHistoryItem } from '../../types/api';

export interface QueryBuilderProps {
  /** Custom class name */
  className?: string;
  /** Maximum history items to keep */
  maxHistoryItems?: number;
}

/**
 * Main Query Builder component for executing memcached commands.
 * Combines input, response display, and history into a single interface.
 */
export function QueryBuilder({
  className = '',
  maxHistoryItems = 20,
}: QueryBuilderProps): React.ReactElement {
  const [response, setResponse] = useState<QueryResponseType | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [history, setHistory] = useState<QueryHistoryItem[]>([]);
  const [error, setError] = useState<string | null>(null);

  const handleExecute = useCallback(
    async (command: string) => {
      setIsLoading(true);
      setError(null);

      try {
        const result = await executeQuery(command);
        setResponse(result);

        // Add to history
        const historyItem: QueryHistoryItem = {
          id: `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
          command,
          response: result.response,
          success: result.success,
          timestamp: new Date(),
        };

        setHistory((prev) => {
          const newHistory = [historyItem, ...prev];
          return newHistory.slice(0, maxHistoryItems);
        });
      } catch (err) {
        const errorMessage = err instanceof Error ? err.message : 'Unknown error occurred';
        setError(errorMessage);
        setResponse({
          response: '',
          success: false,
          error: errorMessage,
        });
      } finally {
        setIsLoading(false);
      }
    },
    [maxHistoryItems]
  );

  const handleReExecute = useCallback((command: string) => {
    handleExecute(command);
  }, [handleExecute]);

  return (
    <div
      className={`query-builder ${className}`}
      data-testid="query-builder"
      style={{
        display: 'grid',
        gridTemplateColumns: '1fr 300px',
        gap: '1.5rem',
        padding: '1.5rem',
      }}
    >
      {/* Main area: Input + Response */}
      <div
        className="query-builder__main"
        style={{
          display: 'flex',
          flexDirection: 'column',
          gap: '1.5rem',
        }}
      >
        <div
          className="query-builder__header"
          style={{
            marginBottom: '0.5rem',
          }}
        >
          <h2
            style={{
              margin: 0,
              fontSize: '1.5rem',
              fontWeight: 600,
              color: 'var(--text-primary, #1f2937)',
            }}
          >
            Query Builder
          </h2>
          <p
            style={{
              margin: '0.5rem 0 0 0',
              fontSize: '0.875rem',
              color: 'var(--text-secondary, #6b7280)',
            }}
          >
            Execute memcached commands and view responses in real-time.
          </p>
        </div>

        <QueryInput
          onExecute={handleExecute}
          isLoading={isLoading}
        />

        {error && !response && (
          <div
            className="query-builder__error"
            data-testid="query-builder-error"
            style={{
              padding: '1rem',
              backgroundColor: 'var(--bg-error-light, #fef2f2)',
              border: '1px solid var(--border-error, #fecaca)',
              borderRadius: '6px',
              color: 'var(--text-error, #dc2626)',
              fontSize: '0.875rem',
            }}
          >
            {error}
          </div>
        )}

        <div
          className="query-builder__response"
          data-testid="query-response-area"
        >
          <h3
            style={{
              margin: '0 0 0.75rem 0',
              fontSize: '0.875rem',
              fontWeight: 600,
              color: 'var(--text-primary, #1f2937)',
            }}
          >
            Response
          </h3>
          <QueryResponse response={response} isLoading={isLoading} />
        </div>
      </div>

      {/* Sidebar: History */}
      <aside
        className="query-builder__sidebar"
        style={{
          borderLeft: '1px solid var(--border-color, #e5e7eb)',
          paddingLeft: '1.5rem',
        }}
      >
        <QueryHistory
          history={history}
          onReExecute={handleReExecute}
          maxItems={10}
        />
      </aside>
    </div>
  );
}

export default QueryBuilder;
