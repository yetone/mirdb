/**
 * Query response display component
 *
 * Owner: Scenario 6 - Interactive Query Builder
 *
 * Features:
 * - Displays command response in formatted view
 * - Shows success/error status
 * - Copy response to clipboard
 * - Monospace formatting for readability
 */

import React, { useState, useCallback } from 'react';
import type { QueryResponse as QueryResponseType } from '../../types/api';

export interface QueryResponseProps {
  /** The query response to display */
  response: QueryResponseType | null;
  /** Whether a query is currently loading */
  isLoading?: boolean;
  /** Custom class name */
  className?: string;
}

/**
 * Component for displaying memcached command responses.
 */
export function QueryResponse({
  response,
  isLoading = false,
  className = '',
}: QueryResponseProps): React.ReactElement {
  const [copied, setCopied] = useState(false);

  const handleCopy = useCallback(async () => {
    if (!response?.response) return;

    try {
      await navigator.clipboard.writeText(response.response);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch (err) {
      console.error('Failed to copy to clipboard:', err);
    }
  }, [response]);

  if (isLoading) {
    return (
      <div
        className={`query-response query-response--loading ${className}`}
        data-testid="query-response"
        style={{
          padding: '1.5rem',
          textAlign: 'center',
          backgroundColor: 'var(--bg-secondary, #f9fafb)',
          borderRadius: '6px',
          border: '1px solid var(--border-color, #e5e7eb)',
        }}
      >
        <div
          className="loading-spinner"
          aria-label="Executing command"
          style={{ color: 'var(--text-secondary, #6b7280)' }}
        >
          Executing command...
        </div>
      </div>
    );
  }

  if (!response) {
    return (
      <div
        className={`query-response query-response--empty ${className}`}
        data-testid="query-response"
        style={{
          padding: '2rem',
          textAlign: 'center',
          backgroundColor: 'var(--bg-secondary, #f9fafb)',
          borderRadius: '6px',
          border: '1px dashed var(--border-color, #d1d5db)',
          color: 'var(--text-secondary, #6b7280)',
        }}
      >
        <p style={{ margin: 0, fontSize: '0.875rem' }}>
          Enter a command above and click Execute to see the response here.
        </p>
      </div>
    );
  }

  const isError = !response.success;

  return (
    <div
      className={`query-response ${isError ? 'query-response--error' : 'query-response--success'} ${className}`}
      data-testid="query-response"
      style={{
        display: 'flex',
        flexDirection: 'column',
        borderRadius: '6px',
        border: '1px solid',
        borderColor: isError
          ? 'var(--border-error, #fecaca)'
          : 'var(--border-success, #bbf7d0)',
        overflow: 'hidden',
      }}
    >
      {/* Header */}
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          padding: '0.75rem 1rem',
          backgroundColor: isError
            ? 'var(--bg-error-light, #fef2f2)'
            : 'var(--bg-success-light, #f0fdf4)',
          borderBottom: '1px solid',
          borderBottomColor: isError
            ? 'var(--border-error, #fecaca)'
            : 'var(--border-success, #bbf7d0)',
        }}
      >
        <span
          data-testid="query-response-status"
          style={{
            fontWeight: 600,
            fontSize: '0.875rem',
            color: isError
              ? 'var(--text-error, #dc2626)'
              : 'var(--text-success, #16a34a)',
          }}
        >
          {isError ? 'Error' : 'Success'}
          {response.execution_time_ms !== undefined && (
            <span
              style={{
                marginLeft: '0.5rem',
                fontWeight: 400,
                color: 'var(--text-secondary, #6b7280)',
              }}
            >
              ({response.execution_time_ms}ms)
            </span>
          )}
        </span>
        <button
          onClick={handleCopy}
          data-testid="query-response-copy"
          style={{
            padding: '0.25rem 0.5rem',
            fontSize: '0.75rem',
            backgroundColor: 'transparent',
            border: '1px solid var(--border-color, #d1d5db)',
            borderRadius: '4px',
            cursor: 'pointer',
          }}
          aria-label={copied ? 'Copied!' : 'Copy response'}
        >
          {copied ? 'Copied!' : 'Copy'}
        </button>
      </div>

      {/* Response Content */}
      <pre
        data-testid="query-response-content"
        style={{
          margin: 0,
          padding: '1rem',
          fontFamily: 'monospace',
          fontSize: '0.8125rem',
          lineHeight: 1.6,
          backgroundColor: 'var(--bg-code, #1f2937)',
          color: 'var(--text-code, #f9fafb)',
          overflow: 'auto',
          maxHeight: '400px',
          whiteSpace: 'pre-wrap',
          wordBreak: 'break-word',
        }}
      >
        {response.response || response.error || 'No response'}
      </pre>
    </div>
  );
}

export default QueryResponse;
