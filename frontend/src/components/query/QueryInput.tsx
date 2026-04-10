/**
 * Query input component for entering memcached commands
 *
 * Owner: Scenario 6 - Interactive Query Builder
 *
 * Features:
 * - Text input field for command entry
 * - Execute button
 * - Loading state while executing
 * - Keyboard shortcut (Ctrl+Enter or Cmd+Enter to execute)
 */

import React, { useState, useCallback, KeyboardEvent } from 'react';

export interface QueryInputProps {
  /** Callback when command should be executed */
  onExecute: (command: string) => void;
  /** Whether a query is currently executing */
  isLoading?: boolean;
  /** Initial value for the input */
  initialValue?: string;
  /** Placeholder text */
  placeholder?: string;
  /** Custom class name */
  className?: string;
}

/**
 * Input component for entering and executing memcached commands.
 */
export function QueryInput({
  onExecute,
  isLoading = false,
  initialValue = '',
  placeholder = 'Enter memcached command (e.g., stats, get mykey)',
  className = '',
}: QueryInputProps): React.ReactElement {
  const [command, setCommand] = useState(initialValue);

  const handleExecute = useCallback(() => {
    const trimmed = command.trim();
    if (trimmed && !isLoading) {
      onExecute(trimmed);
    }
  }, [command, isLoading, onExecute]);

  const handleKeyDown = useCallback(
    (e: KeyboardEvent<HTMLTextAreaElement>) => {
      // Execute on Ctrl+Enter or Cmd+Enter
      if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
        e.preventDefault();
        handleExecute();
      }
    },
    [handleExecute]
  );

  return (
    <div
      className={`query-input ${className}`}
      data-testid="query-input"
      style={{
        display: 'flex',
        flexDirection: 'column',
        gap: '0.75rem',
      }}
    >
      <label
        htmlFor="query-command"
        style={{
          fontWeight: 600,
          fontSize: '0.875rem',
          color: 'var(--text-primary, #1f2937)',
        }}
      >
        Command
      </label>
      <textarea
        id="query-command"
        value={command}
        onChange={(e) => setCommand(e.target.value)}
        onKeyDown={handleKeyDown}
        placeholder={placeholder}
        disabled={isLoading}
        data-testid="query-command-input"
        rows={3}
        style={{
          width: '100%',
          padding: '0.75rem',
          fontSize: '0.875rem',
          fontFamily: 'monospace',
          border: '1px solid var(--border-color, #d1d5db)',
          borderRadius: '6px',
          resize: 'vertical',
          backgroundColor: isLoading ? 'var(--bg-disabled, #f3f4f6)' : 'var(--bg-input, white)',
        }}
        aria-label="Memcached command input"
      />
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
        }}
      >
        <span
          style={{
            fontSize: '0.75rem',
            color: 'var(--text-secondary, #6b7280)',
          }}
        >
          Press Ctrl+Enter to execute
        </span>
        <button
          onClick={handleExecute}
          disabled={isLoading || !command.trim()}
          data-testid="query-execute-button"
          style={{
            padding: '0.5rem 1rem',
            fontSize: '0.875rem',
            fontWeight: 500,
            backgroundColor: isLoading || !command.trim() ? '#9ca3af' : '#2563eb',
            color: 'white',
            border: 'none',
            borderRadius: '6px',
            cursor: isLoading || !command.trim() ? 'not-allowed' : 'pointer',
            display: 'flex',
            alignItems: 'center',
            gap: '0.5rem',
          }}
          aria-label={isLoading ? 'Executing command...' : 'Execute command'}
        >
          {isLoading ? (
            <>
              <span className="spinner" aria-hidden="true">...</span>
              Executing
            </>
          ) : (
            'Execute'
          )}
        </button>
      </div>
    </div>
  );
}

export default QueryInput;
