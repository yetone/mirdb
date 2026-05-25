/**
 * Key-Value Explorer DELETE Operation Panel.
 * Owner: Scenario 10 - KV Explorer - DELETE Operations
 *
 * Provides a UI for deleting individual keys via the MirDB API.
 * Covers REQ-7 (delete operations).
 */

import { useState, useCallback } from 'react';
import { executeOperation } from '../../api/client';
import type { KVOperationResponse } from '../../types';

export interface KVDeletePanelProps {
  onDeleteSuccess?: (key: string) => void;
  onDeleteError?: (error: string) => void;
}

export type DeleteStatus = 'idle' | 'loading' | 'deleted' | 'not_found' | 'error';

export function KVDeletePanel({ onDeleteSuccess, onDeleteError }: KVDeletePanelProps) {
  const [key, setKey] = useState('');
  const [validationError, setValidationError] = useState('');
  const [status, setStatus] = useState<DeleteStatus>('idle');
  const [statusMessage, setStatusMessage] = useState('');

  const handleKeyChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    setKey(e.target.value);
    if (validationError) {
      setValidationError('');
    }
  }, [validationError]);

  const handleDelete = useCallback(async () => {
    const trimmedKey = key.trim();
    if (!trimmedKey) {
      setValidationError('Key is required');
      setStatus('idle');
      setStatusMessage('');
      return;
    }

    setValidationError('');
    setStatus('loading');
    setStatusMessage('');

    try {
      const response: KVOperationResponse = await executeOperation({
        op: 'delete',
        key: trimmedKey,
      });

      if (response.status === 'deleted') {
        setStatus('deleted');
        setStatusMessage('DELETED');
        onDeleteSuccess?.(trimmedKey);
      } else if (response.status === 'not_found') {
        setStatus('not_found');
        const msg = response.message || 'Key not found';
        setStatusMessage(msg);
        onDeleteError?.(msg);
      } else {
        setStatus('error');
        const msg = response.message || 'Delete operation failed';
        setStatusMessage(msg);
        onDeleteError?.(msg);
      }
    } catch (err) {
      setStatus('error');
      const msg = err instanceof Error ? err.message : 'Network error';
      setStatusMessage(msg);
      onDeleteError?.(msg);
    }
  }, [key, onDeleteSuccess, onDeleteError]);

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      handleDelete();
    }
  };

  return (
    <section
      className="kv-delete-panel"
      aria-label="Key-Value DELETE Operation"
      data-testid="kv-delete-panel"
    >
      <h2 className="kv-delete-panel__heading" data-testid="kv-delete-heading">
        Delete Key
      </h2>

      {status === 'deleted' && (
        <div
          className="kv-delete-panel__message kv-delete-panel__message--success"
          role="status"
          data-testid="kv-delete-success-message"
        >
          {statusMessage}
        </div>
      )}

      {status === 'not_found' && (
        <div
          className="kv-delete-panel__message kv-delete-panel__message--not-found"
          role="status"
          data-testid="kv-delete-not-found-message"
        >
          {statusMessage}
        </div>
      )}

      {status === 'error' && statusMessage && (
        <div
          className="kv-delete-panel__message kv-delete-panel__message--error"
          role="alert"
          data-testid="kv-delete-error-message"
        >
          {statusMessage}
        </div>
      )}

      <div className="kv-delete-panel__controls" data-testid="kv-delete-controls">
        <div className="kv-delete-panel__field">
          <label htmlFor="kv-delete-key" className="kv-delete-panel__label">
            Key
          </label>
          <input
            id="kv-delete-key"
            type="text"
            value={key}
            onChange={handleKeyChange}
            onKeyDown={handleKeyDown}
            placeholder="Enter key to delete..."
            className={`kv-delete-panel__input${validationError ? ' kv-delete-panel__input--error' : ''}`}
            data-testid="kv-delete-key-input"
            aria-invalid={!!validationError}
            aria-describedby={validationError ? 'kv-delete-key-error' : undefined}
            disabled={status === 'loading'}
          />
          {validationError && (
            <span
              id="kv-delete-key-error"
              className="kv-delete-panel__error"
              data-testid="kv-delete-key-error"
              role="alert"
            >
              {validationError}
            </span>
          )}
        </div>

        <button
          type="button"
          onClick={handleDelete}
          disabled={status === 'loading'}
          className="kv-delete-panel__button kv-delete-panel__button--delete"
          data-testid="kv-delete-button"
        >
          {status === 'loading' ? 'Deleting...' : 'Delete'}
        </button>
      </div>
    </section>
  );
}

export default KVDeletePanel;
