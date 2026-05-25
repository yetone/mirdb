/**
 * Key-Value Explorer DELETE and FLUSH Operation Panel.
 * Owner: Scenario 10 - KV Explorer - DELETE and FLUSH Operations
 *
 * Provides a UI for deleting individual keys and flushing all data.
 * Covers REQ-7 (delete operations) and Appendix A (flush_all).
 */

import { useState, useCallback } from 'react';
import type { KVOperationResponse } from '../../types';
import { executeOperation } from '../../api/client';

export interface KVDeletePanelProps {
  onDeleteSuccess?: (key: string) => void;
  onDeleteNotFound?: (key: string) => void;
  onDeleteError?: (error: string) => void;
  onFlushSuccess?: () => void;
  onFlushError?: (error: string) => void;
}

export type DeleteStatus = 'idle' | 'loading' | 'deleted' | 'not_found' | 'error';
export type FlushStatus = 'idle' | 'confirming' | 'loading' | 'success' | 'error';

export interface DeleteResult {
  status: DeleteStatus;
  message?: string;
}

export interface FlushResult {
  status: FlushStatus;
  message?: string;
}

export function validateDeleteKey(key: string): string | undefined {
  if (!key || key.trim() === '') {
    return 'Key is required';
  }
  return undefined;
}

export function KVDeletePanel({
  onDeleteSuccess,
  onDeleteNotFound,
  onDeleteError,
  onFlushSuccess,
  onFlushError,
}: KVDeletePanelProps) {
  const [deleteKey, setDeleteKey] = useState('');
  const [deleteError, setDeleteError] = useState('');
  const [deleteResult, setDeleteResult] = useState<DeleteResult>({ status: 'idle' });
  const [flushResult, setFlushResult] = useState<FlushResult>({ status: 'idle' });

  const handleDeleteKeyChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    setDeleteKey(e.target.value);
    if (deleteError) {
      setDeleteError('');
    }
    if (deleteResult.status !== 'idle') {
      setDeleteResult({ status: 'idle' });
    }
  }, [deleteError, deleteResult.status]);

  const handleDelete = useCallback(async () => {
    const validationError = validateDeleteKey(deleteKey);
    if (validationError) {
      setDeleteError(validationError);
      setDeleteResult({ status: 'idle' });
      return;
    }

    setDeleteError('');
    setDeleteResult({ status: 'loading' });

    const key = deleteKey.trim();

    try {
      const response: KVOperationResponse = await executeOperation({ op: 'delete', key });

      if (response.status === 'deleted') {
        setDeleteResult({ status: 'deleted', message: response.message });
        onDeleteSuccess?.(key);
      } else if (response.status === 'not_found') {
        setDeleteResult({
          status: 'not_found',
          message: response.message || 'Key not found',
        });
        onDeleteNotFound?.(key);
      } else {
        const msg = response.message || 'Delete operation failed';
        setDeleteResult({ status: 'error', message: msg });
        onDeleteError?.(msg);
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Network error';
      setDeleteResult({ status: 'error', message: msg });
      onDeleteError?.(msg);
    }
  }, [deleteKey, onDeleteSuccess, onDeleteNotFound, onDeleteError]);

  const handleDeleteKeyDown = useCallback((e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      handleDelete();
    }
  }, [handleDelete]);

  const handleFlushClick = useCallback(() => {
    setFlushResult({ status: 'confirming' });
  }, []);

  const handleFlushConfirm = useCallback(async () => {
    setFlushResult({ status: 'loading' });

    try {
      const response: KVOperationResponse = await executeOperation({ op: 'flush_all' });

      if (response.status === 'ok') {
        setFlushResult({ status: 'success', message: response.message });
        onFlushSuccess?.();
      } else {
        const msg = response.message || 'Flush operation failed';
        setFlushResult({ status: 'error', message: msg });
        onFlushError?.(msg);
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Network error';
      setFlushResult({ status: 'error', message: msg });
      onFlushError?.(msg);
    }
  }, [onFlushSuccess, onFlushError]);

  const handleFlushCancel = useCallback(() => {
    setFlushResult({ status: 'idle' });
  }, []);

  const isDeleteLoading = deleteResult.status === 'loading';
  const isFlushLoading = flushResult.status === 'loading';
  const isAnyLoading = isDeleteLoading || isFlushLoading;

  return (
    <section
      className="kv-delete-panel"
      aria-label="Key-Value DELETE and FLUSH Operations"
      data-testid="kv-delete-panel"
    >
      <h2 className="kv-delete-panel__heading" data-testid="kv-delete-heading">
        Delete Key-Value Pair
      </h2>

      {/* Delete Result Messages */}
      {deleteResult.status === 'deleted' && (
        <div
          className="kv-delete-panel__message kv-delete-panel__message--success"
          role="status"
          data-testid="kv-delete-success-message"
        >
          {deleteResult.message || 'Key deleted successfully'}
        </div>
      )}

      {deleteResult.status === 'not_found' && (
        <div
          className="kv-delete-panel__message kv-delete-panel__message--not-found"
          role="status"
          data-testid="kv-delete-not-found-message"
        >
          {deleteResult.message || 'Key not found'}
        </div>
      )}

      {deleteResult.status === 'error' && (
        <div
          className="kv-delete-panel__message kv-delete-panel__message--error"
          role="alert"
          data-testid="kv-delete-error-message"
        >
          {deleteResult.message || 'An error occurred'}
        </div>
      )}

      {/* Flush Result Messages */}
      {flushResult.status === 'success' && (
        <div
          className="kv-delete-panel__message kv-delete-panel__message--success"
          role="status"
          data-testid="kv-flush-success-message"
        >
          {flushResult.message || 'All data flushed successfully'}
        </div>
      )}

      {flushResult.status === 'error' && flushResult.message && (
        <div
          className="kv-delete-panel__message kv-delete-panel__message--error"
          role="alert"
          data-testid="kv-flush-error-message"
        >
          {flushResult.message}
        </div>
      )}

      {/* Delete Key Input */}
      <div className="kv-delete-panel__section" data-testid="kv-delete-section">
        <div className="kv-delete-panel__field">
          <label htmlFor="kv-delete-key" className="kv-delete-panel__label">
            Key to Delete
          </label>
          <input
            id="kv-delete-key"
            type="text"
            value={deleteKey}
            onChange={handleDeleteKeyChange}
            onKeyDown={handleDeleteKeyDown}
            placeholder="Enter key to delete..."
            className={`kv-delete-panel__input${deleteError ? ' kv-delete-panel__input--error' : ''}`}
            data-testid="kv-delete-key-input"
            aria-invalid={!!deleteError}
            aria-describedby={deleteError ? 'kv-delete-key-error' : undefined}
            disabled={isAnyLoading}
          />
          {deleteError && (
            <span
              id="kv-delete-key-error"
              className="kv-delete-panel__field-error"
              data-testid="kv-delete-key-error"
              role="alert"
            >
              {deleteError}
            </span>
          )}
        </div>

        <button
          type="button"
          onClick={handleDelete}
          disabled={isAnyLoading}
          className="kv-delete-panel__button kv-delete-panel__button--delete"
          data-testid="kv-delete-button"
        >
          {isDeleteLoading ? 'Deleting...' : 'Delete'}
        </button>
      </div>

      {/* Flush All Section */}
      <div className="kv-delete-panel__section" data-testid="kv-flush-section">
        <div className="kv-delete-panel__flush-info">
          <span className="kv-delete-panel__flush-label">Danger Zone</span>
          <p className="kv-delete-panel__flush-description">
            Remove all key-value pairs from the store. This action cannot be undone.
          </p>
        </div>

        <button
          type="button"
          onClick={handleFlushClick}
          disabled={isAnyLoading || flushResult.status === 'confirming'}
          className="kv-delete-panel__button kv-delete-panel__button--flush"
          data-testid="kv-flush-button"
        >
          Flush All
        </button>
      </div>

      {/* Flush Confirmation Dialog */}
      {flushResult.status === 'confirming' && (
        <div
          className="kv-delete-panel__dialog-overlay"
          data-testid="kv-flush-dialog-overlay"
          role="dialog"
          aria-modal="true"
          aria-labelledby="flush-dialog-title"
        >
          <div className="kv-delete-panel__dialog">
            <h3
              id="flush-dialog-title"
              className="kv-delete-panel__dialog-title"
              data-testid="kv-flush-dialog-title"
            >
              Confirm Flush All
            </h3>
            <p
              className="kv-delete-panel__dialog-text"
              data-testid="kv-flush-dialog-text"
            >
              Are you sure you want to delete all key-value pairs? This action is
              irreversible.
            </p>
            <div className="kv-delete-panel__dialog-actions">
              <button
                type="button"
                onClick={handleFlushConfirm}
                className="kv-delete-panel__button kv-delete-panel__button--confirm"
                data-testid="kv-flush-confirm-button"
              >
                Yes, Flush All
              </button>
              <button
                type="button"
                onClick={handleFlushCancel}
                className="kv-delete-panel__button kv-delete-panel__button--cancel"
                data-testid="kv-flush-cancel-button"
              >
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}
    </section>
  );
}

export default KVDeletePanel;
