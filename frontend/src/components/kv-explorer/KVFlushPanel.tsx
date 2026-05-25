/**
 * Key-Value Explorer FLUSH Operation Panel.
 * Owner: Scenario 10 - KV Explorer - DELETE and FLUSH Operations
 *
 * Provides a UI for flushing all data with confirmation dialog.
 * Covers Appendix A (flush_all).
 */

import { useState, useCallback } from 'react';
import { executeOperation } from '../../api/client';
import type { KVOperationResponse } from '../../types';

export interface KVFlushPanelProps {
  onFlushSuccess?: () => void;
  onFlushError?: (error: string) => void;
}

export type FlushStatus = 'idle' | 'confirming' | 'loading' | 'success' | 'error';

export function KVFlushPanel({ onFlushSuccess, onFlushError }: KVFlushPanelProps) {
  const [status, setStatus] = useState<FlushStatus>('idle');
  const [statusMessage, setStatusMessage] = useState('');

  const handleFlushClick = useCallback(() => {
    setStatus('confirming');
    setStatusMessage('');
  }, []);

  const handleConfirm = useCallback(async () => {
    setStatus('loading');
    setStatusMessage('');

    try {
      const response: KVOperationResponse = await executeOperation({
        op: 'flush_all',
      });

      if (response.status === 'ok') {
        setStatus('success');
        setStatusMessage('All data has been flushed.');
        onFlushSuccess?.();
      } else {
        setStatus('error');
        const msg = response.message || 'Flush operation failed';
        setStatusMessage(msg);
        onFlushError?.(msg);
      }
    } catch (err) {
      setStatus('error');
      const msg = err instanceof Error ? err.message : 'Network error';
      setStatusMessage(msg);
      onFlushError?.(msg);
    }
  }, [onFlushSuccess, onFlushError]);

  const handleCancel = useCallback(() => {
    setStatus('idle');
    setStatusMessage('');
  }, []);

  return (
    <section
      className="kv-flush-panel"
      aria-label="Key-Value FLUSH Operation"
      data-testid="kv-flush-panel"
    >
      <h2 className="kv-flush-panel__heading" data-testid="kv-flush-heading">
        Flush All Data
      </h2>

      <p className="kv-flush-panel__warning" data-testid="kv-flush-warning">
        This will permanently remove all keys from the store. This action cannot be undone.
      </p>

      {status === 'success' && (
        <div
          className="kv-flush-panel__message kv-flush-panel__message--success"
          role="status"
          data-testid="kv-flush-success-message"
        >
          {statusMessage}
        </div>
      )}

      {status === 'error' && statusMessage && (
        <div
          className="kv-flush-panel__message kv-flush-panel__message--error"
          role="alert"
          data-testid="kv-flush-error-message"
        >
          {statusMessage}
        </div>
      )}

      {status === 'confirming' && (
        <div
          className="kv-flush-panel__dialog"
          role="alertdialog"
          aria-modal="true"
          aria-labelledby="kv-flush-dialog-title"
          data-testid="kv-flush-confirmation-dialog"
        >
          <h3 id="kv-flush-dialog-title" data-testid="kv-flush-dialog-title">
            Confirm Flush
          </h3>
          <p data-testid="kv-flush-dialog-message">
            Are you sure you want to delete all keys? This action cannot be undone.
          </p>
          <div className="kv-flush-panel__dialog-actions" data-testid="kv-flush-dialog-actions">
            <button
              type="button"
              onClick={handleConfirm}
              className="kv-flush-panel__button kv-flush-panel__button--confirm"
              data-testid="kv-flush-confirm-button"
            >
              Yes, Flush All
            </button>
            <button
              type="button"
              onClick={handleCancel}
              className="kv-flush-panel__button kv-flush-panel__button--cancel"
              data-testid="kv-flush-cancel-button"
            >
              Cancel
            </button>
          </div>
        </div>
      )}

      {status !== 'confirming' && (
        <button
          type="button"
          onClick={handleFlushClick}
          disabled={status === 'loading'}
          className="kv-flush-panel__button kv-flush-panel__button--flush"
          data-testid="kv-flush-button"
        >
          {status === 'loading' ? 'Flushing...' : 'Flush All'}
        </button>
      )}
    </section>
  );
}

export default KVFlushPanel;
