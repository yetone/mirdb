/**
 * Key-Value Explorer component.
 * Owner: Scenarios 8, 9, 10 - Key-Value Explorer operations
 *
 * Provides UI for GET, SET, DELETE operations.
 * Covers REQ-7 (read operations).
 */

import { useState, useCallback } from 'react';
import { executeOperation } from '../../api/client';
import type { KVOperationResponse } from '../../types';

export type OperationResult = {
  status: 'idle' | 'loading' | 'ok' | 'not_found' | 'error';
  value?: string;
  message?: string;
};

export interface KVExplorerProps {
  onGet?: (key: string) => Promise<KVOperationResponse>;
}

const INITIAL_RESULT: OperationResult = { status: 'idle' };

export default function KVExplorer({ onGet }: KVExplorerProps) {
  const [key, setKey] = useState('');
  const [result, setResult] = useState<OperationResult>(INITIAL_RESULT);
  const [validationError, setValidationError] = useState('');

  const handleGet = useCallback(async () => {
    if (!key.trim()) {
      setValidationError('Key is required');
      setResult(INITIAL_RESULT);
      return;
    }

    setValidationError('');
    setResult({ status: 'loading' });

    try {
      const getFn = onGet || ((k: string) => executeOperation({ op: 'get', key: k }));
      const response = await getFn(key.trim());

      if (response.status === 'ok') {
        setResult({ status: 'ok', value: response.value });
      } else if (response.status === 'not_found') {
        setResult({ status: 'not_found', message: response.message || 'Key not found' });
      } else {
        setResult({ status: 'error', message: response.message || 'An error occurred' });
      }
    } catch {
      setResult({ status: 'error', message: 'Failed to fetch value' });
    }
  }, [key, onGet]);

  const handleKeyChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setKey(e.target.value);
    if (validationError) {
      setValidationError('');
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      handleGet();
    }
  };

  return (
    <section
      className="kv-explorer"
      aria-label="Key-Value Explorer"
      data-testid="kv-explorer"
    >
      <div className="kv-explorer__container">
        <h2 className="kv-explorer__heading" data-testid="kv-explorer-heading">
          Key-Value Explorer
        </h2>

        <div className="kv-explorer__controls" data-testid="kv-explorer-controls">
          <div className="kv-explorer__field">
            <label htmlFor="kv-key" className="kv-explorer__label">
              Key
            </label>
            <input
              id="kv-key"
              type="text"
              value={key}
              onChange={handleKeyChange}
              onKeyDown={handleKeyDown}
              placeholder="Enter key..."
              className={`kv-explorer__input${validationError ? ' kv-explorer__input--error' : ''}`}
              data-testid="kv-key-input"
              aria-invalid={!!validationError}
              aria-describedby={validationError ? 'kv-key-error' : undefined}
              disabled={result.status === 'loading'}
            />
            {validationError && (
              <span
                id="kv-key-error"
                className="kv-explorer__error"
                data-testid="kv-key-error"
                role="alert"
              >
                {validationError}
              </span>
            )}
          </div>

          <button
            type="button"
            onClick={handleGet}
            disabled={result.status === 'loading'}
            className="kv-explorer__button kv-explorer__button--get"
            data-testid="kv-get-button"
          >
            Get
          </button>
        </div>

        <div className="kv-explorer__result" data-testid="kv-result">
          {result.status === 'loading' && (
            <div className="kv-explorer__loading" data-testid="kv-loading">
              Loading...
            </div>
          )}

          {result.status === 'ok' && (
            <div className="kv-explorer__result-value" data-testid="kv-result-value">
              <span className="kv-explorer__result-label">Value:</span>
              <span className="kv-explorer__result-text" data-testid="kv-result-text">
                {result.value}
              </span>
            </div>
          )}

          {result.status === 'not_found' && (
            <div className="kv-explorer__not-found" data-testid="kv-not-found">
              {result.message || 'Key not found'}
            </div>
          )}

          {result.status === 'error' && (
            <div className="kv-explorer__error-message" data-testid="kv-error-message">
              {result.message || 'An error occurred'}
            </div>
          )}
        </div>
      </div>
    </section>
  );
}
