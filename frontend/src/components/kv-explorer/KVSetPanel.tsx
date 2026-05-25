/**
 * Key-Value Explorer SET Operation Panel.
 * Owner: Scenario 9 - KV Explorer - SET Operations
 *
 * Provides a form for storing key-value pairs via the MirDB API.
 * Covers REQ-7 (write operations).
 */

import { useState, useCallback } from 'react';
import type { KVOperationRequest, KVOperationResponse } from '../../types';
import { executeOperation } from '../../api/client';

export interface KVSetPanelProps {
  onSetSuccess?: (key: string, value: string) => void;
  onSetError?: (error: string) => void;
}

export interface SetFormData {
  key: string;
  value: string;
  flags: string;
  exptime: string;
}

export interface SetFormErrors {
  key?: string;
  value?: string;
}

export type SetStatus = 'idle' | 'loading' | 'success' | 'error';

const DEFAULT_FORM_DATA: SetFormData = {
  key: '',
  value: '',
  flags: '0',
  exptime: '0',
};

export function validateSetForm(data: SetFormData): SetFormErrors {
  const errors: SetFormErrors = {};

  if (!data.key || data.key.trim() === '') {
    errors.key = 'Key is required';
  }

  if (!data.value || data.value.trim() === '') {
    errors.value = 'Value is required';
  }

  return errors;
}

export function isFormValid(errors: SetFormErrors): boolean {
  return Object.keys(errors).length === 0;
}

export function KVSetPanel({ onSetSuccess, onSetError }: KVSetPanelProps) {
  const [formData, setFormData] = useState<SetFormData>(DEFAULT_FORM_DATA);
  const [errors, setErrors] = useState<SetFormErrors>({});
  const [status, setStatus] = useState<SetStatus>('idle');
  const [statusMessage, setStatusMessage] = useState<string>('');

  const handleInputChange = useCallback(
    (field: keyof SetFormData) => (e: React.ChangeEvent<HTMLInputElement>) => {
      const newValue = e.target.value;
      setFormData((prev) => ({ ...prev, [field]: newValue }));

      if (errors[field as keyof SetFormErrors]) {
        setErrors((prev) => {
          const next = { ...prev };
          delete next[field as keyof SetFormErrors];
          return next;
        });
      }
    },
    [errors]
  );

  const handleSubmit = useCallback(
    async (e: React.FormEvent) => {
      e.preventDefault();

      const validationErrors = validateSetForm(formData);
      setErrors(validationErrors);

      if (!isFormValid(validationErrors)) {
        setStatus('error');
        setStatusMessage('Please fix the errors below.');
        onSetError?.('Please fix the errors below.');
        return;
      }

      setStatus('loading');
      setStatusMessage('');

      const request: KVOperationRequest = {
        operation: 'set',
        key: formData.key.trim(),
        value: formData.value.trim(),
        flags: parseInt(formData.flags, 10) || 0,
        exptime: parseInt(formData.exptime, 10) || 0,
      };

      try {
        const response: KVOperationResponse = await executeOperation(request);

        if (response.success) {
          setStatus('success');
          setStatusMessage('STORED');
          onSetSuccess?.(request.key!, request.value!);
        } else {
          setStatus('error');
          const errorMsg = response.error || 'Operation failed';
          setStatusMessage(errorMsg);
          onSetError?.(errorMsg);
        }
      } catch (err) {
        setStatus('error');
        const errorMsg = err instanceof Error ? err.message : 'Network error';
        setStatusMessage(errorMsg);
        onSetError?.(errorMsg);
      }
    },
    [formData, onSetSuccess, onSetError]
  );

  const handleReset = useCallback(() => {
    setFormData(DEFAULT_FORM_DATA);
    setErrors({});
    setStatus('idle');
    setStatusMessage('');
  }, []);

  return (
    <section
      className="kv-set-panel"
      aria-label="Key-Value SET Operation"
      data-testid="kv-set-panel"
    >
      <h2 className="kv-set-panel__heading" data-testid="kv-set-heading">
        Set Key-Value Pair
      </h2>

      {status === 'success' && (
        <div
          className="kv-set-panel__message kv-set-panel__message--success"
          role="status"
          data-testid="kv-set-success-message"
        >
          {statusMessage}
        </div>
      )}

      {status === 'error' && statusMessage && (
        <div
          className="kv-set-panel__message kv-set-panel__message--error"
          role="alert"
          data-testid="kv-set-error-message"
        >
          {statusMessage}
        </div>
      )}

      <form
        onSubmit={handleSubmit}
        className="kv-set-panel__form"
        data-testid="kv-set-form"
        noValidate
      >
        <div className="kv-set-panel__field">
          <label htmlFor="kv-set-key" className="kv-set-panel__label">
            Key
          </label>
          <input
            id="kv-set-key"
            type="text"
            value={formData.key}
            onChange={handleInputChange('key')}
            placeholder="Enter key..."
            className={`kv-set-panel__input${errors.key ? ' kv-set-panel__input--error' : ''}`}
            data-testid="kv-set-key-input"
            aria-invalid={errors.key ? 'true' : 'false'}
            aria-describedby={errors.key ? 'kv-set-key-error' : undefined}
            disabled={status === 'loading'}
          />
          {errors.key && (
            <span
              id="kv-set-key-error"
              className="kv-set-panel__field-error"
              role="alert"
              data-testid="kv-set-key-error"
            >
              {errors.key}
            </span>
          )}
        </div>

        <div className="kv-set-panel__field">
          <label htmlFor="kv-set-value" className="kv-set-panel__label">
            Value
          </label>
          <input
            id="kv-set-value"
            type="text"
            value={formData.value}
            onChange={handleInputChange('value')}
            placeholder="Enter value..."
            className={`kv-set-panel__input${errors.value ? ' kv-set-panel__input--error' : ''}`}
            data-testid="kv-set-value-input"
            aria-invalid={errors.value ? 'true' : 'false'}
            aria-describedby={errors.value ? 'kv-set-value-error' : undefined}
            disabled={status === 'loading'}
          />
          {errors.value && (
            <span
              id="kv-set-value-error"
              className="kv-set-panel__field-error"
              role="alert"
              data-testid="kv-set-value-error"
            >
              {errors.value}
            </span>
          )}
        </div>

        <div className="kv-set-panel__row">
          <div className="kv-set-panel__field kv-set-panel__field--half">
            <label htmlFor="kv-set-flags" className="kv-set-panel__label">
              Flags
            </label>
            <input
              id="kv-set-flags"
              type="number"
              min="0"
              value={formData.flags}
              onChange={handleInputChange('flags')}
              placeholder="0"
              className="kv-set-panel__input"
              data-testid="kv-set-flags-input"
              disabled={status === 'loading'}
            />
          </div>

          <div className="kv-set-panel__field kv-set-panel__field--half">
            <label htmlFor="kv-set-exptime" className="kv-set-panel__label">
              Expiration (seconds)
            </label>
            <input
              id="kv-set-exptime"
              type="number"
              min="0"
              value={formData.exptime}
              onChange={handleInputChange('exptime')}
              placeholder="0"
              className="kv-set-panel__input"
              data-testid="kv-set-exptime-input"
              disabled={status === 'loading'}
            />
          </div>
        </div>

        <div className="kv-set-panel__actions">
          <button
            type="submit"
            className="kv-set-panel__button kv-set-panel__button--primary"
            data-testid="kv-set-submit-button"
            disabled={status === 'loading'}
          >
            {status === 'loading' ? 'Setting...' : 'Set'}
          </button>
          <button
            type="button"
            className="kv-set-panel__button kv-set-panel__button--secondary"
            data-testid="kv-set-reset-button"
            onClick={handleReset}
            disabled={status === 'loading'}
          >
            Reset
          </button>
        </div>
      </form>
    </section>
  );
}

export default KVSetPanel;
