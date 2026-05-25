/**
 * Retry Button component.
 * Owner: Scenario 17 - Error Handling and Graceful Degradation
 *
 * Provides a retry button with loading state for error recovery.
 * Covers NFR-4 (retry functionality).
 */

import React, { useState, useCallback } from 'react';

export interface RetryButtonProps {
  onRetry: () => void | Promise<void>;
  label?: string;
  loadingLabel?: string;
  disabled?: boolean;
  'data-testid'?: string;
}

const RetryButton: React.FC<RetryButtonProps> = ({
  onRetry,
  label = 'Retry',
  loadingLabel = 'Retrying...',
  disabled = false,
  'data-testid': testId = 'retry-button',
}) => {
  const [isRetrying, setIsRetrying] = useState(false);

  const handleClick = useCallback(async () => {
    if (isRetrying || disabled) return;

    setIsRetrying(true);
    try {
      await onRetry();
    } finally {
      setIsRetrying(false);
    }
  }, [onRetry, isRetrying, disabled]);

  return (
    <button
      type="button"
      className="retry-button"
      onClick={handleClick}
      disabled={disabled || isRetrying}
      data-testid={testId}
      aria-busy={isRetrying}
    >
      {isRetrying ? (
        <>
          <span
            className="retry-button__spinner"
            aria-hidden="true"
            data-testid={`${testId}-spinner`}
          />
          <span>{loadingLabel}</span>
        </>
      ) : (
        <>
          <span className="retry-button__icon" aria-hidden="true">
            ↻
          </span>
          <span>{label}</span>
        </>
      )}
    </button>
  );
};

export default RetryButton;
