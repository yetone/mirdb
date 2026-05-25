/**
 * Error Message component.
 * Owner: Scenario 17 - Error Handling and Graceful Degradation
 *
 * Displays user-friendly error messages with appropriate styling.
 * Covers NFR-4 (clear user feedback).
 */

import React from 'react';

export type ErrorVariant = 'error' | 'warning' | 'info' | 'critical';

export interface ErrorMessageProps {
  title?: string;
  message: string;
  variant?: ErrorVariant;
  'data-testid'?: string;
}

const VARIANT_STYLES: Record<ErrorVariant, { icon: string; containerClass: string; titleClass: string; messageClass: string }> = {
  error: {
    icon: '⚠️',
    containerClass: 'error-message error-message--error',
    titleClass: 'error-message__title error-message__title--error',
    messageClass: 'error-message__text error-message__text--error',
  },
  warning: {
    icon: '⚡',
    containerClass: 'error-message error-message--warning',
    titleClass: 'error-message__title error-message__title--warning',
    messageClass: 'error-message__text error-message__text--warning',
  },
  info: {
    icon: 'ℹ️',
    containerClass: 'error-message error-message--info',
    titleClass: 'error-message__title error-message__title--info',
    messageClass: 'error-message__text error-message__text--info',
  },
  critical: {
    icon: '🔴',
    containerClass: 'error-message error-message--critical',
    titleClass: 'error-message__title error-message__title--critical',
    messageClass: 'error-message__text error-message__text--critical',
  },
};

const ErrorMessage: React.FC<ErrorMessageProps> = ({
  title,
  message,
  variant = 'error',
  'data-testid': testId = 'error-message',
}) => {
  const styles = VARIANT_STYLES[variant];

  return (
    <div className={styles.containerClass} data-testid={testId} role="alert">
      <span className="error-message__icon" aria-hidden="true">
        {styles.icon}
      </span>
      <div className="error-message__content">
        {title && (
          <h3 className={styles.titleClass} data-testid={`${testId}-title`}>
            {title}
          </h3>
        )}
        <p className={styles.messageClass} data-testid={`${testId}-text`}>
          {message}
        </p>
      </div>
    </div>
  );
};

export default ErrorMessage;
