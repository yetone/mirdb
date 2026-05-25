/**
 * Error Boundary component.
 * Owner: Scenario 17 - Error Handling and Graceful Degradation
 *
 * Catches React rendering errors and displays a user-friendly fallback UI.
 * Covers NFR-4 (graceful degradation).
 */

import React, { Component, type ReactNode } from 'react';
import ErrorMessage from './ErrorMessage';
import RetryButton from './RetryButton';

export interface ErrorBoundaryProps {
  children: ReactNode;
  fallback?: ReactNode;
  onError?: (error: Error, errorInfo: React.ErrorInfo) => void;
}

export interface ErrorBoundaryState {
  hasError: boolean;
  error: Error | null;
}

export class ErrorBoundary extends Component<ErrorBoundaryProps, ErrorBoundaryState> {
  constructor(props: ErrorBoundaryProps) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, errorInfo: React.ErrorInfo): void {
    this.props.onError?.(error, errorInfo);
  }

  handleRetry = (): void => {
    this.setState({ hasError: false, error: null });
  };

  render(): ReactNode {
    if (this.state.hasError) {
      if (this.props.fallback) {
        return this.props.fallback;
      }

      return (
        <div
          className="error-boundary"
          data-testid="error-boundary"
          role="alert"
          aria-label="Application error"
        >
          <ErrorMessage
            title="Something went wrong"
            message="An unexpected error occurred. Please try again or refresh the page."
            variant="critical"
            data-testid="error-boundary-message"
          />
          <div className="error-boundary__actions">
            <RetryButton
              onRetry={this.handleRetry}
              label="Try Again"
              data-testid="error-boundary-retry"
            />
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}

export default ErrorBoundary;
