import './ContentErrorState.css'

/**
 * ContentErrorState component displays a graceful error state
 * when content fails to load from the API.
 */
export interface ContentErrorStateProps {
  /** Error message to display */
  message?: string
  /** Callback function for retry button */
  onRetry?: () => void
  /** Whether to show the retry button */
  showRetry?: boolean
}

export const ContentErrorState: React.FC<ContentErrorStateProps> = ({
  message = 'Unable to load content. Please try again later.',
  onRetry,
  showRetry = true,
}) => {
  return (
    <div
      className="content-error-state"
      data-testid="content-error-state"
      role="alert"
      aria-live="polite"
    >
      <svg
        className="error-icon"
        xmlns="http://www.w3.org/2000/svg"
        width="48"
        height="48"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
        aria-hidden="true"
      >
        <circle cx="12" cy="12" r="10" />
        <line x1="12" y1="8" x2="12" y2="12" />
        <line x1="12" y1="16" x2="12.01" y2="16" />
      </svg>
      <p className="error-message" data-testid="error-message">
        {message}
      </p>
      {showRetry && onRetry && (
        <button
          type="button"
          className="retry-button"
          data-testid="retry-button"
          onClick={onRetry}
        >
          Try Again
        </button>
      )}
    </div>
  )
}
