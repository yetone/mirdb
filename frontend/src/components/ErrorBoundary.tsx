import { Component } from 'react'
import type { ErrorInfo, ReactNode } from 'react'

interface Props {
  children: ReactNode
  fallback?: ReactNode
  sectionName?: string
}

interface State {
  hasError: boolean
  error: Error | null
}

/**
 * ErrorBoundary component that catches JavaScript errors in child components.
 * Displays a fallback UI instead of crashing the entire component tree.
 */
export default class ErrorBoundary extends Component<Props, State> {
  constructor(props: Props) {
    super(props)
    this.state = { hasError: false, error: null }
  }

  static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error }
  }

  componentDidCatch(error: Error, errorInfo: ErrorInfo): void {
    // Log error to console in development
    console.error('ErrorBoundary caught an error:', error, errorInfo)
  }

  render(): ReactNode {
    if (this.state.hasError) {
      // If a custom fallback is provided, use it
      if (this.props.fallback) {
        return this.props.fallback
      }

      // Default fallback UI
      return (
        <div
          data-testid="error-boundary-fallback"
          className="flex items-center justify-center p-8 bg-base-200 rounded-lg m-4"
          role="alert"
          aria-live="polite"
        >
          <div className="text-center">
            <div className="text-error mb-2">
              <svg
                xmlns="http://www.w3.org/2000/svg"
                className="h-12 w-12 mx-auto"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"
                />
              </svg>
            </div>
            <h3 className="text-lg font-semibold text-base-content mb-1">
              {this.props.sectionName
                ? `Unable to load ${this.props.sectionName}`
                : 'Something went wrong'}
            </h3>
            <p className="text-base-content/70 text-sm">
              This section encountered an error. The rest of the page should still work.
            </p>
          </div>
        </div>
      )
    }

    return this.props.children
  }
}
