import { Component, ReactNode } from 'react'
import { AlertTriangle, RefreshCw } from 'lucide-react'

interface ErrorBoundaryProps {
  children: ReactNode
  fallback?: ReactNode
}

interface ErrorBoundaryState {
  hasError: boolean
  error: Error | null
}

export class ErrorBoundary extends Component<ErrorBoundaryProps, ErrorBoundaryState> {
  constructor(props: ErrorBoundaryProps) {
    super(props)
    this.state = { hasError: false, error: null }
  }

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return { hasError: true, error }
  }

  componentDidCatch(error: Error, errorInfo: React.ErrorInfo): void {
    console.error('ErrorBoundary caught an error:', error, errorInfo)
  }

  handleRetry = (): void => {
    this.setState({ hasError: false, error: null })
  }

  render(): ReactNode {
    if (this.state.hasError) {
      if (this.props.fallback) {
        return this.props.fallback
      }

      return (
        <div
          data-testid="error-boundary-fallback"
          className="min-h-[50vh] flex items-center justify-center px-4"
          role="alert"
          aria-live="assertive"
        >
          <div className="text-center max-w-md">
            <AlertTriangle
              className="w-16 h-16 text-warning mx-auto mb-4"
              aria-hidden="true"
            />
            <h2 className="text-2xl font-bold text-base-content mb-2">
              Something went wrong
            </h2>
            <p className="text-base-content/70 mb-6">
              We encountered an unexpected error. Please try again or refresh the page.
            </p>
            <button
              onClick={this.handleRetry}
              className="btn btn-primary gap-2"
              data-testid="error-retry-button"
            >
              <RefreshCw className="w-4 h-4" aria-hidden="true" />
              Try Again
            </button>
          </div>
        </div>
      )
    }

    return this.props.children
  }
}

export default ErrorBoundary
