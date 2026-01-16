import { Component, ErrorInfo, ReactNode } from 'react'
import FuturisticButton from './FuturisticButton'

interface Props {
  children: ReactNode
  fallback?: ReactNode
  onError?: (error: Error, errorInfo: ErrorInfo) => void
  'data-testid'?: string
}

interface State {
  hasError: boolean
  error: Error | null
}

export default class ErrorBoundary extends Component<Props, State> {
  constructor(props: Props) {
    super(props)
    this.state = { hasError: false, error: null }
  }

  static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error }
  }

  componentDidCatch(error: Error, errorInfo: ErrorInfo): void {
    console.error('ErrorBoundary caught an error:', error, errorInfo)
    this.props.onError?.(error, errorInfo)
  }

  handleRetry = (): void => {
    this.setState({ hasError: false, error: null })
  }

  handleRefresh = (): void => {
    window.location.reload()
  }

  render(): ReactNode {
    if (this.state.hasError) {
      if (this.props.fallback) {
        return this.props.fallback
      }

      return (
        <div
          data-testid={this.props['data-testid'] || 'error-boundary-fallback'}
          className="min-h-screen flex items-center justify-center bg-base-100"
          role="alert"
          aria-live="assertive"
        >
          <div className="text-center p-8 max-w-md">
            <div className="mb-6">
              <svg
                className="w-16 h-16 mx-auto text-error"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
                xmlns="http://www.w3.org/2000/svg"
                aria-hidden="true"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"
                />
              </svg>
            </div>
            <h1 className="text-2xl font-bold text-base-content mb-4">
              Something went wrong
            </h1>
            <p className="text-base-content/70 mb-6">
              We're sorry, but something unexpected happened. Please try again or refresh the page.
            </p>
            <div className="flex flex-col sm:flex-row gap-4 justify-center">
              <FuturisticButton
                onClick={this.handleRetry}
                variant="primary"
                size="md"
                data-testid="error-retry-button"
              >
                Try Again
              </FuturisticButton>
              <FuturisticButton
                onClick={this.handleRefresh}
                variant="secondary"
                size="md"
                data-testid="error-refresh-button"
              >
                Refresh Page
              </FuturisticButton>
            </div>
            {process.env.NODE_ENV === 'development' && this.state.error && (
              <details className="mt-6 text-left">
                <summary className="cursor-pointer text-sm text-base-content/60 hover:text-base-content">
                  Error details (development only)
                </summary>
                <pre className="mt-2 p-4 bg-base-200 rounded-lg text-xs overflow-auto max-h-40 text-error">
                  {this.state.error.message}
                  {'\n'}
                  {this.state.error.stack}
                </pre>
              </details>
            )}
          </div>
        </div>
      )
    }

    return this.props.children
  }
}
