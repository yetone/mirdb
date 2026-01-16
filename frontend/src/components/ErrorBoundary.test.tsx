import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import ErrorBoundary from './ErrorBoundary'

// Component that throws an error for testing
function ThrowingComponent({ shouldThrow }: { shouldThrow: boolean }) {
  if (shouldThrow) {
    throw new Error('Test error message')
  }
  return <div data-testid="child-content">Child content rendered successfully</div>
}

// Component that throws specific error types
const ThrowSpecificError = ({ errorType }: { errorType: string }) => {
  if (errorType === 'type') {
    throw new TypeError('Type error occurred')
  }
  if (errorType === 'reference') {
    throw new ReferenceError('Reference error occurred')
  }
  return <div>No error</div>
}

// Wrapper to provide required context
function TestWrapper({ children }: { children: React.ReactNode }) {
  return <MemoryRouter>{children}</MemoryRouter>
}

describe('ErrorBoundary', () => {
  // Suppress console.error during tests
  const originalError = console.error
  beforeEach(() => {
    console.error = vi.fn()
  })
  afterEach(() => {
    console.error = originalError
  })

  describe('Normal rendering', () => {
    it('should render children when no error occurs', () => {
      render(
        <TestWrapper>
          <ErrorBoundary>
            <ThrowingComponent shouldThrow={false} />
          </ErrorBoundary>
        </TestWrapper>
      )

      expect(screen.getByTestId('child-content')).toBeInTheDocument()
      expect(screen.getByText('Child content rendered successfully')).toBeInTheDocument()
    })

    it('should not show error UI when children render successfully', () => {
      render(
        <TestWrapper>
          <ErrorBoundary>
            <ThrowingComponent shouldThrow={false} />
          </ErrorBoundary>
        </TestWrapper>
      )

      expect(screen.queryByTestId('error-boundary-fallback')).not.toBeInTheDocument()
    })

    it('renders nested children correctly without errors', () => {
      render(
        <TestWrapper>
          <ErrorBoundary>
            <div data-testid="parent">
              <div data-testid="nested-child">
                <ThrowingComponent shouldThrow={false} />
              </div>
            </div>
          </ErrorBoundary>
        </TestWrapper>
      )

      expect(screen.getByTestId('parent')).toBeInTheDocument()
      expect(screen.getByTestId('nested-child')).toBeInTheDocument()
      expect(screen.getByTestId('child-content')).toBeInTheDocument()
    })
  })

  describe('Error catching', () => {
    it('should catch errors and display fallback UI', () => {
      render(
        <TestWrapper>
          <ErrorBoundary>
            <ThrowingComponent shouldThrow={true} />
          </ErrorBoundary>
        </TestWrapper>
      )

      expect(screen.getByTestId('error-boundary-fallback')).toBeInTheDocument()
      expect(screen.getByText('Something went wrong')).toBeInTheDocument()
    })

    it('should display helpful error message', () => {
      render(
        <TestWrapper>
          <ErrorBoundary>
            <ThrowingComponent shouldThrow={true} />
          </ErrorBoundary>
        </TestWrapper>
      )

      expect(screen.getByText(/We're sorry, but something unexpected happened/)).toBeInTheDocument()
    })

    it('should call onError callback when error occurs', () => {
      const onError = vi.fn()

      render(
        <TestWrapper>
          <ErrorBoundary onError={onError}>
            <ThrowingComponent shouldThrow={true} />
          </ErrorBoundary>
        </TestWrapper>
      )

      expect(onError).toHaveBeenCalledTimes(1)
      expect(onError).toHaveBeenCalledWith(
        expect.any(Error),
        expect.objectContaining({ componentStack: expect.any(String) })
      )
    })

    it('should log error to console', () => {
      render(
        <TestWrapper>
          <ErrorBoundary>
            <ThrowingComponent shouldThrow={true} />
          </ErrorBoundary>
        </TestWrapper>
      )

      expect(console.error).toHaveBeenCalled()
    })

    it('catches different types of errors (TypeError)', () => {
      render(
        <TestWrapper>
          <ErrorBoundary>
            <ThrowSpecificError errorType="type" />
          </ErrorBoundary>
        </TestWrapper>
      )

      expect(screen.getByTestId('error-boundary-fallback')).toBeInTheDocument()
    })

    it('catches different types of errors (ReferenceError)', () => {
      render(
        <TestWrapper>
          <ErrorBoundary>
            <ThrowSpecificError errorType="reference" />
          </ErrorBoundary>
        </TestWrapper>
      )

      expect(screen.getByTestId('error-boundary-fallback')).toBeInTheDocument()
    })

    it('catches errors from deeply nested components', () => {
      render(
        <TestWrapper>
          <ErrorBoundary>
            <div>
              <div>
                <ThrowingComponent shouldThrow={true} />
              </div>
            </div>
          </ErrorBoundary>
        </TestWrapper>
      )

      expect(screen.getByTestId('error-boundary-fallback')).toBeInTheDocument()
    })
  })

  describe('Custom fallback', () => {
    it('should render custom fallback when provided', () => {
      const customFallback = <div data-testid="custom-fallback">Custom error message</div>

      render(
        <TestWrapper>
          <ErrorBoundary fallback={customFallback}>
            <ThrowingComponent shouldThrow={true} />
          </ErrorBoundary>
        </TestWrapper>
      )

      expect(screen.getByTestId('custom-fallback')).toBeInTheDocument()
      expect(screen.getByText('Custom error message')).toBeInTheDocument()
    })
  })

  describe('Recovery actions', () => {
    it('should display Try Again button', () => {
      render(
        <TestWrapper>
          <ErrorBoundary>
            <ThrowingComponent shouldThrow={true} />
          </ErrorBoundary>
        </TestWrapper>
      )

      expect(screen.getByTestId('error-retry-button')).toBeInTheDocument()
      expect(screen.getByText('Try Again')).toBeInTheDocument()
    })

    it('should display Refresh Page button', () => {
      render(
        <TestWrapper>
          <ErrorBoundary>
            <ThrowingComponent shouldThrow={true} />
          </ErrorBoundary>
        </TestWrapper>
      )

      expect(screen.getByTestId('error-refresh-button')).toBeInTheDocument()
      expect(screen.getByText('Refresh Page')).toBeInTheDocument()
    })

    it('should attempt to retry rendering when Try Again is clicked', () => {
      // Using a stateful approach to test retry
      let throwError = true
      function ConditionalThrow() {
        if (throwError) {
          throw new Error('Test error')
        }
        return <div data-testid="recovered-content">Recovered!</div>
      }

      render(
        <TestWrapper>
          <ErrorBoundary>
            <ConditionalThrow />
          </ErrorBoundary>
        </TestWrapper>
      )

      // Error UI should be shown
      expect(screen.getByTestId('error-boundary-fallback')).toBeInTheDocument()

      // Set flag to not throw on next render
      throwError = false

      // Click Try Again
      fireEvent.click(screen.getByTestId('error-retry-button'))

      // Should show recovered content
      expect(screen.getByTestId('recovered-content')).toBeInTheDocument()
    })
  })

  describe('Accessibility', () => {
    it('should have proper ARIA attributes on error UI', () => {
      render(
        <TestWrapper>
          <ErrorBoundary>
            <ThrowingComponent shouldThrow={true} />
          </ErrorBoundary>
        </TestWrapper>
      )

      const errorContainer = screen.getByTestId('error-boundary-fallback')
      expect(errorContainer).toHaveAttribute('role', 'alert')
      expect(errorContainer).toHaveAttribute('aria-live', 'assertive')
    })

    it('should render proper heading hierarchy', () => {
      render(
        <TestWrapper>
          <ErrorBoundary>
            <ThrowingComponent shouldThrow={true} />
          </ErrorBoundary>
        </TestWrapper>
      )

      const heading = screen.getByRole('heading', { level: 1 })
      expect(heading).toHaveTextContent('Something went wrong')
    })

    it('should have accessible buttons', () => {
      render(
        <TestWrapper>
          <ErrorBoundary>
            <ThrowingComponent shouldThrow={true} />
          </ErrorBoundary>
        </TestWrapper>
      )

      const retryButton = screen.getByTestId('error-retry-button')
      const refreshButton = screen.getByTestId('error-refresh-button')

      expect(retryButton).toBeEnabled()
      expect(refreshButton).toBeEnabled()
    })

    it('fallback UI has proper styling with visible container', () => {
      render(
        <TestWrapper>
          <ErrorBoundary>
            <ThrowingComponent shouldThrow={true} />
          </ErrorBoundary>
        </TestWrapper>
      )

      const fallback = screen.getByTestId('error-boundary-fallback')
      expect(fallback).toHaveClass('flex')
      expect(fallback).toHaveClass('items-center')
      expect(fallback).toHaveClass('justify-center')
    })
  })

  describe('Custom testid', () => {
    it('should accept custom data-testid', () => {
      render(
        <TestWrapper>
          <ErrorBoundary data-testid="custom-error-boundary">
            <ThrowingComponent shouldThrow={true} />
          </ErrorBoundary>
        </TestWrapper>
      )

      expect(screen.getByTestId('custom-error-boundary')).toBeInTheDocument()
    })
  })
})
