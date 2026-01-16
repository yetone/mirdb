import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import ErrorBoundary from './ErrorBoundary'

// Mock console.error to suppress error boundary logging in tests
const originalConsoleError = console.error
beforeEach(() => {
  console.error = vi.fn()
})
afterEach(() => {
  console.error = originalConsoleError
})

// Component that throws an error on demand
const ThrowError = ({ shouldThrow = false }: { shouldThrow?: boolean }) => {
  if (shouldThrow) {
    throw new Error('Test error message')
  }
  return <div data-testid="child-content">Normal content</div>
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

describe('ErrorBoundary', () => {
  describe('Test Case 3: Verify React error boundary - Component errors are caught and display fallback UI', () => {
    it('renders children when there is no error', () => {
      render(
        <ErrorBoundary>
          <ThrowError shouldThrow={false} />
        </ErrorBoundary>
      )

      expect(screen.getByTestId('child-content')).toBeInTheDocument()
      expect(screen.getByText('Normal content')).toBeInTheDocument()
    })

    it('renders fallback UI when child component throws an error', () => {
      render(
        <ErrorBoundary>
          <ThrowError shouldThrow={true} />
        </ErrorBoundary>
      )

      // Should display error boundary UI instead of child content
      expect(screen.queryByTestId('child-content')).not.toBeInTheDocument()
      expect(screen.getByTestId('error-boundary-fallback')).toBeInTheDocument()
    })

    it('displays a helpful error message in fallback UI', () => {
      render(
        <ErrorBoundary>
          <ThrowError shouldThrow={true} />
        </ErrorBoundary>
      )

      // Should show user-friendly error message
      expect(screen.getByTestId('error-boundary-fallback')).toHaveTextContent(/something went wrong/i)
    })

    it('provides a retry button in fallback UI', () => {
      render(
        <ErrorBoundary>
          <ThrowError shouldThrow={true} />
        </ErrorBoundary>
      )

      const retryButton = screen.getByTestId('error-boundary-retry')
      expect(retryButton).toBeInTheDocument()
      expect(retryButton).toHaveAttribute('type', 'button')
    })

    it('retry button triggers page reload when clicked', () => {
      const reloadMock = vi.fn()
      Object.defineProperty(window, 'location', {
        writable: true,
        value: { reload: reloadMock }
      })

      render(
        <ErrorBoundary>
          <ThrowError shouldThrow={true} />
        </ErrorBoundary>
      )

      const retryButton = screen.getByTestId('error-boundary-retry')
      fireEvent.click(retryButton)

      expect(reloadMock).toHaveBeenCalled()
    })

    it('catches different types of errors (TypeError)', () => {
      render(
        <ErrorBoundary>
          <ThrowSpecificError errorType="type" />
        </ErrorBoundary>
      )

      expect(screen.getByTestId('error-boundary-fallback')).toBeInTheDocument()
    })

    it('catches different types of errors (ReferenceError)', () => {
      render(
        <ErrorBoundary>
          <ThrowSpecificError errorType="reference" />
        </ErrorBoundary>
      )

      expect(screen.getByTestId('error-boundary-fallback')).toBeInTheDocument()
    })

    it('fallback UI has proper styling with visible container', () => {
      render(
        <ErrorBoundary>
          <ThrowError shouldThrow={true} />
        </ErrorBoundary>
      )

      const fallback = screen.getByTestId('error-boundary-fallback')
      expect(fallback).toHaveClass('flex')
      expect(fallback).toHaveClass('items-center')
      expect(fallback).toHaveClass('justify-center')
    })

    it('fallback UI is accessible with proper heading hierarchy', () => {
      render(
        <ErrorBoundary>
          <ThrowError shouldThrow={true} />
        </ErrorBoundary>
      )

      const heading = screen.getByRole('heading')
      expect(heading).toBeInTheDocument()
    })

    it('accepts custom fallback component', () => {
      const CustomFallback = () => (
        <div data-testid="custom-fallback">Custom error UI</div>
      )

      render(
        <ErrorBoundary fallback={<CustomFallback />}>
          <ThrowError shouldThrow={true} />
        </ErrorBoundary>
      )

      expect(screen.getByTestId('custom-fallback')).toBeInTheDocument()
      expect(screen.getByText('Custom error UI')).toBeInTheDocument()
    })

    it('renders nested children correctly without errors', () => {
      render(
        <ErrorBoundary>
          <div data-testid="parent">
            <div data-testid="nested-child">
              <ThrowError shouldThrow={false} />
            </div>
          </div>
        </ErrorBoundary>
      )

      expect(screen.getByTestId('parent')).toBeInTheDocument()
      expect(screen.getByTestId('nested-child')).toBeInTheDocument()
      expect(screen.getByTestId('child-content')).toBeInTheDocument()
    })

    it('catches errors from deeply nested components', () => {
      render(
        <ErrorBoundary>
          <div>
            <div>
              <ThrowError shouldThrow={true} />
            </div>
          </div>
        </ErrorBoundary>
      )

      expect(screen.getByTestId('error-boundary-fallback')).toBeInTheDocument()
    })
  })
})
