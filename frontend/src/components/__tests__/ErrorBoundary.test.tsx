import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import ErrorBoundary from '../ErrorBoundary'

/**
 * Tests for ErrorBoundary component
 * Scenario: Error Handling - Graceful Degradation
 * Test Case 3: Check React Error Boundary implementation
 */

// Component that throws an error for testing
const ThrowError = ({ shouldThrow }: { shouldThrow: boolean }) => {
  if (shouldThrow) {
    throw new Error('Test error')
  }
  return <div data-testid="child-component">Child rendered successfully</div>
}

describe('ErrorBoundary', () => {
  // Suppress console.error during tests since we expect errors
  const originalConsoleError = console.error
  beforeEach(() => {
    console.error = vi.fn()
  })
  afterEach(() => {
    console.error = originalConsoleError
  })

  describe('Test Case 3: Homepage has error boundary to catch component errors', () => {
    it('renders children when no error occurs', () => {
      render(
        <ErrorBoundary>
          <ThrowError shouldThrow={false} />
        </ErrorBoundary>
      )

      expect(screen.getByTestId('child-component')).toBeInTheDocument()
      expect(screen.getByText('Child rendered successfully')).toBeInTheDocument()
    })

    it('catches errors and displays fallback UI', () => {
      render(
        <ErrorBoundary>
          <ThrowError shouldThrow={true} />
        </ErrorBoundary>
      )

      // Child should not be rendered
      expect(screen.queryByTestId('child-component')).not.toBeInTheDocument()

      // Fallback UI should be displayed
      expect(screen.getByTestId('error-boundary-fallback')).toBeInTheDocument()
      expect(screen.getByText('Something went wrong')).toBeInTheDocument()
      expect(
        screen.getByText(/We're sorry, but something unexpected happened/)
      ).toBeInTheDocument()
    })

    it('displays a refresh button that reloads the page', () => {
      // Mock window.location.reload
      const reloadMock = vi.fn()
      Object.defineProperty(window, 'location', {
        value: { reload: reloadMock },
        writable: true,
      })

      render(
        <ErrorBoundary>
          <ThrowError shouldThrow={true} />
        </ErrorBoundary>
      )

      const refreshButton = screen.getByTestId('error-boundary-refresh-button')
      expect(refreshButton).toBeInTheDocument()
      expect(refreshButton).toHaveTextContent('Refresh Page')

      fireEvent.click(refreshButton)
      expect(reloadMock).toHaveBeenCalledTimes(1)
    })

    it('fallback UI has proper accessibility role', () => {
      render(
        <ErrorBoundary>
          <ThrowError shouldThrow={true} />
        </ErrorBoundary>
      )

      const fallback = screen.getByTestId('error-boundary-fallback')
      expect(fallback).toHaveAttribute('role', 'alert')
    })

    it('renders custom fallback when provided', () => {
      const customFallback = <div data-testid="custom-fallback">Custom Error Message</div>

      render(
        <ErrorBoundary fallback={customFallback}>
          <ThrowError shouldThrow={true} />
        </ErrorBoundary>
      )

      expect(screen.getByTestId('custom-fallback')).toBeInTheDocument()
      expect(screen.getByText('Custom Error Message')).toBeInTheDocument()
      expect(screen.queryByTestId('error-boundary-fallback')).not.toBeInTheDocument()
    })

    it('calls onError callback when error occurs', () => {
      const onErrorMock = vi.fn()

      render(
        <ErrorBoundary onError={onErrorMock}>
          <ThrowError shouldThrow={true} />
        </ErrorBoundary>
      )

      expect(onErrorMock).toHaveBeenCalledTimes(1)
      expect(onErrorMock).toHaveBeenCalledWith(
        expect.any(Error),
        expect.objectContaining({
          componentStack: expect.any(String),
        })
      )
    })

    it('logs error to console', () => {
      render(
        <ErrorBoundary>
          <ThrowError shouldThrow={true} />
        </ErrorBoundary>
      )

      expect(console.error).toHaveBeenCalled()
    })

    it('does not affect other children when one throws', () => {
      // Test that ErrorBoundary isolates errors
      render(
        <div>
          <div data-testid="sibling-before">Sibling Before</div>
          <ErrorBoundary>
            <ThrowError shouldThrow={true} />
          </ErrorBoundary>
          <div data-testid="sibling-after">Sibling After</div>
        </div>
      )

      // Siblings outside the boundary should still render
      expect(screen.getByTestId('sibling-before')).toBeInTheDocument()
      expect(screen.getByTestId('sibling-after')).toBeInTheDocument()

      // Error boundary fallback should be displayed
      expect(screen.getByTestId('error-boundary-fallback')).toBeInTheDocument()
    })
  })
})
