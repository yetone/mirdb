import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import ErrorBoundary from '../components/ErrorBoundary'

// Component that throws an error during render
function ThrowingComponent({ shouldThrow = true }: { shouldThrow?: boolean }) {
  if (shouldThrow) {
    throw new Error('Test error from ThrowingComponent')
  }
  return <div data-testid="working-component">Component rendered successfully</div>
}

// Component that works normally
function WorkingComponent() {
  return <div data-testid="working-component">Working component content</div>
}

describe('ErrorBoundary', () => {
  // Suppress console.error during these tests since we expect errors
  const originalConsoleError = console.error
  beforeEach(() => {
    console.error = vi.fn()
  })
  afterEach(() => {
    console.error = originalConsoleError
  })

  describe('Test Case 1: Component throws render error - Error boundary catches error and displays fallback UI', () => {
    it('should catch errors thrown by child components', () => {
      render(
        <ErrorBoundary>
          <ThrowingComponent />
        </ErrorBoundary>
      )

      // The throwing component should not be rendered
      expect(screen.queryByTestId('working-component')).not.toBeInTheDocument()

      // The fallback UI should be displayed
      const fallback = screen.getByTestId('error-boundary-fallback')
      expect(fallback).toBeInTheDocument()
    })

    it('should display default fallback UI when error occurs', () => {
      render(
        <ErrorBoundary>
          <ThrowingComponent />
        </ErrorBoundary>
      )

      // Check that the fallback UI contains appropriate error messaging
      expect(screen.getByText('Something went wrong')).toBeInTheDocument()
      expect(
        screen.getByText('This section encountered an error. The rest of the page should still work.')
      ).toBeInTheDocument()
    })

    it('should display section-specific error message when sectionName is provided', () => {
      render(
        <ErrorBoundary sectionName="Features">
          <ThrowingComponent />
        </ErrorBoundary>
      )

      expect(screen.getByText('Unable to load Features')).toBeInTheDocument()
    })

    it('should display custom fallback when provided', () => {
      const customFallback = <div data-testid="custom-fallback">Custom error message</div>

      render(
        <ErrorBoundary fallback={customFallback}>
          <ThrowingComponent />
        </ErrorBoundary>
      )

      expect(screen.getByTestId('custom-fallback')).toBeInTheDocument()
      expect(screen.getByText('Custom error message')).toBeInTheDocument()
      // Default fallback should not be shown
      expect(screen.queryByTestId('error-boundary-fallback')).not.toBeInTheDocument()
    })

    it('should render fallback with role="alert" for accessibility', () => {
      render(
        <ErrorBoundary>
          <ThrowingComponent />
        </ErrorBoundary>
      )

      const fallback = screen.getByRole('alert')
      expect(fallback).toBeInTheDocument()
    })

    it('should render fallback with aria-live="polite" for screen readers', () => {
      render(
        <ErrorBoundary>
          <ThrowingComponent />
        </ErrorBoundary>
      )

      const fallback = screen.getByTestId('error-boundary-fallback')
      expect(fallback).toHaveAttribute('aria-live', 'polite')
    })

    it('should log error to console when error is caught', () => {
      render(
        <ErrorBoundary>
          <ThrowingComponent />
        </ErrorBoundary>
      )

      expect(console.error).toHaveBeenCalled()
    })
  })

  describe('Normal operation without errors', () => {
    it('should render children when no error occurs', () => {
      render(
        <ErrorBoundary>
          <WorkingComponent />
        </ErrorBoundary>
      )

      expect(screen.getByTestId('working-component')).toBeInTheDocument()
      expect(screen.getByText('Working component content')).toBeInTheDocument()
      // Fallback should not be displayed
      expect(screen.queryByTestId('error-boundary-fallback')).not.toBeInTheDocument()
    })

    it('should render multiple children when no error occurs', () => {
      render(
        <ErrorBoundary>
          <div data-testid="child-1">Child 1</div>
          <div data-testid="child-2">Child 2</div>
        </ErrorBoundary>
      )

      expect(screen.getByTestId('child-1')).toBeInTheDocument()
      expect(screen.getByTestId('child-2')).toBeInTheDocument()
    })
  })
})
