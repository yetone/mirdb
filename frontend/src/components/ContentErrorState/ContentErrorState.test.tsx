import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { ContentErrorState } from './ContentErrorState'

/**
 * Integration tests for ContentErrorState component
 * Test Case 3: Test content API failure - Page displays fallback content or error state gracefully
 */
describe('ContentErrorState Component', () => {
  describe('Rendering', () => {
    it('renders with default error message', () => {
      render(<ContentErrorState />)

      const errorState = screen.getByTestId('content-error-state')
      expect(errorState).toBeInTheDocument()

      const message = screen.getByTestId('error-message')
      expect(message).toHaveTextContent(/unable to load content/i)
    })

    it('renders with custom error message', () => {
      const customMessage = 'Custom error message for testing'
      render(<ContentErrorState message={customMessage} />)

      const message = screen.getByTestId('error-message')
      expect(message).toHaveTextContent(customMessage)
    })

    it('renders retry button when onRetry is provided', () => {
      const onRetry = vi.fn()
      render(<ContentErrorState onRetry={onRetry} />)

      const retryButton = screen.getByTestId('retry-button')
      expect(retryButton).toBeInTheDocument()
      expect(retryButton).toHaveTextContent(/try again/i)
    })

    it('does not render retry button when showRetry is false', () => {
      const onRetry = vi.fn()
      render(<ContentErrorState onRetry={onRetry} showRetry={false} />)

      const retryButton = screen.queryByTestId('retry-button')
      expect(retryButton).not.toBeInTheDocument()
    })

    it('does not render retry button when onRetry is not provided', () => {
      render(<ContentErrorState />)

      const retryButton = screen.queryByTestId('retry-button')
      expect(retryButton).not.toBeInTheDocument()
    })
  })

  describe('Interactions', () => {
    it('calls onRetry when retry button is clicked', () => {
      const onRetry = vi.fn()
      render(<ContentErrorState onRetry={onRetry} />)

      const retryButton = screen.getByTestId('retry-button')
      fireEvent.click(retryButton)

      expect(onRetry).toHaveBeenCalledTimes(1)
    })

    it('calls onRetry multiple times when clicked multiple times', () => {
      const onRetry = vi.fn()
      render(<ContentErrorState onRetry={onRetry} />)

      const retryButton = screen.getByTestId('retry-button')
      fireEvent.click(retryButton)
      fireEvent.click(retryButton)
      fireEvent.click(retryButton)

      expect(onRetry).toHaveBeenCalledTimes(3)
    })
  })

  describe('Accessibility', () => {
    it('has proper role for accessibility', () => {
      render(<ContentErrorState />)

      const errorState = screen.getByTestId('content-error-state')
      expect(errorState).toHaveAttribute('role', 'alert')
    })

    it('has aria-live for screen readers', () => {
      render(<ContentErrorState />)

      const errorState = screen.getByTestId('content-error-state')
      expect(errorState).toHaveAttribute('aria-live', 'polite')
    })

    it('retry button is keyboard accessible', () => {
      const onRetry = vi.fn()
      render(<ContentErrorState onRetry={onRetry} />)

      const retryButton = screen.getByTestId('retry-button')
      expect(retryButton).toHaveAttribute('type', 'button')

      // Simulate keyboard activation
      retryButton.focus()
      fireEvent.keyDown(retryButton, { key: 'Enter', code: 'Enter' })
      fireEvent.click(retryButton)

      expect(onRetry).toHaveBeenCalled()
    })
  })

  describe('Error scenarios', () => {
    it('displays network error message appropriately', () => {
      render(
        <ContentErrorState message="Network connection lost. Please check your internet." />
      )

      expect(
        screen.getByText(/network connection lost/i)
      ).toBeInTheDocument()
    })

    it('displays server error message appropriately', () => {
      render(
        <ContentErrorState message="Server error. Our team has been notified." />
      )

      expect(screen.getByText(/server error/i)).toBeInTheDocument()
    })

    it('displays timeout error message appropriately', () => {
      render(
        <ContentErrorState message="Request timed out. Please try again." />
      )

      expect(screen.getByText(/request timed out/i)).toBeInTheDocument()
    })
  })
})
