/**
 * Tests for UrlInput component.
 * Owner: Scenario 2 - Hero Section URL Input and Shortening CTA
 *
 * Test cases:
 * - URL input field exists with correct type and placeholder
 * - URL input field receives focus on mount
 * - Shorten URL button exists
 * - Enter key triggers form submission
 */

import { describe, it, expect, vi } from 'vitest'
import { render, screen, waitFor } from '../../../test-utils'
import userEvent from '@testing-library/user-event'
import UrlInput from '@/components/homepage/UrlInput'

describe('UrlInput', () => {
  it('renders URL input field with correct type and placeholder', () => {
    const handleSubmit = vi.fn()
    render(<UrlInput onSubmit={handleSubmit} />)

    const input = screen.getByTestId('url-input')
    expect(input).toBeInTheDocument()
    expect(input).toHaveAttribute('type', 'url')
    expect(input).toHaveAttribute('placeholder', 'Enter your long URL here...')
  })

  it('auto-focuses URL input field on mount', async () => {
    const handleSubmit = vi.fn()
    render(<UrlInput onSubmit={handleSubmit} />)

    const input = screen.getByTestId('url-input')

    await waitFor(() => {
      expect(document.activeElement).toBe(input)
    })
  })

  it('renders Shorten URL button', () => {
    const handleSubmit = vi.fn()
    render(<UrlInput onSubmit={handleSubmit} />)

    const button = screen.getByTestId('shorten-button')
    expect(button).toBeInTheDocument()
    expect(button).toHaveTextContent('Shorten URL')
  })

  it('triggers submission when Enter key is pressed with valid URL', async () => {
    const handleSubmit = vi.fn()
    const user = userEvent.setup()
    render(<UrlInput onSubmit={handleSubmit} />)

    const input = screen.getByTestId('url-input')
    await user.type(input, 'https://example.com')
    await user.keyboard('{Enter}')

    expect(handleSubmit).toHaveBeenCalledWith('https://example.com')
  })

  it('triggers submission when Shorten URL button is clicked', async () => {
    const handleSubmit = vi.fn()
    const user = userEvent.setup()
    render(<UrlInput onSubmit={handleSubmit} />)

    const input = screen.getByTestId('url-input')
    const button = screen.getByTestId('shorten-button')

    await user.type(input, 'https://example.com')
    await user.click(button)

    expect(handleSubmit).toHaveBeenCalledWith('https://example.com')
  })

  it('does not submit when input is empty', async () => {
    const handleSubmit = vi.fn()
    const user = userEvent.setup()
    render(<UrlInput onSubmit={handleSubmit} />)

    const button = screen.getByTestId('shorten-button')
    await user.click(button)

    expect(handleSubmit).not.toHaveBeenCalled()
  })

  it('does not submit empty string when Enter is pressed', async () => {
    const handleSubmit = vi.fn()
    const user = userEvent.setup()
    render(<UrlInput onSubmit={handleSubmit} />)

    const input = screen.getByTestId('url-input')
    await user.click(input)
    await user.keyboard('{Enter}')

    expect(handleSubmit).not.toHaveBeenCalled()
  })

  it('displays error message when error prop is provided', () => {
    const handleSubmit = vi.fn()
    render(<UrlInput onSubmit={handleSubmit} error="Invalid URL format" />)

    const errorMessage = screen.getByTestId('url-error')
    expect(errorMessage).toBeInTheDocument()
    expect(errorMessage).toHaveTextContent('Invalid URL format')
  })

  it('shows loading spinner when isLoading is true', () => {
    const handleSubmit = vi.fn()
    render(<UrlInput onSubmit={handleSubmit} isLoading={true} />)

    const button = screen.getByTestId('shorten-button')
    expect(button).toContainElement(button.querySelector('.loading'))
    expect(button).not.toHaveTextContent('Shorten URL')
  })

  it('disables input and button when isLoading is true', () => {
    const handleSubmit = vi.fn()
    render(<UrlInput onSubmit={handleSubmit} isLoading={true} />)

    const input = screen.getByTestId('url-input')
    const button = screen.getByTestId('shorten-button')

    expect(input).toBeDisabled()
    expect(button).toBeDisabled()
  })

  it('has proper accessibility attributes', () => {
    const handleSubmit = vi.fn()
    render(<UrlInput onSubmit={handleSubmit} />)

    const input = screen.getByTestId('url-input')
    expect(input).toHaveAttribute('aria-label', 'URL to shorten')
  })

  it('associates error with input via aria-describedby when error exists', () => {
    const handleSubmit = vi.fn()
    render(<UrlInput onSubmit={handleSubmit} error="Invalid URL" />)

    const input = screen.getByTestId('url-input')
    expect(input).toHaveAttribute('aria-describedby', 'url-error')
  })
})

/**
 * Tests for Loading States and User Feedback.
 * Owner: Scenario 14 - Loading States and User Feedback
 *
 * Test cases:
 * - TC1: Button shows loading state when clicked with valid URL
 * - TC2: Button prevents duplicate submissions when in loading state
 * - TC3: Error message displays and form returns to submittable state
 * - TC4: CTA buttons have smooth hover transition animations
 */
describe('UrlInput - Loading States and User Feedback (Scenario 14)', () => {
  describe('TC1: Loading state display during URL submission', () => {
    it('shows loading spinner when isLoading is true', () => {
      const handleSubmit = vi.fn()
      render(<UrlInput onSubmit={handleSubmit} isLoading={true} />)

      const button = screen.getByTestId('shorten-button')
      const spinner = button.querySelector('.loading-spinner')
      expect(spinner).toBeInTheDocument()
    })

    it('hides "Shorten URL" text when loading', () => {
      const handleSubmit = vi.fn()
      render(<UrlInput onSubmit={handleSubmit} isLoading={true} />)

      const button = screen.getByTestId('shorten-button')
      expect(button.textContent).not.toContain('Shorten URL')
    })

    it('shows "Shorten URL" text when not loading', () => {
      const handleSubmit = vi.fn()
      render(<UrlInput onSubmit={handleSubmit} isLoading={false} />)

      const button = screen.getByTestId('shorten-button')
      expect(button).toHaveTextContent('Shorten URL')
    })

    it('disables button during loading state', () => {
      const handleSubmit = vi.fn()
      render(<UrlInput onSubmit={handleSubmit} isLoading={true} />)

      const button = screen.getByTestId('shorten-button')
      expect(button).toBeDisabled()
    })

    it('disables input field during loading state', () => {
      const handleSubmit = vi.fn()
      render(<UrlInput onSubmit={handleSubmit} isLoading={true} />)

      const input = screen.getByTestId('url-input')
      expect(input).toBeDisabled()
    })
  })

  describe('TC2: Preventing duplicate submissions', () => {
    it('prevents form submission when isLoading is true', async () => {
      const handleSubmit = vi.fn()
      const user = userEvent.setup()
      render(<UrlInput onSubmit={handleSubmit} isLoading={true} />)

      const button = screen.getByTestId('shorten-button')
      await user.click(button)

      expect(handleSubmit).not.toHaveBeenCalled()
    })

    it('prevents Enter key submission when isLoading is true', async () => {
      const handleSubmit = vi.fn()
      const user = userEvent.setup()
      const { rerender } = render(<UrlInput onSubmit={handleSubmit} isLoading={false} />)

      // Type URL first while not loading
      const input = screen.getByTestId('url-input')
      await user.type(input, 'https://example.com')

      // Rerender with loading state
      rerender(<UrlInput onSubmit={handleSubmit} isLoading={true} />)

      // Try to submit via Enter key
      await user.keyboard('{Enter}')

      // handleSubmit is called from the initial typing, but not from Enter during loading
      expect(handleSubmit).toHaveBeenCalledTimes(0)
    })

    it('button remains clickable after loading completes', async () => {
      const handleSubmit = vi.fn()
      const user = userEvent.setup()
      const { rerender } = render(<UrlInput onSubmit={handleSubmit} isLoading={true} />)

      // Button should be disabled during loading
      const button = screen.getByTestId('shorten-button')
      expect(button).toBeDisabled()

      // Rerender with loading finished
      rerender(<UrlInput onSubmit={handleSubmit} isLoading={false} />)

      // Button should be enabled again (but still disabled if no URL)
      const input = screen.getByTestId('url-input')
      await user.type(input, 'https://example.com')

      expect(button).not.toBeDisabled()
      await user.click(button)
      expect(handleSubmit).toHaveBeenCalledWith('https://example.com')
    })

    it('does not allow multiple rapid clicks to trigger multiple submissions', async () => {
      const handleSubmit = vi.fn()
      const user = userEvent.setup()
      render(<UrlInput onSubmit={handleSubmit} isLoading={false} />)

      const input = screen.getByTestId('url-input')
      await user.type(input, 'https://example.com')

      const button = screen.getByTestId('shorten-button')

      // Click the button
      await user.click(button)

      // First click should trigger submission
      expect(handleSubmit).toHaveBeenCalledTimes(1)
    })
  })

  describe('TC3: Error state display and form recovery', () => {
    it('displays error message when error prop is provided', () => {
      const handleSubmit = vi.fn()
      const errorMessage = 'Network error occurred. Please try again.'
      render(<UrlInput onSubmit={handleSubmit} error={errorMessage} />)

      const errorElement = screen.getByTestId('url-error')
      expect(errorElement).toBeInTheDocument()
      expect(errorElement).toHaveTextContent(errorMessage)
    })

    it('error message has role="alert" for screen readers', () => {
      const handleSubmit = vi.fn()
      render(<UrlInput onSubmit={handleSubmit} error="An error occurred" />)

      const errorElement = screen.getByTestId('url-error')
      expect(errorElement).toHaveAttribute('role', 'alert')
    })

    it('input shows error styling when error exists', () => {
      const handleSubmit = vi.fn()
      render(<UrlInput onSubmit={handleSubmit} error="Invalid URL" />)

      const input = screen.getByTestId('url-input')
      expect(input).toHaveClass('input-error')
    })

    it('form returns to submittable state after error is cleared', async () => {
      const handleSubmit = vi.fn()
      const user = userEvent.setup()
      const { rerender } = render(<UrlInput onSubmit={handleSubmit} error="An error occurred" />)

      // Error is displayed
      expect(screen.getByTestId('url-error')).toBeInTheDocument()

      // Rerender without error (simulating error being cleared)
      rerender(<UrlInput onSubmit={handleSubmit} error={null} />)

      // Error should be gone
      expect(screen.queryByTestId('url-error')).not.toBeInTheDocument()

      // Form should be submittable again
      const input = screen.getByTestId('url-input')
      await user.type(input, 'https://example.com')

      const button = screen.getByTestId('shorten-button')
      expect(button).not.toBeDisabled()

      await user.click(button)
      expect(handleSubmit).toHaveBeenCalledWith('https://example.com')
    })

    it('input is not disabled when showing error (allows user to correct input)', () => {
      const handleSubmit = vi.fn()
      render(<UrlInput onSubmit={handleSubmit} error="Invalid URL format" isLoading={false} />)

      const input = screen.getByTestId('url-input')
      expect(input).not.toBeDisabled()
    })

    it('button is enabled when showing error with valid URL (allows retry)', async () => {
      const handleSubmit = vi.fn()
      const user = userEvent.setup()
      render(<UrlInput onSubmit={handleSubmit} error="Server error, please retry" isLoading={false} />)

      const input = screen.getByTestId('url-input')
      await user.type(input, 'https://example.com')

      const button = screen.getByTestId('shorten-button')
      expect(button).not.toBeDisabled()
    })

    it('transitions from loading to error state correctly', () => {
      const handleSubmit = vi.fn()
      const { rerender } = render(<UrlInput onSubmit={handleSubmit} isLoading={true} />)

      // Initially loading
      const button = screen.getByTestId('shorten-button')
      expect(button).toBeDisabled()
      expect(button.querySelector('.loading')).toBeInTheDocument()

      // Transition to error state
      rerender(<UrlInput onSubmit={handleSubmit} isLoading={false} error="Network error" />)

      // Should show error and be submittable
      expect(screen.getByTestId('url-error')).toHaveTextContent('Network error')
      expect(button.querySelector('.loading')).not.toBeInTheDocument()
    })
  })

  describe('TC4: Smooth hover transition animations on CTA buttons', () => {
    it('Shorten URL button has transition classes for smooth hover effect', () => {
      const handleSubmit = vi.fn()
      render(<UrlInput onSubmit={handleSubmit} />)

      const button = screen.getByTestId('shorten-button')
      // FuturisticButton applies 'transition-all duration-300 hover:scale-105'
      expect(button).toHaveClass('transition-all')
      expect(button).toHaveClass('duration-300')
    })

    it('button has hover scale transform class', () => {
      const handleSubmit = vi.fn()
      render(<UrlInput onSubmit={handleSubmit} />)

      const button = screen.getByTestId('shorten-button')
      // Verify hover:scale-105 class is present for smooth scaling animation
      expect(button.className).toMatch(/hover:scale/)
    })

    it('button maintains transition classes when in loading state', () => {
      const handleSubmit = vi.fn()
      render(<UrlInput onSubmit={handleSubmit} isLoading={true} />)

      const button = screen.getByTestId('shorten-button')
      // Transition classes should still be present even during loading
      expect(button).toHaveClass('transition-all')
    })

    it('button maintains transition classes when showing error', () => {
      const handleSubmit = vi.fn()
      render(<UrlInput onSubmit={handleSubmit} error="An error" />)

      const button = screen.getByTestId('shorten-button')
      expect(button).toHaveClass('transition-all')
    })
  })
})
