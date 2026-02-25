/**
 * Unit Tests for UrlShortenForm Component
 * Owner: Scenario 2 - URL Shortening Form Functionality
 * Also: Scenario 14 - Form Validation Error Handling
 *
 * Tests:
 * - URL validation (empty, invalid format)
 * - Form submission
 * - Error display
 * - Copy to clipboard
 * - Form validation error handling (Scenario 14)
 */
import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent, waitFor } from '../../../test-utils'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw'
import { server } from '../../../mocks/server'
import { UrlShortenForm } from '../../../../src/components/homepage/UrlShortenForm'

describe('UrlShortenForm', () => {
  describe('Validation', () => {
    it('displays validation error for empty URL', async () => {
      render(<UrlShortenForm />)

      const submitButton = screen.getByRole('button', { name: /shorten url/i })
      await userEvent.click(submitButton)

      expect(screen.getByRole('alert')).toHaveTextContent('URL is required')
    })

    it('displays validation error for invalid URL format', async () => {
      render(<UrlShortenForm />)

      const input = screen.getByRole('textbox', { name: /url input/i })
      await userEvent.type(input, 'not-a-url')

      const submitButton = screen.getByRole('button', { name: /shorten url/i })
      await userEvent.click(submitButton)

      expect(screen.getByRole('alert')).toHaveTextContent('Please enter a valid URL')
    })

    it('clears error when user starts typing after error', async () => {
      render(<UrlShortenForm />)

      const submitButton = screen.getByRole('button', { name: /shorten url/i })
      await userEvent.click(submitButton)

      expect(screen.getByRole('alert')).toBeInTheDocument()

      const input = screen.getByRole('textbox', { name: /url input/i })
      await userEvent.type(input, 'h')

      expect(screen.queryByRole('alert')).not.toBeInTheDocument()
    })

    it('accepts valid http URL', async () => {
      render(<UrlShortenForm />)

      const input = screen.getByRole('textbox', { name: /url input/i })
      await userEvent.type(input, 'http://example.com/path')

      const submitButton = screen.getByRole('button', { name: /shorten url/i })
      await userEvent.click(submitButton)

      await waitFor(() => {
        expect(screen.queryByText('Please enter a valid URL')).not.toBeInTheDocument()
      })
    })

    it('accepts valid https URL', async () => {
      render(<UrlShortenForm />)

      const input = screen.getByRole('textbox', { name: /url input/i })
      await userEvent.type(input, 'https://example.com/very/long/path')

      const submitButton = screen.getByRole('button', { name: /shorten url/i })
      await userEvent.click(submitButton)

      await waitFor(() => {
        expect(screen.queryByText('Please enter a valid URL')).not.toBeInTheDocument()
      })
    })
  })

  describe('Form Submission', () => {
    it('submits form when Enter key is pressed with valid URL', async () => {
      render(<UrlShortenForm />)

      const input = screen.getByRole('textbox', { name: /url input/i })
      await userEvent.type(input, 'https://example.com/very/long/path{enter}')

      await waitFor(() => {
        expect(screen.getByLabelText(/shortened url result/i)).toBeInTheDocument()
      })
    })

    it('shows loading state during submission', async () => {
      render(<UrlShortenForm />)

      const input = screen.getByRole('textbox', { name: /url input/i })
      await userEvent.type(input, 'https://example.com/path')

      const submitButton = screen.getByRole('button', { name: /shorten url/i })
      fireEvent.click(submitButton)

      await waitFor(() => {
        expect(screen.getByLabelText(/loading/i)).toBeInTheDocument()
      })
    })

    it('disables input and button during submission', async () => {
      render(<UrlShortenForm />)

      const input = screen.getByRole('textbox', { name: /url input/i })
      await userEvent.type(input, 'https://example.com/path')

      const submitButton = screen.getByRole('button', { name: /shorten url/i })
      fireEvent.click(submitButton)

      await waitFor(() => {
        expect(input).toBeDisabled()
        expect(submitButton).toBeDisabled()
      })
    })
  })

  describe('Result Display', () => {
    it('displays shortened URL after successful submission', async () => {
      render(<UrlShortenForm />)

      const input = screen.getByRole('textbox', { name: /url input/i })
      await userEvent.type(input, 'https://example.com/very/long/path/to/resource')

      const submitButton = screen.getByRole('button', { name: /shorten url/i })
      await userEvent.click(submitButton)

      await waitFor(() => {
        expect(screen.getByLabelText(/shortened url result/i)).toBeInTheDocument()
        expect(screen.getByDisplayValue(/\/r\/abc123/)).toBeInTheDocument()
      })
    })

    it('calls onSuccess callback with shortened URL', async () => {
      const onSuccess = vi.fn()
      render(<UrlShortenForm onSuccess={onSuccess} />)

      const input = screen.getByRole('textbox', { name: /url input/i })
      await userEvent.type(input, 'https://example.com/path')

      const submitButton = screen.getByRole('button', { name: /shorten url/i })
      await userEvent.click(submitButton)

      await waitFor(() => {
        expect(onSuccess).toHaveBeenCalledWith(
          expect.objectContaining({
            shortCode: 'abc123',
            originalUrl: 'https://example.com/path',
          })
        )
      })
    })
  })

  describe('Copy to Clipboard', () => {
    it('copies shortened URL to clipboard when copy button is clicked', async () => {
      render(<UrlShortenForm />)

      const input = screen.getByRole('textbox', { name: /url input/i })
      await userEvent.type(input, 'https://example.com/path')

      const submitButton = screen.getByRole('button', { name: /shorten url/i })
      await userEvent.click(submitButton)

      await waitFor(() => {
        expect(screen.getByLabelText(/shortened url result/i)).toBeInTheDocument()
      })

      const copyButton = screen.getByRole('button', { name: /copy to clipboard/i })
      await userEvent.click(copyButton)

      expect(navigator.clipboard.writeText).toHaveBeenCalled()
    })

    it('shows success feedback after copying', async () => {
      render(<UrlShortenForm />)

      const input = screen.getByRole('textbox', { name: /url input/i })
      await userEvent.type(input, 'https://example.com/path')

      const submitButton = screen.getByRole('button', { name: /shorten url/i })
      await userEvent.click(submitButton)

      await waitFor(() => {
        expect(screen.getByLabelText(/shortened url result/i)).toBeInTheDocument()
      })

      const copyButton = screen.getByRole('button', { name: /copy to clipboard/i })
      await userEvent.click(copyButton)

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /copied/i })).toBeInTheDocument()
      })
    })
  })

  describe('Reset Functionality', () => {
    it('allows shortening another URL after reset', async () => {
      render(<UrlShortenForm />)

      const input = screen.getByRole('textbox', { name: /url input/i })
      await userEvent.type(input, 'https://example.com/path')

      const submitButton = screen.getByRole('button', { name: /shorten url/i })
      await userEvent.click(submitButton)

      await waitFor(() => {
        expect(screen.getByLabelText(/shortened url result/i)).toBeInTheDocument()
      })

      const resetButton = screen.getByRole('button', { name: /shorten another url/i })
      await userEvent.click(resetButton)

      expect(screen.queryByLabelText(/shortened url result/i)).not.toBeInTheDocument()
      expect(screen.getByRole('textbox', { name: /url input/i })).toHaveValue('')
    })
  })

  describe('Accessibility', () => {
    it('has proper ARIA labels', () => {
      render(<UrlShortenForm />)

      expect(screen.getByRole('form', { name: /url shortening form/i })).toBeInTheDocument()
      expect(screen.getByRole('textbox', { name: /url input/i })).toBeInTheDocument()
      expect(screen.getByRole('button', { name: /shorten url/i })).toBeInTheDocument()
    })

    it('sets aria-invalid on input when error exists', async () => {
      render(<UrlShortenForm />)

      const input = screen.getByRole('textbox', { name: /url input/i })
      const submitButton = screen.getByRole('button', { name: /shorten url/i })

      await userEvent.click(submitButton)

      expect(input).toHaveAttribute('aria-invalid', 'true')
    })
  })

  /**
   * Form Validation Error Handling Tests
   * Owner: Scenario 14 - Form Validation Error Handling
   *
   * Test cases:
   * 1. Submit form with empty URL field -> "URL is required" displayed inline
   * 2. Submit form with invalid URL format -> "Please enter a valid URL" displayed
   * 3. Enter invalid URL then correct it -> Error clears when valid URL entered
   * 4. API returns error during URL shortening -> Error from API displayed gracefully
   */
  describe('Form Validation Error Handling (Scenario 14)', () => {
    it('displays "URL is required" error when submitting empty form', async () => {
      render(<UrlShortenForm />)

      // Submit form without entering a URL
      const submitButton = screen.getByRole('button', { name: /shorten url/i })
      await userEvent.click(submitButton)

      // Error message should be displayed inline
      const errorMessage = screen.getByRole('alert')
      expect(errorMessage).toHaveTextContent('URL is required')
      expect(errorMessage).toBeVisible()
    })

    it('displays "Please enter a valid URL" for invalid URL format', async () => {
      render(<UrlShortenForm />)

      // Enter an invalid URL
      const input = screen.getByRole('textbox', { name: /url input/i })
      await userEvent.type(input, 'invalid-url-format')

      // Submit the form
      const submitButton = screen.getByRole('button', { name: /shorten url/i })
      await userEvent.click(submitButton)

      // Error message should indicate invalid format
      const errorMessage = screen.getByRole('alert')
      expect(errorMessage).toHaveTextContent('Please enter a valid URL')
      expect(errorMessage).toBeVisible()
    })

    it('clears error message when valid URL is entered after invalid input', async () => {
      render(<UrlShortenForm />)

      // First, enter an invalid URL and submit
      const input = screen.getByRole('textbox', { name: /url input/i })
      await userEvent.type(input, 'invalid')

      const submitButton = screen.getByRole('button', { name: /shorten url/i })
      await userEvent.click(submitButton)

      // Verify error is shown
      expect(screen.getByRole('alert')).toHaveTextContent('Please enter a valid URL')

      // Clear input and enter a valid URL
      await userEvent.clear(input)
      await userEvent.type(input, 'https://example.com/valid-url')

      // Error should be cleared
      expect(screen.queryByRole('alert')).not.toBeInTheDocument()
    })

    it('displays API error message gracefully when URL shortening fails', async () => {
      // Mock API to return an error
      server.use(
        http.post('/api/urls/', () => {
          return HttpResponse.json(
            { detail: 'Service temporarily unavailable' },
            { status: 503 }
          )
        })
      )

      render(<UrlShortenForm />)

      // Enter a valid URL
      const input = screen.getByRole('textbox', { name: /url input/i })
      await userEvent.type(input, 'https://example.com/valid-url')

      // Submit the form
      const submitButton = screen.getByRole('button', { name: /shorten url/i })
      await userEvent.click(submitButton)

      // Error message should be displayed gracefully (user-friendly message)
      await waitFor(() => {
        const errorMessage = screen.getByRole('alert')
        expect(errorMessage).toHaveTextContent('Something went wrong. Please try again.')
        expect(errorMessage).toBeVisible()
      })
    })

    it('shows error near the input field for accessibility', async () => {
      render(<UrlShortenForm />)

      // Submit empty form
      const submitButton = screen.getByRole('button', { name: /shorten url/i })
      await userEvent.click(submitButton)

      // Error should be associated with the input via aria-describedby
      const input = screen.getByRole('textbox', { name: /url input/i })
      const errorElement = screen.getByRole('alert')

      expect(input).toHaveAttribute('aria-invalid', 'true')
      expect(input).toHaveAttribute('aria-describedby', 'url-error')
      expect(errorElement).toHaveAttribute('id', 'url-error')
    })

    it('applies error styling to input when validation fails', async () => {
      render(<UrlShortenForm />)

      // Submit empty form
      const submitButton = screen.getByRole('button', { name: /shorten url/i })
      await userEvent.click(submitButton)

      // Input should have error class
      const input = screen.getByRole('textbox', { name: /url input/i })
      expect(input).toHaveClass('input-error')
    })

    it('removes error styling when user starts correcting input', async () => {
      render(<UrlShortenForm />)

      // Submit empty form to trigger error
      const submitButton = screen.getByRole('button', { name: /shorten url/i })
      await userEvent.click(submitButton)

      const input = screen.getByRole('textbox', { name: /url input/i })
      expect(input).toHaveClass('input-error')

      // Start typing to correct the input
      await userEvent.type(input, 'h')

      // Error styling should be removed
      expect(input).not.toHaveClass('input-error')
    })
  })
})
