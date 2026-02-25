/**
 * Unit Tests for UrlShortenForm Component
 * Owner: Scenario 2 - URL Shortening Form Functionality
 *
 * Tests:
 * - URL validation (empty, invalid format)
 * - Form submission
 * - Error display
 * - Copy to clipboard
 */
import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent, waitFor } from '../../../test-utils'
import userEvent from '@testing-library/user-event'
import { UrlShortenForm } from '../../../../src/components/homepage/UrlShortenForm'

describe('UrlShortenForm', () => {
  describe('Validation', () => {
    it('displays validation error for empty URL', async () => {
      render(<UrlShortenForm />)

      const submitButton = screen.getByRole('button', { name: /shorten url/i })
      await userEvent.click(submitButton)

      expect(screen.getByRole('alert')).toHaveTextContent('Please enter a URL')
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
})
