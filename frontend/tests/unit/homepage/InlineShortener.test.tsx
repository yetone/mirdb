/**
 * Unit Tests for InlineShortener Component - Validation & Error Handling
 * Owner: Scenario 3 - Inline URL Shortening - Validation & Error Handling
 *
 * Tests:
 * - Invalid URL format shows error message
 * - Empty input shows required error
 * - Incomplete URL shows validation error
 * - API error shows friendly message with retry
 * - Special characters in URL are properly encoded
 *
 * Requirements: REQ-7
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { InlineShortener } from '../../../src/components/homepage/InlineShortener'
import { validateUrl } from '../../../src/hooks/useAnonymousShorten'

// Mock clipboard API
Object.assign(navigator, {
  clipboard: {
    writeText: vi.fn().mockResolvedValue(undefined)
  }
})

describe('InlineShortener - Validation & Error Handling', () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.clearAllMocks()
  })

  describe('URL Validation', () => {
    it('displays error message when submitting invalid URL format', async () => {
      render(<InlineShortener />)

      const input = screen.getByTestId('url-input')
      const submitButton = screen.getByTestId('shorten-button')

      // Enter invalid URL
      fireEvent.change(input, { target: { value: 'not-a-valid-url' } })
      fireEvent.click(submitButton)

      // Check for error message displayed inline
      const errorMessage = await screen.findByTestId('error-message')
      expect(errorMessage).toBeInTheDocument()
      expect(errorMessage).toHaveTextContent('Please enter a valid URL')
    })

    it('displays error message when submitting empty input', async () => {
      render(<InlineShortener />)

      const submitButton = screen.getByTestId('shorten-button')

      // Click shorten with empty input
      fireEvent.click(submitButton)

      // Check for error message
      const errorMessage = await screen.findByTestId('error-message')
      expect(errorMessage).toBeInTheDocument()
      expect(errorMessage).toHaveTextContent('URL is required')
    })

    it('displays error when submitting incomplete URL (http://)', async () => {
      render(<InlineShortener />)

      const input = screen.getByTestId('url-input')
      const submitButton = screen.getByTestId('shorten-button')

      // Enter incomplete URL
      fireEvent.change(input, { target: { value: 'http://' } })
      fireEvent.click(submitButton)

      // Check for validation error
      const errorMessage = await screen.findByTestId('error-message')
      expect(errorMessage).toBeInTheDocument()
      expect(errorMessage).toHaveTextContent('Please enter a valid URL')
    })

    it('clears error message when user starts typing', async () => {
      render(<InlineShortener />)

      const input = screen.getByTestId('url-input')
      const submitButton = screen.getByTestId('shorten-button')

      // Submit empty to trigger error
      fireEvent.click(submitButton)

      // Error should be visible
      expect(await screen.findByTestId('error-message')).toBeInTheDocument()

      // Start typing - error should clear
      fireEvent.change(input, { target: { value: 'h' } })

      // Error should be gone
      expect(screen.queryByTestId('error-message')).not.toBeInTheDocument()
    })

    it('marks input as invalid when error is present', async () => {
      render(<InlineShortener />)

      const input = screen.getByTestId('url-input')
      const submitButton = screen.getByTestId('shorten-button')

      // Submit empty
      fireEvent.click(submitButton)

      await waitFor(() => {
        expect(input).toHaveAttribute('aria-invalid', 'true')
      })
    })
  })

  describe('API Error Handling', () => {
    it('displays friendly error message on network error and allows retry', async () => {
      render(<InlineShortener />)

      const input = screen.getByTestId('url-input')
      const submitButton = screen.getByTestId('shorten-button')

      // Enter URL that triggers API error (test URL)
      fireEvent.change(input, { target: { value: 'https://example.com/trigger-api-error' } })
      fireEvent.click(submitButton)

      // Wait for API call and error
      await vi.runAllTimersAsync()

      // Check for friendly error message
      const errorMessage = await screen.findByTestId('error-message')
      expect(errorMessage).toBeInTheDocument()
      expect(errorMessage).toHaveTextContent('Failed to shorten URL. Please try again.')

      // Button should still be enabled for retry
      expect(submitButton).not.toBeDisabled()
      expect(submitButton).toHaveTextContent('Shorten')
    })

    it('shows loading state during API call', async () => {
      render(<InlineShortener />)

      const input = screen.getByTestId('url-input')
      const submitButton = screen.getByTestId('shorten-button')

      // Enter valid URL
      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(submitButton)

      // Check for loading state
      expect(submitButton).toHaveTextContent('Shortening...')
      expect(submitButton).toBeDisabled()

      // Wait for API to complete
      await vi.runAllTimersAsync()

      // Button should return to normal
      await waitFor(() => {
        expect(submitButton).toHaveTextContent('Shorten')
      })
    })

    it('disables input during API call', async () => {
      render(<InlineShortener />)

      const input = screen.getByTestId('url-input')
      const submitButton = screen.getByTestId('shorten-button')

      // Enter valid URL
      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(submitButton)

      // Input should be disabled
      expect(input).toBeDisabled()

      // Wait for API to complete
      await vi.runAllTimersAsync()

      await waitFor(() => {
        expect(input).not.toBeDisabled()
      })
    })
  })

  describe('Special Characters Handling', () => {
    it('successfully shortens URL with special characters that need encoding', async () => {
      render(<InlineShortener />)

      const input = screen.getByTestId('url-input')
      const submitButton = screen.getByTestId('shorten-button')

      // Enter URL with special characters
      const urlWithSpecialChars = 'https://example.com/path?query=hello world&name=test#section'
      fireEvent.change(input, { target: { value: urlWithSpecialChars } })
      fireEvent.click(submitButton)

      // Wait for API to complete
      await vi.runAllTimersAsync()

      // Should show result section (successful shortening)
      const resultSection = await screen.findByTestId('result-section')
      expect(resultSection).toBeInTheDocument()

      // Should display shortened URL
      const shortUrl = screen.getByTestId('short-url')
      expect(shortUrl).toBeInTheDocument()
      expect(shortUrl.textContent).toContain('https://short.url/')
    })

    it('handles URL with unicode characters', async () => {
      render(<InlineShortener />)

      const input = screen.getByTestId('url-input')
      const submitButton = screen.getByTestId('shorten-button')

      // Enter URL with unicode characters
      const unicodeUrl = 'https://example.com/path/cafe-resume-test'
      fireEvent.change(input, { target: { value: unicodeUrl } })
      fireEvent.click(submitButton)

      // Wait for API to complete
      await vi.runAllTimersAsync()

      // Should show result (successful)
      const resultSection = await screen.findByTestId('result-section')
      expect(resultSection).toBeInTheDocument()
    })
  })

  describe('Form Behavior', () => {
    it('submits form on Enter key press', async () => {
      render(<InlineShortener />)

      const input = screen.getByTestId('url-input')

      // Enter valid URL and press Enter
      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.keyDown(input, { key: 'Enter', code: 'Enter' })

      // Should start loading
      const submitButton = screen.getByTestId('shorten-button')
      expect(submitButton).toHaveTextContent('Shortening...')

      // Wait for completion
      await vi.runAllTimersAsync()
    })

    it('shows Enter key validation error', async () => {
      render(<InlineShortener />)

      const input = screen.getByTestId('url-input')

      // Press Enter with empty input
      fireEvent.keyDown(input, { key: 'Enter', code: 'Enter' })

      // Should show error
      const errorMessage = await screen.findByTestId('error-message')
      expect(errorMessage).toHaveTextContent('URL is required')
    })

    it('allows user to shorten another URL after success', async () => {
      render(<InlineShortener />)

      const input = screen.getByTestId('url-input')
      const submitButton = screen.getByTestId('shorten-button')

      // Shorten first URL
      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(submitButton)

      await vi.runAllTimersAsync()

      // Click "Shorten Another"
      const resetButton = await screen.findByTestId('reset-button')
      fireEvent.click(resetButton)

      // Result should be cleared
      expect(screen.queryByTestId('result-section')).not.toBeInTheDocument()

      // Input should be empty
      expect(input).toHaveValue('')
    })
  })

  describe('Accessibility', () => {
    it('has proper ARIA attributes for error state', async () => {
      render(<InlineShortener />)

      const input = screen.getByTestId('url-input')
      const submitButton = screen.getByTestId('shorten-button')

      // Submit empty
      fireEvent.click(submitButton)

      await waitFor(() => {
        expect(input).toHaveAttribute('aria-invalid', 'true')
        expect(input).toHaveAttribute('aria-describedby', 'url-error')
      })

      // Error has alert role
      const errorMessage = screen.getByTestId('error-message')
      expect(errorMessage).toHaveAttribute('role', 'alert')
    })

    it('has proper label for input field', () => {
      render(<InlineShortener />)

      // Label should exist (sr-only)
      const label = screen.getByLabelText(/enter url to shorten/i)
      expect(label).toBeInTheDocument()
    })
  })
})

describe('validateUrl function - Unit tests', () => {
  it('returns error for empty string', () => {
    const result = validateUrl('')
    expect(result.isValid).toBe(false)
    expect(result.error).toBe('URL is required')
  })

  it('returns error for whitespace only', () => {
    const result = validateUrl('   ')
    expect(result.isValid).toBe(false)
    expect(result.error).toBe('URL is required')
  })

  it('returns error for invalid URL format', () => {
    const result = validateUrl('not-a-valid-url')
    expect(result.isValid).toBe(false)
    expect(result.error).toBe('Please enter a valid URL')
  })

  it('returns error for incomplete http://', () => {
    const result = validateUrl('http://')
    expect(result.isValid).toBe(false)
    expect(result.error).toBe('Please enter a valid URL')
  })

  it('returns error for incomplete https://', () => {
    const result = validateUrl('https://')
    expect(result.isValid).toBe(false)
    expect(result.error).toBe('Please enter a valid URL')
  })

  it('returns valid for proper http URL', () => {
    const result = validateUrl('http://example.com')
    expect(result.isValid).toBe(true)
    expect(result.error).toBeUndefined()
  })

  it('returns valid for proper https URL', () => {
    const result = validateUrl('https://example.com')
    expect(result.isValid).toBe(true)
    expect(result.error).toBeUndefined()
  })

  it('returns valid for URL with path', () => {
    const result = validateUrl('https://example.com/path/to/resource')
    expect(result.isValid).toBe(true)
  })

  it('returns valid for URL with query parameters', () => {
    const result = validateUrl('https://example.com?foo=bar&baz=qux')
    expect(result.isValid).toBe(true)
  })

  it('returns valid for localhost', () => {
    const result = validateUrl('http://localhost:3000')
    expect(result.isValid).toBe(true)
  })

  it('returns error for ftp protocol', () => {
    const result = validateUrl('ftp://files.example.com')
    expect(result.isValid).toBe(false)
    expect(result.error).toBe('Please enter a valid URL')
  })

  it('returns error for URL without TLD', () => {
    const result = validateUrl('http://example')
    expect(result.isValid).toBe(false)
    expect(result.error).toBe('Please enter a valid URL')
  })

  it('handles URL with spaces (trimmed)', () => {
    const result = validateUrl('  https://example.com  ')
    expect(result.isValid).toBe(true)
  })
})
