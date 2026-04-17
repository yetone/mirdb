/**
 * UrlShortenerForm Component Tests
 * Owner: Scenario 2 - Inline URL Shortening Demo
 *
 * Test cases:
 * 1. Unit: Input field exists with placeholder text
 * 2. Integration: Valid URL submission shows loading then result
 * 3. Unit: Invalid URL shows error message
 * 4. E2E: Copy button copies to clipboard
 * 5. Integration: Enter key submits form
 * 6. Integration: API error shows error message
 * 7. Unit: Empty URL shows validation error
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor, act } from '../../test-utils'
import { UrlShortenerForm } from '../../../src/components/homepage/UrlShortenerForm'
import * as api from '../../../src/api'

// Mock the API module
vi.mock('../../../src/api', () => ({
  shortenUrl: vi.fn(),
}))

// Mock clipboard API
const mockWriteText = vi.fn().mockResolvedValue(undefined)

describe('UrlShortenerForm', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    // Mock clipboard API
    Object.assign(navigator, {
      clipboard: {
        writeText: mockWriteText,
      },
    })
  })

  /**
   * Test Case 1: Unit test - Input field exists with placeholder text
   * Input: Render homepage and query for URL input field
   * Expected: Input field with placeholder text exists in hero section
   */
  describe('TC1: URL input field exists', () => {
    it('should render URL input field with placeholder text', () => {
      render(<UrlShortenerForm />)

      const input = screen.getByTestId('url-input')
      expect(input).toBeInTheDocument()
      expect(input).toHaveAttribute('placeholder', 'Enter your long URL here...')
    })

    it('should render shorten button', () => {
      render(<UrlShortenerForm />)

      const button = screen.getByTestId('shorten-button')
      expect(button).toBeInTheDocument()
      expect(button).toHaveTextContent('Shorten URL')
    })

    it('should accept custom placeholder text', () => {
      render(<UrlShortenerForm placeholder="Paste URL here" />)

      const input = screen.getByTestId('url-input')
      expect(input).toHaveAttribute('placeholder', 'Paste URL here')
    })

    it('should have proper accessibility attributes', () => {
      render(<UrlShortenerForm />)

      const input = screen.getByTestId('url-input')
      expect(input).toHaveAttribute('aria-label', 'URL to shorten')
    })
  })

  /**
   * Test Case 2: Integration test - Valid URL submission
   * Input: Enter 'https://example.com/very/long/url/path' and submit
   * Expected: Loading state is shown, then shortened URL is displayed
   */
  describe('TC2: Valid URL submission', () => {
    it('should show loading state then display shortened URL', async () => {
      const mockResponse = {
        shortCode: 'abc123',
        shortUrl: 'https://short.url/abc123',
        originalUrl: 'https://example.com/very/long/url/path',
        createdAt: new Date().toISOString(),
      }

      vi.mocked(api.shortenUrl).mockResolvedValueOnce(mockResponse)

      render(<UrlShortenerForm />)

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      // Enter URL
      fireEvent.change(input, { target: { value: 'https://example.com/very/long/url/path' } })

      // Submit
      fireEvent.click(button)

      // Check loading state
      await waitFor(() => {
        expect(screen.getByText('Shortening...')).toBeInTheDocument()
      })

      // Check success state
      await waitFor(() => {
        expect(screen.getByTestId('success-state')).toBeInTheDocument()
        expect(screen.getByTestId('shortened-url')).toHaveTextContent('https://short.url/abc123')
      })
    })

    it('should call onSuccess callback when URL is shortened', async () => {
      const mockResponse = {
        shortCode: 'abc123',
        shortUrl: 'https://short.url/abc123',
        originalUrl: 'https://example.com',
        createdAt: new Date().toISOString(),
      }

      vi.mocked(api.shortenUrl).mockResolvedValueOnce(mockResponse)

      const onSuccess = vi.fn()
      render(<UrlShortenerForm onSuccess={onSuccess} />)

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      await waitFor(() => {
        expect(onSuccess).toHaveBeenCalledWith('https://short.url/abc123', 'https://example.com')
      })
    })
  })

  /**
   * Test Case 3: Unit test - Invalid URL error
   * Input: Enter invalid URL 'ftp://example.com' and submit (not http/https)
   * Expected: Error message is displayed indicating invalid URL format
   */
  describe('TC3: Invalid URL shows error', () => {
    it('should show error message for invalid URL format', async () => {
      render(<UrlShortenerForm />)

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      // Use ftp:// which is not http/https
      fireEvent.change(input, { target: { value: 'ftp://example.com' } })
      fireEvent.click(button)

      await waitFor(() => {
        const errorMessage = screen.getByTestId('error-message')
        expect(errorMessage).toBeInTheDocument()
        expect(errorMessage).toHaveTextContent('Please enter a valid URL')
      })
    })

    it('should have error styling on input', async () => {
      render(<UrlShortenerForm />)

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'ftp://example.com' } })
      fireEvent.click(button)

      await waitFor(() => {
        expect(input).toHaveAttribute('aria-invalid', 'true')
      })
    })
  })

  /**
   * Test Case 4: E2E test - Copy to clipboard
   * Input: Successfully shorten URL and click copy button
   * Expected: Shortened URL is copied to clipboard and success feedback is shown
   */
  describe('TC4: Copy to clipboard', () => {
    it('should copy shortened URL to clipboard and show success feedback', async () => {
      const mockResponse = {
        shortCode: 'abc123',
        shortUrl: 'https://short.url/abc123',
        originalUrl: 'https://example.com',
        createdAt: new Date().toISOString(),
      }

      vi.mocked(api.shortenUrl).mockResolvedValueOnce(mockResponse)

      render(<UrlShortenerForm />)

      // Shorten URL first
      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      // Wait for success state
      await waitFor(() => {
        expect(screen.getByTestId('success-state')).toBeInTheDocument()
      })

      // Click copy button
      const copyButton = screen.getByTestId('copy-button')
      await act(async () => {
        fireEvent.click(copyButton)
      })

      // Check clipboard was called
      expect(mockWriteText).toHaveBeenCalledWith('https://short.url/abc123')

      // Check success feedback
      await waitFor(() => {
        expect(copyButton).toHaveTextContent('Copied!')
      })
    })
  })

  /**
   * Test Case 5: Integration test - Enter key submission
   * Input: Press Enter key while URL input is focused
   * Expected: Form submits same as clicking the shorten button
   */
  describe('TC5: Enter key submission', () => {
    it('should submit form when Enter is pressed', async () => {
      const mockResponse = {
        shortCode: 'abc123',
        shortUrl: 'https://short.url/abc123',
        originalUrl: 'https://example.com',
        createdAt: new Date().toISOString(),
      }

      vi.mocked(api.shortenUrl).mockResolvedValueOnce(mockResponse)

      render(<UrlShortenerForm />)

      const input = screen.getByTestId('url-input')

      // Type URL and press Enter
      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.keyDown(input, { key: 'Enter', code: 'Enter' })

      // Check that API was called
      await waitFor(() => {
        expect(api.shortenUrl).toHaveBeenCalledWith({ url: 'https://example.com' })
      })

      // Check success state
      await waitFor(() => {
        expect(screen.getByTestId('success-state')).toBeInTheDocument()
      })
    })
  })

  /**
   * Test Case 6: Integration test - API error handling
   * Input: Submit URL when API is unavailable
   * Expected: Error state is displayed with appropriate error message
   */
  describe('TC6: API error handling', () => {
    it('should show error message when API fails', async () => {
      vi.mocked(api.shortenUrl).mockRejectedValueOnce(new Error('Network error'))

      render(<UrlShortenerForm />)

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      await waitFor(() => {
        const errorMessage = screen.getByTestId('error-message')
        expect(errorMessage).toBeInTheDocument()
        expect(errorMessage).toHaveTextContent('Network error')
      })
    })

    it('should show generic error message for non-Error exceptions', async () => {
      vi.mocked(api.shortenUrl).mockRejectedValueOnce('Unknown error')

      render(<UrlShortenerForm />)

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      await waitFor(() => {
        const errorMessage = screen.getByTestId('error-message')
        expect(errorMessage).toHaveTextContent('Failed to shorten URL. Please try again.')
      })
    })
  })

  /**
   * Test Case 7: Unit test - Empty URL validation
   * Input: Submit empty URL field
   * Expected: Validation error prevents submission and shows error message
   */
  describe('TC7: Empty URL validation', () => {
    it('should disable button for empty URL', () => {
      render(<UrlShortenerForm />)

      const button = screen.getByTestId('shorten-button')

      // Button should be disabled when input is empty
      expect(button).toBeDisabled()
    })

    it('should disable button for whitespace-only URL', () => {
      render(<UrlShortenerForm />)

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: '   ' } })

      // Button should still be disabled for whitespace (trim is applied)
      expect(button).toBeDisabled()
    })

    it('should not call API when input is empty', () => {
      render(<UrlShortenerForm />)

      const input = screen.getByTestId('url-input')

      // Try to submit with Enter on empty input
      fireEvent.keyDown(input, { key: 'Enter', code: 'Enter' })

      expect(api.shortenUrl).not.toHaveBeenCalled()
    })
  })

  describe('Reset functionality', () => {
    it('should allow shortening another URL after reset', async () => {
      const mockResponse = {
        shortCode: 'abc123',
        shortUrl: 'https://short.url/abc123',
        originalUrl: 'https://example.com',
        createdAt: new Date().toISOString(),
      }

      vi.mocked(api.shortenUrl).mockResolvedValue(mockResponse)

      render(<UrlShortenerForm />)

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      // First shortening
      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      await waitFor(() => {
        expect(screen.getByTestId('success-state')).toBeInTheDocument()
      })

      // Click reset button
      const resetButton = screen.getByTestId('reset-button')
      fireEvent.click(resetButton)

      // Should be back to initial state
      await waitFor(() => {
        expect(screen.queryByTestId('success-state')).not.toBeInTheDocument()
        expect(screen.getByTestId('url-input')).toHaveValue('')
      })
    })
  })
})
