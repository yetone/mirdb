/**
 * UrlShortenerForm Component Tests
 * Owner: Scenario 2 - Inline URL Shortening Demo
 * Enhanced: Scenario 15 - Error States and Edge Cases
 *
 * Test cases:
 * 1. Unit: Input field exists with placeholder text
 * 2. Integration: Valid URL submission shows loading then result
 * 3. Unit: Invalid URL shows error message
 * 4. E2E: Copy button copies to clipboard
 * 5. Integration: Enter key submits form
 * 6. Integration: API error shows error message
 * 7. Unit: Empty URL shows validation error
 *
 * Scenario 15 Edge Cases:
 * - TC1: Network offline error handling
 * - TC2: Malicious URL schemes (javascript:)
 * - TC3: URLs with invalid characters (spaces)
 * - TC4: Extremely long URLs
 * - TC5: Rate limiting feedback
 * - TC6: Special characters in URL
 * - TC7: Server 500 error handling
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
        // User-friendly error message for network errors
        expect(errorMessage).toHaveTextContent('Unable to connect')
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

  /**
   * Scenario 15 - Error States and Edge Cases
   */

  /**
   * TC1: Network offline error handling
   * Input: Submit URL when network is offline
   * Expected: Error message indicates network/connection issue
   */
  describe('TC1 (Scenario 15): Network offline error handling', () => {
    it('should show user-friendly message for network errors', async () => {
      vi.mocked(api.shortenUrl).mockRejectedValueOnce(new Error('Network Error'))

      render(<UrlShortenerForm />)

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      await waitFor(() => {
        const errorMessage = screen.getByTestId('error-message')
        expect(errorMessage).toBeInTheDocument()
        expect(errorMessage).toHaveTextContent('Unable to connect')
      })
    })

    it('should show user-friendly message for fetch failures', async () => {
      vi.mocked(api.shortenUrl).mockRejectedValueOnce(new Error('Failed to fetch'))

      render(<UrlShortenerForm />)

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      await waitFor(() => {
        const errorMessage = screen.getByTestId('error-message')
        expect(errorMessage).toHaveTextContent('Unable to connect')
        expect(errorMessage).toHaveTextContent('check your internet connection')
      })
    })
  })

  /**
   * TC2: Malicious URL schemes
   * Input: Submit URL 'javascript:alert(1)'
   * Expected: Validation rejects potentially malicious URL schemes
   */
  describe('TC2 (Scenario 15): Malicious URL scheme rejection', () => {
    it('should reject javascript: URLs with error message', async () => {
      render(<UrlShortenerForm />)

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'javascript:alert(1)' } })
      fireEvent.click(button)

      await waitFor(() => {
        const errorMessage = screen.getByTestId('error-message')
        expect(errorMessage).toBeInTheDocument()
        expect(errorMessage).toHaveTextContent('unsafe protocol')
      })

      // API should not be called
      expect(api.shortenUrl).not.toHaveBeenCalled()
    })

    it('should reject data: URLs with error message', async () => {
      render(<UrlShortenerForm />)

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'data:text/html,<h1>test</h1>' } })
      fireEvent.click(button)

      await waitFor(() => {
        const errorMessage = screen.getByTestId('error-message')
        expect(errorMessage).toHaveTextContent('unsafe protocol')
      })

      expect(api.shortenUrl).not.toHaveBeenCalled()
    })
  })

  /**
   * TC3: URLs with spaces
   * Input: Submit URL with spaces 'https://exa mple.com'
   * Expected: Validation error indicates invalid URL format
   */
  describe('TC3 (Scenario 15): URL with invalid characters', () => {
    it('should show error for URL with spaces in domain', async () => {
      render(<UrlShortenerForm />)

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://exa mple.com' } })
      fireEvent.click(button)

      await waitFor(() => {
        const errorMessage = screen.getByTestId('error-message')
        expect(errorMessage).toBeInTheDocument()
        expect(errorMessage).toHaveTextContent('invalid characters')
      })

      expect(api.shortenUrl).not.toHaveBeenCalled()
    })

    it('should show error for URL with spaces in path', async () => {
      render(<UrlShortenerForm />)

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com/path with spaces' } })
      fireEvent.click(button)

      await waitFor(() => {
        const errorMessage = screen.getByTestId('error-message')
        expect(errorMessage).toHaveTextContent('invalid characters')
      })

      expect(api.shortenUrl).not.toHaveBeenCalled()
    })
  })

  /**
   * TC4: Extremely long URLs (>2000 characters)
   * Input: Submit extremely long URL
   * Expected: System handles gracefully with appropriate error or success
   */
  describe('TC4 (Scenario 15): Extremely long URL handling', () => {
    it('should reject URLs exceeding maximum length', async () => {
      const longUrl = 'https://example.com/' + 'a'.repeat(2100)

      render(<UrlShortenerForm />)

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: longUrl } })
      fireEvent.click(button)

      await waitFor(() => {
        const errorMessage = screen.getByTestId('error-message')
        expect(errorMessage).toBeInTheDocument()
        expect(errorMessage).toHaveTextContent('exceeds maximum length')
      })

      expect(api.shortenUrl).not.toHaveBeenCalled()
    })

    it('should accept URLs at or near the limit', async () => {
      const mockResponse = {
        shortCode: 'abc123',
        shortUrl: 'https://short.url/abc123',
        originalUrl: 'https://example.com/longpath',
        createdAt: new Date().toISOString(),
      }

      vi.mocked(api.shortenUrl).mockResolvedValueOnce(mockResponse)

      const baseUrl = 'https://example.com/'
      const url = baseUrl + 'a'.repeat(2000)

      render(<UrlShortenerForm />)

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: url } })
      fireEvent.click(button)

      await waitFor(() => {
        expect(api.shortenUrl).toHaveBeenCalled()
      })
    })
  })

  /**
   * TC5: Rate limiting
   * Input: Submit 10 URLs within 10 seconds
   * Expected: Rate limiting feedback is shown if applicable
   */
  describe('TC5 (Scenario 15): Rate limiting feedback', () => {
    it('should show rate limit error message when API returns rate limit error', async () => {
      vi.mocked(api.shortenUrl).mockRejectedValueOnce(new Error('Rate limit exceeded'))

      render(<UrlShortenerForm />)

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      await waitFor(() => {
        const errorMessage = screen.getByTestId('error-message')
        expect(errorMessage).toBeInTheDocument()
        expect(errorMessage).toHaveTextContent('Too many requests')
        expect(errorMessage).toHaveTextContent('wait a moment')
      })
    })

    it('should show rate limit error for "too many requests" message', async () => {
      vi.mocked(api.shortenUrl).mockRejectedValueOnce(new Error('Too many requests'))

      render(<UrlShortenerForm />)

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      await waitFor(() => {
        const errorMessage = screen.getByTestId('error-message')
        expect(errorMessage).toHaveTextContent('Too many requests')
      })
    })
  })

  /**
   * TC6: Special characters in URL path
   * Input: Submit URL with special characters in path
   * Expected: URL is properly encoded and shortened successfully
   */
  describe('TC6 (Scenario 15): Special characters in URL', () => {
    it('should successfully shorten URL with encoded special characters', async () => {
      const mockResponse = {
        shortCode: 'abc123',
        shortUrl: 'https://short.url/abc123',
        originalUrl: 'https://example.com/path%20with%20spaces',
        createdAt: new Date().toISOString(),
      }

      vi.mocked(api.shortenUrl).mockResolvedValueOnce(mockResponse)

      render(<UrlShortenerForm />)

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com/path%20with%20spaces' } })
      fireEvent.click(button)

      await waitFor(() => {
        expect(screen.getByTestId('success-state')).toBeInTheDocument()
      })
    })

    it('should successfully shorten URL with query parameters', async () => {
      const mockResponse = {
        shortCode: 'abc123',
        shortUrl: 'https://short.url/abc123',
        originalUrl: 'https://example.com/path?key=value&foo=bar',
        createdAt: new Date().toISOString(),
      }

      vi.mocked(api.shortenUrl).mockResolvedValueOnce(mockResponse)

      render(<UrlShortenerForm />)

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com/path?key=value&foo=bar' } })
      fireEvent.click(button)

      await waitFor(() => {
        expect(screen.getByTestId('success-state')).toBeInTheDocument()
        expect(api.shortenUrl).toHaveBeenCalledWith({ url: 'https://example.com/path?key=value&foo=bar' })
      })
    })

    it('should successfully shorten URL with fragment identifier', async () => {
      const mockResponse = {
        shortCode: 'abc123',
        shortUrl: 'https://short.url/abc123',
        originalUrl: 'https://example.com/path#section',
        createdAt: new Date().toISOString(),
      }

      vi.mocked(api.shortenUrl).mockResolvedValueOnce(mockResponse)

      render(<UrlShortenerForm />)

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com/path#section' } })
      fireEvent.click(button)

      await waitFor(() => {
        expect(screen.getByTestId('success-state')).toBeInTheDocument()
      })
    })
  })

  /**
   * TC7: Server 500 error
   * Input: Submit URL that returns 500 error from API
   * Expected: User-friendly error message is displayed, not raw error
   */
  describe('TC7 (Scenario 15): Server error handling', () => {
    it('should show user-friendly message for 500 errors', async () => {
      vi.mocked(api.shortenUrl).mockRejectedValueOnce(new Error('500 Internal Server Error'))

      render(<UrlShortenerForm />)

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      await waitFor(() => {
        const errorMessage = screen.getByTestId('error-message')
        expect(errorMessage).toBeInTheDocument()
        expect(errorMessage).toHaveTextContent('Something went wrong on our end')
        expect(errorMessage).toHaveTextContent('try again later')
      })
    })

    it('should show user-friendly message for internal server errors', async () => {
      vi.mocked(api.shortenUrl).mockRejectedValueOnce(new Error('Internal server error'))

      render(<UrlShortenerForm />)

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      await waitFor(() => {
        const errorMessage = screen.getByTestId('error-message')
        expect(errorMessage).toHaveTextContent('Something went wrong on our end')
      })
    })

    it('should not expose raw error details to user', async () => {
      vi.mocked(api.shortenUrl).mockRejectedValueOnce(new Error('500: Database connection failed at pool.js:142'))

      render(<UrlShortenerForm />)

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      await waitFor(() => {
        const errorMessage = screen.getByTestId('error-message')
        // Should show user-friendly message, not raw database error
        expect(errorMessage).toHaveTextContent('Something went wrong on our end')
        expect(errorMessage).not.toHaveTextContent('Database')
        expect(errorMessage).not.toHaveTextContent('pool.js')
      })
    })
  })

  /**
   * Additional error state tests
   */
  describe('Error state accessibility', () => {
    it('should have proper error role and aria attributes', async () => {
      vi.mocked(api.shortenUrl).mockRejectedValueOnce(new Error('Network Error'))

      render(<UrlShortenerForm />)

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      await waitFor(() => {
        const errorMessage = screen.getByTestId('error-message')
        expect(errorMessage).toHaveAttribute('role', 'alert')
      })
    })

    it('should mark input as invalid when error occurs', async () => {
      render(<UrlShortenerForm />)

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'javascript:alert(1)' } })
      fireEvent.click(button)

      await waitFor(() => {
        expect(input).toHaveAttribute('aria-invalid', 'true')
      })
    })
  })
})
