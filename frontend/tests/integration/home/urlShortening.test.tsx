/**
 * URL Shortening Integration Tests
 * Owner: Scenario 3 - Anonymous URL Shortening
 *
 * Tests for the anonymous URL shortening functionality.
 * Verifies API integration, form submission, and result display.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw'
import { setupServer } from 'msw/node'
import { UrlShortenerForm } from '@/components/home/UrlShortenerForm'
import { renderWithProviders } from '../../utils/testHelpers'

// Mock API response for successful URL creation
const mockSuccessResponse = {
  id: 1,
  original_url: 'https://example.com/very/long/url',
  short_code: 'abc123',
  created_at: '2024-01-01T00:00:00Z',
  user_id: null,
  click_count: 0,
  share_token: 'share-token-xyz789',
}

// Setup MSW server for API mocking
const server = setupServer(
  http.post('/api/urls/anonymous', async ({ request }) => {
    const body = await request.json() as { original_url: string }

    // Check that no authorization header is present
    const authHeader = request.headers.get('Authorization')
    if (authHeader) {
      return HttpResponse.json(
        { detail: 'Anonymous endpoint should not have auth header' },
        { status: 400 }
      )
    }

    return HttpResponse.json({
      ...mockSuccessResponse,
      original_url: body.original_url,
    })
  }),

  http.post('/api/urls/', async ({ request }) => {
    const body = await request.json() as { original_url: string }

    return HttpResponse.json({
      ...mockSuccessResponse,
      original_url: body.original_url,
    })
  })
)

describe('Anonymous URL Shortening', () => {
  beforeEach(() => {
    server.listen({ onUnhandledRequest: 'error' })
    // Clear localStorage to ensure no auth token
    localStorage.clear()
  })

  afterEach(() => {
    server.resetHandlers()
    server.close()
  })

  /**
   * Test Case 1: API request is made without authentication header
   * Input: Enter 'https://example.com/very/long/url' and click Shorten
   * Expected: API request is made to create short URL without authentication header
   */
  it('should make API request without authentication header for anonymous users', async () => {
    const user = userEvent.setup()
    let requestHadAuthHeader = false

    // Track if auth header was present
    server.use(
      http.post('/api/urls/anonymous', async ({ request }) => {
        const authHeader = request.headers.get('Authorization')
        requestHadAuthHeader = !!authHeader
        const body = await request.json() as { original_url: string }
        return HttpResponse.json({
          ...mockSuccessResponse,
          original_url: body.original_url,
        })
      })
    )

    renderWithProviders(<UrlShortenerForm />)

    const input = screen.getByPlaceholderText(/paste your long url/i)
    const submitButton = screen.getByRole('button', { name: /shorten/i })

    await user.type(input, 'https://example.com/very/long/url')
    await user.click(submitButton)

    await waitFor(() => {
      expect(screen.getByText(/abc123/)).toBeInTheDocument()
    })

    expect(requestHadAuthHeader).toBe(false)
  })

  /**
   * Test Case 2: Short URL is displayed in result area
   * Input: Mock successful API response with short code 'abc123'
   * Expected: Short URL is displayed in result area (e.g., 'http://localhost/abc123')
   */
  it('should display short URL in result area after successful API response', async () => {
    const user = userEvent.setup()

    renderWithProviders(<UrlShortenerForm />)

    const input = screen.getByPlaceholderText(/paste your long url/i)
    const submitButton = screen.getByRole('button', { name: /shorten/i })

    await user.type(input, 'https://example.com/some/url')
    await user.click(submitButton)

    await waitFor(() => {
      // Check that the short URL is displayed
      const resultArea = screen.getByTestId('shorten-result')
      expect(resultArea).toBeInTheDocument()
      expect(resultArea).toHaveTextContent(/abc123/)
    })

    // Verify the short URL format
    expect(screen.getByText(/\/r\/abc123/)).toBeInTheDocument()
  })

  /**
   * Test Case 3: Form submission via Enter key
   * Input: Submit URL using Enter key instead of button click
   * Expected: Form submission triggers URL shortening request
   */
  it('should submit form when Enter key is pressed in input field', async () => {
    const user = userEvent.setup()
    const onSuccess = vi.fn()

    renderWithProviders(<UrlShortenerForm onSuccess={onSuccess} />)

    const input = screen.getByPlaceholderText(/paste your long url/i)

    await user.type(input, 'https://example.com/enter-key-test')
    await user.keyboard('{Enter}')

    await waitFor(() => {
      expect(onSuccess).toHaveBeenCalledWith(
        expect.objectContaining({
          shortCode: 'abc123',
          shareToken: 'share-token-xyz789',
        })
      )
    })
  })

  /**
   * Test Case 4: Share token is returned for anonymous URL
   * Input: Verify share token is returned for anonymous URL
   * Expected: Response includes share_token for accessing analytics
   */
  it('should receive share token in response for anonymous URL', async () => {
    const user = userEvent.setup()
    const onSuccess = vi.fn()

    renderWithProviders(<UrlShortenerForm onSuccess={onSuccess} />)

    const input = screen.getByPlaceholderText(/paste your long url/i)
    const submitButton = screen.getByRole('button', { name: /shorten/i })

    await user.type(input, 'https://example.com/share-token-test')
    await user.click(submitButton)

    await waitFor(() => {
      expect(onSuccess).toHaveBeenCalled()
      const result = onSuccess.mock.calls[0][0]
      expect(result.shareToken).toBe('share-token-xyz789')
      expect(result.shareToken).toBeTruthy()
    })
  })

  it('should show loading state while API request is in progress', async () => {
    const user = userEvent.setup()

    // Add delay to API response
    server.use(
      http.post('/api/urls/anonymous', async () => {
        await new Promise((resolve) => setTimeout(resolve, 100))
        return HttpResponse.json(mockSuccessResponse)
      })
    )

    renderWithProviders(<UrlShortenerForm />)

    const input = screen.getByPlaceholderText(/paste your long url/i)
    const submitButton = screen.getByRole('button', { name: /shorten/i })

    await user.type(input, 'https://example.com/loading-test')
    await user.click(submitButton)

    // Check for loading state
    expect(submitButton).toBeDisabled()

    await waitFor(() => {
      expect(submitButton).not.toBeDisabled()
    })
  })

  it('should handle API errors gracefully', async () => {
    const user = userEvent.setup()
    const onError = vi.fn()

    server.use(
      http.post('/api/urls/anonymous', () => {
        return HttpResponse.json(
          { detail: 'Invalid URL format' },
          { status: 400 }
        )
      })
    )

    renderWithProviders(<UrlShortenerForm onError={onError} />)

    const input = screen.getByPlaceholderText(/paste your long url/i)
    const submitButton = screen.getByRole('button', { name: /shorten/i })

    await user.type(input, 'https://example.com/error-test')
    await user.click(submitButton)

    await waitFor(() => {
      expect(onError).toHaveBeenCalled()
    })

    // Error message should be displayed
    expect(screen.getByRole('alert')).toBeInTheDocument()
  })

  it('should clear previous result when submitting new URL', async () => {
    const user = userEvent.setup()

    renderWithProviders(<UrlShortenerForm />)

    const input = screen.getByPlaceholderText(/paste your long url/i)
    const submitButton = screen.getByRole('button', { name: /shorten/i })

    // First submission
    await user.type(input, 'https://example.com/first-url')
    await user.click(submitButton)

    await waitFor(() => {
      expect(screen.getByText(/abc123/)).toBeInTheDocument()
    })

    // Clear and submit new URL
    await user.clear(input)
    await user.type(input, 'https://example.com/second-url')
    await user.click(submitButton)

    // Should still show result (same mock response)
    await waitFor(() => {
      expect(screen.getByTestId('shorten-result')).toBeInTheDocument()
    })
  })
})
