/**
 * Integration Tests for URL Shortening
 * Owner: Scenario 2 - URL Shortening Form Functionality
 *
 * Tests:
 * - API call to /api/urls/ endpoint
 * - Full form submission flow
 * - API error handling
 */
import { describe, it, expect, vi } from 'vitest'
import { render, screen, waitFor } from '../test-utils'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw'
import { server } from '../mocks/server'
import { UrlShortenForm } from '../../src/components/homepage/UrlShortenForm'
import Home from '../../src/pages/Home'

describe('URL Shortening Integration', () => {
  describe('API Integration', () => {
    it('makes API call to /api/urls/ endpoint with valid URL', async () => {
      const apiCallSpy = vi.fn()

      server.use(
        http.post('/api/urls/', async ({ request }) => {
          const body = await request.json() as { original_url: string }
          apiCallSpy(body)
          return HttpResponse.json({
            id: 1,
            original_url: body.original_url,
            short_code: 'abc123',
            created_at: new Date().toISOString(),
            user_id: null,
            click_count: 0,
          }, { status: 201 })
        })
      )

      render(<UrlShortenForm />)

      const input = screen.getByRole('textbox', { name: /url input/i })
      await userEvent.type(input, 'https://example.com/very/long/path/to/resource')

      const submitButton = screen.getByRole('button', { name: /shorten url/i })
      await userEvent.click(submitButton)

      await waitFor(() => {
        expect(apiCallSpy).toHaveBeenCalledWith({
          original_url: 'https://example.com/very/long/path/to/resource',
        })
      })
    })

    it('displays short URL after successful API response', async () => {
      server.use(
        http.post('/api/urls/', async ({ request }) => {
          const body = await request.json() as { original_url: string }
          return HttpResponse.json({
            id: 1,
            original_url: body.original_url,
            short_code: 'xyz789',
            created_at: new Date().toISOString(),
            user_id: null,
            click_count: 0,
          }, { status: 201 })
        })
      )

      render(<UrlShortenForm />)

      const input = screen.getByRole('textbox', { name: /url input/i })
      await userEvent.type(input, 'https://example.com/path')

      const submitButton = screen.getByRole('button', { name: /shorten url/i })
      await userEvent.click(submitButton)

      await waitFor(() => {
        expect(screen.getByDisplayValue(/\/r\/xyz789/)).toBeInTheDocument()
      })
    })

    it('submits form when Enter key is pressed in URL input', async () => {
      const apiCallSpy = vi.fn()

      server.use(
        http.post('/api/urls/', async ({ request }) => {
          const body = await request.json() as { original_url: string }
          apiCallSpy(body)
          return HttpResponse.json({
            id: 1,
            original_url: body.original_url,
            short_code: 'abc123',
            created_at: new Date().toISOString(),
            user_id: null,
            click_count: 0,
          }, { status: 201 })
        })
      )

      render(<UrlShortenForm />)

      const input = screen.getByRole('textbox', { name: /url input/i })
      await userEvent.type(input, 'https://example.com/enter-key-test{enter}')

      await waitFor(() => {
        expect(apiCallSpy).toHaveBeenCalledWith({
          original_url: 'https://example.com/enter-key-test',
        })
      })

      await waitFor(() => {
        expect(screen.getByLabelText(/shortened url result/i)).toBeInTheDocument()
      })
    })
  })

  describe('Error Handling', () => {
    it('displays error message on network failure', async () => {
      server.use(
        http.post('/api/urls/', () => {
          return HttpResponse.error()
        })
      )

      render(<UrlShortenForm />)

      const input = screen.getByRole('textbox', { name: /url input/i })
      await userEvent.type(input, 'https://example.com/path')

      const submitButton = screen.getByRole('button', { name: /shorten url/i })
      await userEvent.click(submitButton)

      await waitFor(() => {
        expect(screen.getByRole('alert')).toHaveTextContent(/unable to connect/i)
      })
    })

    it('displays error message on server error', async () => {
      server.use(
        http.post('/api/urls/', () => {
          return HttpResponse.json(
            { detail: 'Internal server error' },
            { status: 500 }
          )
        })
      )

      render(<UrlShortenForm />)

      const input = screen.getByRole('textbox', { name: /url input/i })
      await userEvent.type(input, 'https://example.com/path')

      const submitButton = screen.getByRole('button', { name: /shorten url/i })
      await userEvent.click(submitButton)

      await waitFor(() => {
        expect(screen.getByRole('alert')).toHaveTextContent(/something went wrong/i)
      })
    })
  })

  /**
   * API Error Handling Tests
   * Owner: Scenario 20 - API Error Handling
   *
   * Tests graceful handling when backend API is unavailable or returns errors.
   * Verifies user-friendly error messages are displayed and page remains functional.
   */
  describe('API Error Handling (Scenario 20)', () => {
    describe('Test Case 1: 500 Server Error', () => {
      it('displays user-friendly error message when API returns 500 error', async () => {
        // Mock API to return 500 error
        server.use(
          http.post('/api/urls/', () => {
            return HttpResponse.json(
              { detail: 'Internal server error' },
              { status: 500 }
            )
          })
        )

        render(<UrlShortenForm />)

        // Enter a valid URL
        const input = screen.getByRole('textbox', { name: /url input/i })
        await userEvent.type(input, 'https://example.com/test-500-error')

        // Submit the form
        const submitButton = screen.getByRole('button', { name: /shorten url/i })
        await userEvent.click(submitButton)

        // Verify user-friendly error message is displayed
        await waitFor(() => {
          const errorAlert = screen.getByRole('alert')
          expect(errorAlert).toBeInTheDocument()
          expect(errorAlert).toHaveTextContent('Something went wrong. Please try again.')
        })

        // Verify technical error details are NOT shown
        expect(screen.queryByText(/500/)).not.toBeInTheDocument()
        expect(screen.queryByText(/Internal server error/)).not.toBeInTheDocument()
      })

      it('does not crash the page on 500 error', async () => {
        server.use(
          http.post('/api/urls/', () => {
            return HttpResponse.json(
              { detail: 'Internal server error' },
              { status: 500 }
            )
          })
        )

        render(<UrlShortenForm />)

        const input = screen.getByRole('textbox', { name: /url input/i })
        await userEvent.type(input, 'https://example.com/crash-test')

        const submitButton = screen.getByRole('button', { name: /shorten url/i })
        await userEvent.click(submitButton)

        await waitFor(() => {
          expect(screen.getByRole('alert')).toBeInTheDocument()
        })

        // Page elements should still be functional
        expect(screen.getByRole('textbox', { name: /url input/i })).toBeInTheDocument()
        expect(screen.getByRole('button', { name: /shorten url/i })).toBeInTheDocument()
        expect(screen.getByRole('form')).toBeInTheDocument()
      })
    })

    describe('Test Case 2: Network Error', () => {
      it('displays connection error message when API is unreachable', async () => {
        // Mock network failure
        server.use(
          http.post('/api/urls/', () => {
            return HttpResponse.error()
          })
        )

        render(<UrlShortenForm />)

        const input = screen.getByRole('textbox', { name: /url input/i })
        await userEvent.type(input, 'https://example.com/network-error-test')

        const submitButton = screen.getByRole('button', { name: /shorten url/i })
        await userEvent.click(submitButton)

        // Verify error message indicates connection issue
        await waitFor(() => {
          const errorAlert = screen.getByRole('alert')
          expect(errorAlert).toBeInTheDocument()
          expect(errorAlert).toHaveTextContent(/unable to connect/i)
        })
      })

      it('suggests checking internet connection on network failure', async () => {
        server.use(
          http.post('/api/urls/', () => {
            return HttpResponse.error()
          })
        )

        render(<UrlShortenForm />)

        const input = screen.getByRole('textbox', { name: /url input/i })
        await userEvent.type(input, 'https://example.com/connectivity-test')

        const submitButton = screen.getByRole('button', { name: /shorten url/i })
        await userEvent.click(submitButton)

        await waitFor(() => {
          const errorAlert = screen.getByRole('alert')
          expect(errorAlert).toHaveTextContent(/internet connection/i)
        })
      })
    })

    describe('Test Case 3: Retry After Error', () => {
      it('allows form resubmission after API error is resolved', async () => {
        let requestCount = 0

        // First request fails with 500, subsequent requests succeed
        server.use(
          http.post('/api/urls/', async ({ request }) => {
            requestCount++
            if (requestCount === 1) {
              return HttpResponse.json(
                { detail: 'Internal server error' },
                { status: 500 }
              )
            }
            const body = await request.json() as { original_url: string }
            return HttpResponse.json({
              id: 1,
              original_url: body.original_url,
              short_code: 'retry123',
              created_at: new Date().toISOString(),
              user_id: null,
              click_count: 0,
            }, { status: 201 })
          })
        )

        render(<UrlShortenForm />)

        const input = screen.getByRole('textbox', { name: /url input/i })
        await userEvent.type(input, 'https://example.com/retry-test')

        const submitButton = screen.getByRole('button', { name: /shorten url/i })

        // First submission - should fail
        await userEvent.click(submitButton)

        await waitFor(() => {
          expect(screen.getByRole('alert')).toHaveTextContent(/something went wrong/i)
        })

        // Clear and re-enter URL for retry
        await userEvent.clear(input)
        await userEvent.type(input, 'https://example.com/retry-success')

        // Second submission - should succeed
        await userEvent.click(submitButton)

        // Wait for successful result
        await waitFor(() => {
          expect(screen.getByLabelText(/shortened url result/i)).toBeInTheDocument()
        })

        // Error should be cleared
        expect(screen.queryByRole('alert')).not.toBeInTheDocument()

        // Verify the shortened URL is displayed
        expect(screen.getByDisplayValue(/\/r\/retry123/)).toBeInTheDocument()
      })

      it('clears previous error when user starts typing new URL', async () => {
        server.use(
          http.post('/api/urls/', () => {
            return HttpResponse.json(
              { detail: 'Internal server error' },
              { status: 500 }
            )
          })
        )

        render(<UrlShortenForm />)

        const input = screen.getByRole('textbox', { name: /url input/i })
        await userEvent.type(input, 'https://example.com/clear-error-test')

        const submitButton = screen.getByRole('button', { name: /shorten url/i })
        await userEvent.click(submitButton)

        // Wait for error to appear
        await waitFor(() => {
          expect(screen.getByRole('alert')).toBeInTheDocument()
        })

        // Start typing new input - error should clear
        await userEvent.type(input, '/new-path')

        // Error should be cleared when user types
        await waitFor(() => {
          expect(screen.queryByRole('alert')).not.toBeInTheDocument()
        })
      })

      it('button remains enabled after error for retry', async () => {
        server.use(
          http.post('/api/urls/', () => {
            return HttpResponse.json(
              { detail: 'Internal server error' },
              { status: 500 }
            )
          })
        )

        render(<UrlShortenForm />)

        const input = screen.getByRole('textbox', { name: /url input/i })
        await userEvent.type(input, 'https://example.com/button-state-test')

        const submitButton = screen.getByRole('button', { name: /shorten url/i })
        await userEvent.click(submitButton)

        await waitFor(() => {
          expect(screen.getByRole('alert')).toBeInTheDocument()
        })

        // Button should not be disabled after error
        expect(submitButton).not.toBeDisabled()
        expect(submitButton).toHaveTextContent('Shorten URL')
      })
    })

    describe('Page Stability', () => {
      it('maintains form functionality after multiple consecutive errors', async () => {
        server.use(
          http.post('/api/urls/', () => {
            return HttpResponse.json(
              { detail: 'Service unavailable' },
              { status: 503 }
            )
          })
        )

        render(<UrlShortenForm />)

        const input = screen.getByRole('textbox', { name: /url input/i })
        const submitButton = screen.getByRole('button', { name: /shorten url/i })

        // Submit multiple times
        for (let i = 0; i < 3; i++) {
          await userEvent.clear(input)
          await userEvent.type(input, `https://example.com/test-${i}`)
          await userEvent.click(submitButton)

          await waitFor(() => {
            expect(screen.getByRole('alert')).toBeInTheDocument()
          })
        }

        // Form should still be functional
        expect(input).toBeInTheDocument()
        expect(input).not.toBeDisabled()
        expect(submitButton).not.toBeDisabled()
      })
    })
  })

  describe('Homepage Integration', () => {
    it('renders URL shortening form on homepage', () => {
      render(<Home />)

      expect(screen.getByRole('textbox', { name: /url input/i })).toBeInTheDocument()
      expect(screen.getByRole('button', { name: /shorten url/i })).toBeInTheDocument()
    })

    it('displays hero section with tagline', () => {
      render(<Home />)

      expect(screen.getByText(/shorten links/i)).toBeInTheDocument()
      expect(screen.getByText(/track insights/i)).toBeInTheDocument()
    })

    it('allows URL shortening from homepage without authentication', async () => {
      render(<Home />)

      const input = screen.getByRole('textbox', { name: /url input/i })
      await userEvent.type(input, 'https://example.com/test')

      const submitButton = screen.getByRole('button', { name: /shorten url/i })
      await userEvent.click(submitButton)

      await waitFor(() => {
        expect(screen.getByLabelText(/shortened url result/i)).toBeInTheDocument()
      })
    })
  })
})
