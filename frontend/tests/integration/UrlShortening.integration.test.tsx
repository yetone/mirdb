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
        expect(screen.getByRole('alert')).toHaveTextContent('Failed to shorten URL')
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
        expect(screen.getByRole('alert')).toHaveTextContent('Failed to shorten URL')
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
