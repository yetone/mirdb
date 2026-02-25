/**
 * MSW Request Handlers
 * Owner: First builder to run
 *
 * Mock Service Worker handlers for API mocking in tests.
 */
import { http, HttpResponse } from 'msw'

export const handlers = [
  // URL shortening endpoint (unauthenticated)
  http.post('/api/urls/', async ({ request }) => {
    const body = await request.json() as { original_url: string }

    if (!body.original_url) {
      return HttpResponse.json(
        { detail: 'original_url is required' },
        { status: 422 }
      )
    }

    // Simulate successful URL shortening
    return HttpResponse.json({
      id: 1,
      original_url: body.original_url,
      short_code: 'abc123',
      created_at: new Date().toISOString(),
      user_id: null,
      click_count: 0,
    }, { status: 201 })
  }),

  // Get current user info
  http.get('/api/users/me', () => {
    return HttpResponse.json({
      id: 1,
      username: 'testuser',
      email: 'test@example.com',
      is_admin: false,
      created_at: new Date().toISOString(),
    })
  }),

  // Authentication endpoint
  http.post('/token', async ({ request }) => {
    const body = await request.formData()
    const username = body.get('username')
    const password = body.get('password')

    if (username === 'testuser' && password === 'password') {
      return HttpResponse.json({
        access_token: 'mock-jwt-token',
        token_type: 'bearer',
      })
    }

    return HttpResponse.json(
      { detail: 'Incorrect username or password' },
      { status: 401 }
    )
  }),
]

// Error handlers for testing error scenarios
export const errorHandlers = {
  networkError: http.post('/api/urls/', () => {
    return HttpResponse.error()
  }),

  serverError: http.post('/api/urls/', () => {
    return HttpResponse.json(
      { detail: 'Internal server error' },
      { status: 500 }
    )
  }),

  validationError: http.post('/api/urls/', () => {
    return HttpResponse.json(
      { detail: 'Invalid URL format' },
      { status: 422 }
    )
  }),
}
