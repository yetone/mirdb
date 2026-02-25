/**
 * MSW Request Handlers
 * Owner: First builder to run
 * Owner: Scenario 19 - Short URL Redirect Verification
 *
 * Mock Service Worker handlers for API mocking in tests.
 */
import { http, HttpResponse } from 'msw'

// Store for dynamically created short URLs (Scenario 19)
export const urlStore: Map<string, { original_url: string; click_count: number }> = new Map()

// Counter for generating unique short codes - Scenario 19
let shortCodeCounter = 0

// Generate unique short code for testing
function generateShortCode(): string {
  shortCodeCounter += 1
  return `test${shortCodeCounter.toString().padStart(3, '0')}`
}

// Reset function for test cleanup - Scenario 19
export function resetUrlStore(): void {
  urlStore.clear()
  shortCodeCounter = 0
}

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

    // Generate unique short code and store URL - Scenario 19
    const shortCode = generateShortCode()
    urlStore.set(shortCode, {
      original_url: body.original_url,
      click_count: 0,
    })

    // Simulate successful URL shortening
    return HttpResponse.json({
      id: shortCodeCounter,
      original_url: body.original_url,
      short_code: shortCode,
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

  // Get URL by short code - Scenario 19 redirect verification
  http.get('/api/urls/:shortCode', ({ params }) => {
    const { shortCode } = params as { shortCode: string }
    const urlData = urlStore.get(shortCode)

    if (urlData) {
      return HttpResponse.json({
        id: 1,
        original_url: urlData.original_url,
        short_code: shortCode,
        created_at: new Date().toISOString(),
        user_id: null,
        click_count: urlData.click_count,
      })
    }

    return HttpResponse.json(
      { detail: 'URL not found' },
      { status: 404 }
    )
  }),

  // Increment click count endpoint - Scenario 19
  http.post('/api/urls/:shortCode/click', ({ params }) => {
    const { shortCode } = params as { shortCode: string }
    const urlData = urlStore.get(shortCode)

    if (urlData) {
      urlData.click_count += 1
      urlStore.set(shortCode, urlData)
      return HttpResponse.json({ success: true, click_count: urlData.click_count })
    }

    return HttpResponse.json(
      { detail: 'URL not found' },
      { status: 404 }
    )
  }),
]

// Get URL by short code - Scenario 19 redirect verification (exported separately for additional testing)
export const redirectHandlers = [
  http.get('/api/urls/:shortCode', ({ params }) => {
    const { shortCode } = params as { shortCode: string }
    const urlData = urlStore.get(shortCode)

    if (urlData) {
      return HttpResponse.json({
        id: 1,
        original_url: urlData.original_url,
        short_code: shortCode,
        created_at: new Date().toISOString(),
        user_id: null,
        click_count: urlData.click_count,
      })
    }

    return HttpResponse.json(
      { detail: 'URL not found' },
      { status: 404 }
    )
  }),

  // Increment click count endpoint - Scenario 19
  http.post('/api/urls/:shortCode/click', ({ params }) => {
    const { shortCode } = params as { shortCode: string }
    const urlData = urlStore.get(shortCode)

    if (urlData) {
      urlData.click_count += 1
      urlStore.set(shortCode, urlData)
      return HttpResponse.json({ success: true, click_count: urlData.click_count })
    }

    return HttpResponse.json(
      { detail: 'URL not found' },
      { status: 404 }
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
