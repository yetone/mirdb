import { http, HttpResponse, delay } from 'msw';

const API_BASE_URL = 'http://localhost:8000/api';

export const handlers = [
  // Anonymous URL shortening endpoint
  http.post(`${API_BASE_URL}/urls/anonymous`, async ({ request }) => {
    const body = await request.json() as { url: string };

    if (!body.url) {
      return HttpResponse.json(
        { detail: 'URL is required' },
        { status: 400 }
      );
    }

    // Simulate successful response
    return HttpResponse.json({
      short_url: 'http://localhost:8000/abc123',
      short_code: 'abc123',
      original_url: body.url,
    });
  }),
];

// Error handlers for testing error scenarios
export const errorHandlers = {
  serverError: http.post(`${API_BASE_URL}/urls/anonymous`, () => {
    return HttpResponse.json(
      { detail: 'Internal server error' },
      { status: 500 }
    );
  }),

  networkTimeout: http.post(`${API_BASE_URL}/urls/anonymous`, async () => {
    // Simulate a very long delay that would trigger a timeout
    await delay(30000);
    return HttpResponse.json({ short_url: 'test' });
  }),

  networkError: http.post(`${API_BASE_URL}/urls/anonymous`, () => {
    return HttpResponse.error();
  }),
};
