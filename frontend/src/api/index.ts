/**
 * API client for the URL Shortener service.
 */

const API_BASE = '/api';

export async function fetchCurrentUser() {
  const response = await fetch(`${API_BASE}/users/me`);
  if (!response.ok) {
    throw new Error('Not authenticated');
  }
  return response.json();
}

export async function shortenUrl(url: string) {
  const response = await fetch(`${API_BASE}/urls`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ url }),
  });
  if (!response.ok) {
    throw new Error('Failed to shorten URL');
  }
  return response.json();
}
