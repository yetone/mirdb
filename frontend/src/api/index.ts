/**
 * API Client & Endpoints
 *
 * Centralized axios instance with interceptors for authentication.
 * Includes endpoints for URL shortening, authentication, and settings.
 */

import axios, { AxiosError, AxiosResponse } from 'axios'
import { CreateUrlResponse, ShortenResult } from '@/types/home'

// Create axios instance
const api = axios.create({
  baseURL: '',
  headers: {
    'Content-Type': 'application/json',
  },
})

// Request interceptor - add JWT token if available
api.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem('token')
    if (token) {
      config.headers.Authorization = `Bearer ${token}`
    }
    return config
  },
  (error) => Promise.reject(error)
)

// Response interceptor - handle 401 errors
api.interceptors.response.use(
  (response) => response,
  (error: AxiosError) => {
    if (error.response?.status === 401) {
      localStorage.removeItem('token')
      // Only redirect if not already on login page
      if (window.location.pathname !== '/login') {
        // Don't redirect for anonymous URL creation
        const isAnonymousRequest = !localStorage.getItem('token')
        if (!isAnonymousRequest) {
          window.location.href = '/login'
        }
      }
    }
    return Promise.reject(error)
  }
)

/**
 * Create a shortened URL (works for both authenticated and anonymous users)
 *
 * For anonymous users, the URL is created without a user_id and includes
 * a share_token for accessing analytics.
 */
export async function createShortUrl(originalUrl: string): Promise<ShortenResult> {
  const response: AxiosResponse<CreateUrlResponse> = await api.post('/api/urls/', {
    original_url: originalUrl,
  })

  const data = response.data
  const baseUrl = window.location.origin

  return {
    id: data.id,
    shortUrl: `${baseUrl}/r/${data.short_code}`,
    shortCode: data.short_code,
    shareToken: data.share_token,
    originalUrl: data.original_url,
    clickCount: data.click_count,
  }
}

/**
 * Create anonymous short URL - explicitly without authentication
 * This creates a URL that is not associated with any user account
 */
export async function createAnonymousShortUrl(originalUrl: string): Promise<ShortenResult> {
  // Create a new request without the auth interceptor
  const response: AxiosResponse<CreateUrlResponse> = await axios.post(
    '/api/urls/anonymous',
    { original_url: originalUrl },
    { headers: { 'Content-Type': 'application/json' } }
  )

  const data = response.data
  const baseUrl = window.location.origin

  return {
    id: data.id,
    shortUrl: `${baseUrl}/r/${data.short_code}`,
    shortCode: data.short_code,
    shareToken: data.share_token,
    originalUrl: data.original_url,
    clickCount: data.click_count,
  }
}

/**
 * Get URL statistics (requires authentication or share token)
 */
export async function getUrlStats(shortCode: string, shareToken?: string) {
  const params = shareToken ? { share_token: shareToken } : {}
  const response = await api.get(`/api/urls/${shortCode}/stats`, { params })
  return response.data
}

/**
 * Check if registration is enabled
 */
export async function getSettings() {
  const response = await api.get('/api/settings/')
  return response.data
}

export default api
