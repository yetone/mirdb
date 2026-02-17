/**
 * API client configuration.
 * Owner: First builder (shared)
 */

import axios from 'axios'

const api = axios.create({
  baseURL: '/api',
  headers: {
    'Content-Type': 'application/json',
  },
})

// Request interceptor to add JWT token
api.interceptors.request.use((config) => {
  const token = localStorage.getItem('token')
  if (token) {
    config.headers.Authorization = `Bearer ${token}`
  }
  return config
})

// Response interceptor to handle errors
api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      localStorage.removeItem('token')
      window.location.href = '/login'
    }
    return Promise.reject(error)
  }
)

export default api

import { ApiRequestError } from '../types'

/** Shorten a URL (public endpoint) */
export async function shortenUrl(originalUrl: string): Promise<{ short_url: string; short_code: string }> {
  try {
    const response = await api.post('/shorten', { url: originalUrl })
    return response.data
  } catch (error: unknown) {
    // Check if it's an axios error
    if (error && typeof error === 'object' && 'isAxiosError' in error) {
      const axiosError = error as { response?: { status?: number; data?: { detail?: string } }; message?: string }
      const statusCode = axiosError.response?.status
      const message = axiosError.response?.data?.detail || axiosError.message || 'Unknown error'

      // Network error (no response)
      if (!axiosError.response) {
        throw new ApiRequestError('Network error occurred', undefined, true)
      }

      throw new ApiRequestError(message, statusCode, false)
    }

    // Unknown error type
    throw new ApiRequestError('An unexpected error occurred', undefined, false)
  }
}

/** Get all URLs for the current user */
export async function getUserUrls() {
  const response = await api.get('/urls')
  return response.data
}

/** Get URL stats */
export async function getUrlStats(shortCode: string) {
  const response = await api.get(`/urls/${shortCode}/stats`)
  return response.data
}
