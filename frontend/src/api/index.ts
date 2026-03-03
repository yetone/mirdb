/**
 * API Client
 * Centralized axios instance with interceptors for authentication.
 * This is an existing module used by multiple scenarios.
 */
import axios, { AxiosError, InternalAxiosRequestConfig } from 'axios'

const API_BASE_URL = import.meta.env.VITE_API_URL || ''

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
})

// Request interceptor: Add auth token
api.interceptors.request.use(
  (config: InternalAxiosRequestConfig) => {
    const token = localStorage.getItem('token')
    if (token && config.headers) {
      config.headers.Authorization = `Bearer ${token}`
    }
    return config
  },
  (error: AxiosError) => Promise.reject(error)
)

// Response interceptor: Handle auth errors
api.interceptors.response.use(
  (response) => response,
  (error: AxiosError) => {
    if (error.response?.status === 401) {
      localStorage.removeItem('token')
      window.location.href = '/login'
    }
    return Promise.reject(error)
  }
)

/**
 * URL Shortening API
 */
export interface ShortenUrlRequest {
  original_url: string
}

export interface ShortenUrlResponse {
  id: number
  original_url: string
  short_code: string
  created_at: string
  user_id: number | null
  click_count: number
}

/**
 * Shorten a URL (works for both authenticated and guest users)
 */
export const shortenUrl = async (originalUrl: string): Promise<ShortenUrlResponse> => {
  const response = await api.post<ShortenUrlResponse>('/api/urls/', {
    original_url: originalUrl,
  })
  return response.data
}

/**
 * Get URL stats
 */
export const getUrlStats = async (shortCode: string, shareToken?: string) => {
  const params = shareToken ? { share_token: shareToken } : {}
  const response = await api.get(`/api/urls/${shortCode}/stats`, { params })
  return response.data
}

export default api
