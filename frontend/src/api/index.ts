/**
 * API Client
 * Centralized API client for URL shortening service
 */
import axios from 'axios'

export const api = axios.create({
  baseURL: '',
  headers: {
    'Content-Type': 'application/json',
  },
})

api.interceptors.request.use((config) => {
  const token = localStorage.getItem('token')
  if (token) {
    config.headers.Authorization = `Bearer ${token}`
  }
  return config
})

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

export interface ShortenUrlRequest {
  original_url: string
}

export interface ShortenedUrlResponse {
  id: number
  original_url: string
  short_code: string
  created_at: string
  user_id: number | null
  click_count: number
}

export const shortenUrl = async (originalUrl: string): Promise<ShortenedUrlResponse> => {
  const response = await api.post<ShortenedUrlResponse>('/api/urls/', {
    original_url: originalUrl,
  })
  return response.data
}
