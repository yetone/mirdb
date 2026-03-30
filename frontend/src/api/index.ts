/**
 * API client for URL shortening service
 */

import axios from 'axios'

const api = axios.create({
  baseURL: '/api',
  headers: {
    'Content-Type': 'application/json',
  },
})

// Request interceptor to add auth token
api.interceptors.request.use((config) => {
  const token = localStorage.getItem('token')
  if (token) {
    config.headers.Authorization = `Bearer ${token}`
  }
  return config
})

// Response interceptor for error handling
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

export interface ShortenUrlRequest {
  url: string
}

export interface ShortenUrlResponse {
  short_code: string
  short_url: string
  original_url: string
}

export const shortenUrl = async (url: string): Promise<ShortenUrlResponse> => {
  const response = await api.post<ShortenUrlResponse>('/urls', { url })
  return response.data
}
