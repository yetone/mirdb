/**
 * API client configuration and endpoints.
 *
 * Centralizes all API calls with JWT authentication handling.
 */

import axios from 'axios'
import type { ShortenedUrl, AuthResponse, LoginCredentials, RegisterData } from '../types'

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

/** Authentication endpoints */
export const authApi = {
  login: async (credentials: LoginCredentials): Promise<AuthResponse> => {
    const formData = new URLSearchParams()
    formData.append('username', credentials.email)
    formData.append('password', credentials.password)
    const { data } = await api.post<AuthResponse>('/token', formData, {
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    })
    return data
  },

  register: async (userData: RegisterData): Promise<{ message: string }> => {
    const { data } = await api.post('/register', userData)
    return data
  },

  getCurrentUser: async () => {
    const { data } = await api.get('/users/me')
    return data
  },
}

/** URL shortening endpoints */
export const urlApi = {
  shorten: async (originalUrl: string): Promise<ShortenedUrl> => {
    const { data } = await api.post<ShortenedUrl>('/shorten', { url: originalUrl })
    return data
  },

  shortenPublic: async (originalUrl: string): Promise<ShortenedUrl> => {
    const { data } = await api.post<ShortenedUrl>('/shorten/public', { url: originalUrl })
    return data
  },

  getMyUrls: async (): Promise<ShortenedUrl[]> => {
    const { data } = await api.get<ShortenedUrl[]>('/urls')
    return data
  },

  getUrlStats: async (shortCode: string) => {
    const { data } = await api.get(`/stats/${shortCode}`)
    return data
  },
}

export default api
