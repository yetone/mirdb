/**
 * API client & endpoints
 *
 * Centralized axios instance with interceptors:
 * - Request interceptor: Adds JWT token from localStorage
 * - Response interceptor: Handles 401 errors, redirects to login
 */
import axios, { AxiosInstance } from 'axios';
import { ShortenUrlResponse } from '../types/homepage';

const API_BASE_URL = import.meta.env.VITE_API_URL || '';

export const api: AxiosInstance = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Request interceptor to add JWT token
api.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem('token');
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  },
  (error) => Promise.reject(error)
);

// Response interceptor for error handling
api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      localStorage.removeItem('token');
      // Only redirect if not already on login page
      if (window.location.pathname !== '/login') {
        window.location.href = '/login';
      }
    }
    return Promise.reject(error);
  }
);

/**
 * Shorten a URL anonymously (no authentication required)
 */
export async function shortenUrlAnonymous(originalUrl: string): Promise<ShortenUrlResponse> {
  const response = await api.post<ShortenUrlResponse>('/api/urls/shorten', {
    original_url: originalUrl,
  });
  return response.data;
}

/**
 * Get the full shortened URL from a short code
 */
export function getFullShortUrl(shortCode: string): string {
  const baseUrl = window.location.origin;
  return `${baseUrl}/r/${shortCode}`;
}

export default api;
