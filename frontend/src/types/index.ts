/**
 * Shared type definitions for the URL Shortening Service frontend.
 *
 * This file is created by the first scenario builder and
 * should contain types used across multiple modules.
 */

/** User account information */
export interface User {
  id: number
  email: string
  is_admin: boolean
  created_at: string
}

/** URL shortening result */
export interface ShortenedUrl {
  id: number
  original_url: string
  short_code: string
  short_url: string
  clicks: number
  created_at: string
  user_id: number
}

/** Theme configuration type */
export type Theme = 'light' | 'dark' | 'cyberpunk' | 'synthwave' | 'retro' | 'valentine'

/** Standard API error response */
export interface ApiError {
  detail: string
  status_code?: number
}

/** Custom error class for API errors with status codes */
export class ApiRequestError extends Error {
  public readonly statusCode: number | undefined
  public readonly isNetworkError: boolean

  constructor(message: string, statusCode?: number, isNetworkError = false) {
    super(message)
    this.name = 'ApiRequestError'
    this.statusCode = statusCode
    this.isNetworkError = isNetworkError
  }

  /** Check if this is a rate limit error (429) */
  isRateLimitError(): boolean {
    return this.statusCode === 429
  }

  /** Check if this is a server error (5xx) */
  isServerError(): boolean {
    return this.statusCode !== undefined && this.statusCode >= 500 && this.statusCode < 600
  }
}

/** Feature item for homepage display */
export interface Feature {
  id: string
  icon: React.ReactNode
  title: string
  description: string
}

/** API response wrapper */
export interface ApiResponse<T> {
  data: T
  message?: string
}
