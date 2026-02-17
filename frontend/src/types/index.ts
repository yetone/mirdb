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
  short_code: string
  original_url: string
  click_count: number
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

/** Authentication response */
export interface AuthResponse {
  access_token: string
  token_type: string
}

/** Login credentials */
export interface LoginCredentials {
  email: string
  password: string
}

/** Registration data */
export interface RegisterData {
  email: string
  password: string
}
