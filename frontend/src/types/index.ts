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
