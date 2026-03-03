/**
 * Type definitions for homepage components.
 *
 * This file is created by the first scenario builder and
 * should contain types used across homepage modules.
 */

/**
 * Feature displayed in the feature cards section
 */
export interface Feature {
  icon: string
  title: string
  description: string
}

/**
 * Result from URL shortening API
 */
export interface ShortenResult {
  id: number
  original_url: string
  short_code: string
  short_url: string
  created_at: string
  user_id: number | null
  click_count: number
}

/**
 * Error from URL shortening API
 */
export interface ShortenError {
  message: string
  code: string
  detail?: string
}

/**
 * State for the guest shortener component
 */
export type ShortenerState = 'idle' | 'loading' | 'success' | 'error'

/**
 * Props for FeatureCard component
 */
export interface FeatureCardProps {
  icon: string | React.ReactNode
  title: string
  description: string
}
