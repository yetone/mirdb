/**
 * Homepage Type Definitions
 * Owner: First builder (shared resource)
 *
 * Type definitions specific to homepage functionality.
 */

/**
 * Result from URL shortening API
 */
export interface ShortenResult {
  /** Full short URL (e.g., http://localhost/r/abc123) */
  shortUrl: string
  /** The short code (e.g., abc123) */
  shortCode: string
  /** Token for accessing analytics without authentication */
  shareToken: string
  /** Original URL that was shortened */
  originalUrl: string
  /** ID of the created URL record */
  id: number
  /** Click count (initially 0) */
  clickCount: number
}

/**
 * URL validation result
 */
export interface ValidationResult {
  /** Whether the URL is valid */
  valid: boolean
  /** Error message if validation failed */
  error?: string
}

/**
 * Feature card data for Features section
 */
export interface Feature {
  /** Icon identifier or emoji */
  icon: string
  /** Feature title */
  title: string
  /** Feature description */
  description: string
}

/**
 * Props for HeroSection component
 */
export interface HeroSectionProps {
  onUrlShortened?: (shortUrl: string, shareToken: string) => void
}

/**
 * Props for UrlShortenerForm component
 */
export interface UrlShortenerFormProps {
  /** Callback when URL is successfully shortened */
  onSuccess?: (result: ShortenResult) => void
  /** Callback when an error occurs */
  onError?: (error: Error) => void
}

/**
 * Props for CopyButton component
 */
export interface CopyButtonProps {
  text: string
  className?: string
  onCopy?: () => void
  onError?: (error: Error) => void
}

/**
 * API response for URL creation
 */
export interface CreateUrlResponse {
  id: number
  original_url: string
  short_code: string
  created_at: string
  user_id: number | null
  click_count: number
  share_token: string
}

/**
 * API error response shape
 */
export interface ApiErrorResponse {
  detail: string
}
