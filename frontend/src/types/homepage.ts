/**
 * Type definitions for homepage components.
 *
 * This file is created by the first scenario builder and
 * should contain types used across homepage modules.
 */

/**
 * Result from URL shortening operation
 */
export interface ShortenedUrlResult {
  shortUrl: string
  originalUrl: string
  shortCode: string
  createdAt: string
}

/**
 * Feature card data structure
 */
export interface FeatureItem {
  icon: React.ReactNode
  title: string
  description: string
}

/**
 * Hero section component props
 */
export interface HeroProps {
  onPrimaryClick?: () => void
  onSecondaryClick?: () => void
}

/**
 * Inline shortener component props
 */
export interface InlineShortenerProps {
  onSuccess?: (result: ShortenedUrlResult) => void
  onError?: (error: string) => void
}

/**
 * URL validation result
 */
export interface UrlValidationResult {
  isValid: boolean
  error?: string
}

/**
 * Hook return type for useAnonymousShorten
 */
export interface UseAnonymousShortenReturn {
  shortenUrl: (url: string) => Promise<ShortenedUrlResult>
  isLoading: boolean
  error: string | null
  result: ShortenedUrlResult | null
  reset: () => void
}
