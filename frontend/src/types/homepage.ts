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
  onShortenSuccess?: (result: ShortenedUrlResult) => void
  onRegisterClick?: () => void
}

/**
 * URL validation result
 */
export interface UrlValidationResult {
  isValid: boolean
  error?: string
}

/**
 * Hook return type for anonymous URL shortening
 */
export interface UseAnonymousShortenReturn {
  shortenUrl: (url: string) => Promise<ShortenedUrlResult | null>
  isLoading: boolean
  error: string | null
  result: ShortenedUrlResult | null
  clearError: () => void
  clearResult: () => void
}
