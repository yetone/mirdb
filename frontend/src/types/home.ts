/**
 * Homepage Type Definitions
 * Owner: First builder (shared resource)
 *
 * Type definitions specific to homepage functionality.
 */

export interface ShortenResult {
  shortUrl: string
  shortCode: string
  shareToken: string
}

export interface ValidationResult {
  valid: boolean
  error?: string
}

export interface Feature {
  icon: string
  title: string
  description: string
}

export interface HeroSectionProps {
  onUrlShortened?: (shortUrl: string, shareToken: string) => void
}

export interface UrlShortenerFormProps {
  onSuccess?: (result: ShortenResult) => void
  onError?: (error: Error) => void
}

export interface CopyButtonProps {
  text: string
  className?: string
  onCopy?: () => void
  onError?: (error: Error) => void
}
