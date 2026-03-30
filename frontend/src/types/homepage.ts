/**
 * TypeScript types for homepage components.
 *
 * This file contains type definitions used across
 * homepage components and hooks.
 */

export interface HeroSectionProps {
  onUrlSubmit?: (url: string) => void
  isAuthenticated?: boolean
}

export interface UrlInputProps {
  onSubmit: (url: string) => void
  isLoading?: boolean
  error?: string | null
}

export interface FeatureCardProps {
  icon: React.ReactNode
  title: string
  description: string
}

export interface Feature {
  id: string
  icon: React.ReactNode
  title: string
  description: string
}

export interface UrlShortenerState {
  shortenUrl: (url: string) => Promise<void>
  isLoading: boolean
  error: string | null
}
