/**
 * Homepage Type Definitions
 * Owner: First builder to run
 *
 * TypeScript types specific to homepage components.
 */

export interface Feature {
  id: string
  title: string
  description: string
  icon: React.ReactNode
}

export interface Step {
  number: number
  title: string
  description: string
  icon: React.ReactNode
}

export interface ShortenedUrl {
  shortCode: string
  originalUrl: string
  shortUrl: string
}

export interface FormState {
  url: string
  error: string | null
  isLoading: boolean
  result: ShortenedUrl | null
}
