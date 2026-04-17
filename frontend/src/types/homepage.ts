/**
 * Homepage TypeScript Type Definitions
 *
 * Shared type definitions for homepage components.
 */

/**
 * API response for URL shortening
 */
export interface ShortenedUrlResponse {
  shortCode: string
  shortUrl: string
  originalUrl: string
  createdAt: string
}

/**
 * Feature card data structure
 */
export interface Feature {
  id?: string
  icon: React.ReactNode
  title: string
  description: string
}

/**
 * Feature card props
 */
export interface FeatureCardProps {
  icon: React.ReactNode
  title: string
  description: string
}

/**
 * How it works step data
 */
export interface Step {
  stepNumber: number
  icon: React.ReactNode
  title: string
  description: string
}

/**
 * Social proof testimonial
 */
export interface Testimonial {
  id: string
  author: string
  name?: string
  role: string
  content: string
  avatar?: string
}

/**
 * Navigation link data
 */
export interface FooterLink {
  label: string
  href: string
  external?: boolean
}

/**
 * Footer section structure
 */
export interface FooterSection {
  title: string
  links: FooterLink[]
}

/**
 * Hero section props
 */
export interface HeroSectionProps {
  className?: string
  onShortenUrl?: (url: string) => Promise<ShortenedUrlResponse>
}
