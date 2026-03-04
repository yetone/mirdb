/**
 * Homepage Type Definitions
 *
 * Type definitions for homepage components and data structures.
 */

export interface Feature {
  id: number
  title: string
  description: string
  icon: string
}

export interface FAQItem {
  id: number
  question: string
  answer: string
}

export interface HeroSectionProps {
  headline?: string
  subheading?: string
  ctaText?: string
  ctaLink?: string
}

export interface FeaturesSectionProps {
  features?: Feature[]
}

export interface FAQSectionProps {
  items?: FAQItem[]
}

export interface CTAFooterProps {
  ctaText?: string
  ctaLink?: string
  showLogin?: boolean
}
