/**
 * Homepage Type Definitions
 *
 * Type definitions for homepage components and data structures.
 */

export interface HeroSectionProps {
  headline?: string
  subheading?: string
  ctaText?: string
  ctaLink?: string
}

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

export interface HomePageProps {
  showFeatures?: boolean
  showFAQ?: boolean
}
