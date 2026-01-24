/**
 * Type definitions for Product Landing Page components.
 *
 * This file is created by the first scenario builder and
 * should contain types used across landing page modules.
 */

export interface Feature {
  icon: string
  title: string
  description: string
}

export interface WorkflowStep {
  number: number
  title: string
  description: string
}

export interface Statistic {
  value: string | number
  label: string
}

export interface FooterLink {
  label: string
  href: string
}

export interface CTAButton {
  text: string
  href: string
}

export interface HeroSectionProps {
  headline?: string
  subheadline?: string
  primaryCTA?: CTAButton
  secondaryCTA?: CTAButton
}

export interface FeaturesSectionProps {
  features?: Feature[]
}

export interface HowItWorksSectionProps {
  steps?: WorkflowStep[]
}

export interface SocialProofSectionProps {
  statistics?: Statistic[]
  showTestimonials?: boolean
}

export interface CTASectionProps {
  message?: string
  ctaText?: string
  ctaHref?: string
}

export interface FooterProps {
  links?: FooterLink[]
  showSocialLinks?: boolean
}
