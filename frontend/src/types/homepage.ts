/**
 * Homepage-specific type definitions.
 *
 * This file is created by the first scenario builder and
 * should contain types used across homepage components.
 */

export interface Feature {
  icon: React.ReactNode | string
  title: string
  description: string
}

export interface Step {
  number: number
  title: string
  description: string
}

export interface Statistic {
  label: string
  value: string | number
  icon?: React.ReactNode | string
}

export interface FooterLink {
  label: string
  href: string
}

export interface HeroProps {
  headline?: string
  subheading?: string
  primaryCtaText?: string
  primaryCtaLink?: string
  secondaryCtaText?: string
  secondaryCtaLink?: string
}
