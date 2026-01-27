/**
 * Shared type definitions for homepage components.
 *
 * This file is created by the first scenario builder and
 * should contain types used across multiple homepage modules.
 */

export interface Feature {
  icon: string
  title: string
  description: string
}

export interface Step {
  number: number
  title: string
  description: string
}

export interface StatItem {
  label: string
  value: number
  suffix?: string
}

export interface FooterLink {
  label: string
  href: string
  external?: boolean
}

export interface HeroSectionProps {
  onLearnMoreClick?: () => void
}
