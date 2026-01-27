/**
 * Homepage-specific type definitions.
 *
 * This file is created by the first scenario builder and
 * should contain types used across homepage components.
 */

export interface Feature {
  icon: React.ReactNode
  title: string
  description: string
}

export interface Step {
  number: number
  title: string
  description: string
  icon?: React.ReactNode
}

export interface SocialProof {
  label: string
  value: string | number
}

export interface FooterLink {
  label: string
  href: string
}
