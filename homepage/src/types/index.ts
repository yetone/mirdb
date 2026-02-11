/**
 * Shared type definitions for the MirDB homepage.
 */

export type Theme = 'light' | 'dark'

export interface Feature {
  name: string
  description: string
  icon?: string
}

export interface NavItem {
  label: string
  href: string
  external?: boolean
}

export interface CodeExample {
  language: string
  code: string
  title?: string
}

export interface HeroContent {
  title: string
  subtitle: string
  description: string
}

export interface ButtonProps {
  variant?: 'primary' | 'secondary' | 'ghost'
  size?: 'sm' | 'md' | 'lg'
  href?: string
  external?: boolean
  onClick?: () => void
  children: React.ReactNode
  className?: string
}
