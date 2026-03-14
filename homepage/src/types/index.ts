/**
 * Shared type definitions for the Product Homepage.
 */

export interface Feature {
  id: string
  icon: string
  title: string
  description: string
}

export interface Testimonial {
  id: string
  quote: string
  author: string
  role: string
  company: string
  avatar?: string
}

export interface NavLink {
  label: string
  href: string
  isExternal?: boolean
}

export interface SocialLink {
  platform: string
  href: string
  icon: string
}

export interface CTAButton {
  label: string
  href: string
  variant: 'primary' | 'secondary' | 'outline'
  size: 'sm' | 'md' | 'lg'
}
