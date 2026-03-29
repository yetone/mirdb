/**
 * Homepage Type Definitions
 * Owner: First scenario builder
 *
 * Shared types for homepage components
 */

export interface Feature {
  icon: React.ReactNode
  title: string
  description: string
  link?: string
}

export interface NavLink {
  label: string
  href: string
}

export interface FooterSection {
  title: string
  links: NavLink[]
}

export interface Testimonial {
  quote: string
  author: string
  role?: string
}

export interface Statistic {
  value: string
  label: string
}
