/**
 * Shared type definitions for the MirDB Homepage.
 */

export type Theme = 'light' | 'dark'

export interface Feature {
  title: string
  description: string
  icon: string
}

export interface CodeExample {
  language: string
  code: string
  title?: string
}

export interface NavLink {
  label: string
  href: string
  external?: boolean
}

export interface ButtonProps {
  children: React.ReactNode
  variant?: 'primary' | 'secondary' | 'outline'
  size?: 'sm' | 'md' | 'lg'
  href?: string
  onClick?: () => void
  className?: string
  'aria-label'?: string
}
