/**
 * Shared type definitions for the MirDB Homepage.
 */

export type Theme = 'light' | 'dark'

export interface Feature {
  id: string
  title: string
  description: string
  icon: string
}

export interface Command {
  name: string
  syntax: string
  description: string
}

export interface NavItem {
  label: string
  href: string
  external?: boolean
}

export interface ButtonProps {
  variant?: 'primary' | 'secondary'
  children: React.ReactNode
  href?: string
  onClick?: () => void
  target?: string
  rel?: string
  className?: string
  'aria-label'?: string
}
