/**
 * Shared type definitions for MirDB Homepage.
 */

export type Theme = 'light' | 'dark'

export type ButtonVariant = 'primary' | 'secondary' | 'ghost'

export interface ExternalLink {
  href: string
  label: string
  icon?: string
}
