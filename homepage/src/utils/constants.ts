/**
 * Application constants for the Product Homepage.
 */

import type { NavLink, SocialLink } from '../types'

export const BREAKPOINTS = {
  mobile: 320,
  tablet: 768,
  desktop: 1024,
  wide: 1440,
} as const

export const COLORS = {
  primary: '#3b82f6',
  primaryDark: '#2563eb',
  secondary: '#64748b',
  accent: '#f59e0b',
  background: '#ffffff',
  text: '#0f172a',
} as const

export const ANIMATION_DURATION = 200

export const NAV_LINKS: NavLink[] = [
  { label: 'Features', href: '#features' },
  { label: 'Pricing', href: '#pricing' },
  { label: 'About', href: '#about' },
  { label: 'Contact', href: '#contact' },
]

export const SOCIAL_LINKS: SocialLink[] = [
  { platform: 'twitter', href: 'https://twitter.com', icon: 'twitter' },
  { platform: 'linkedin', href: 'https://linkedin.com', icon: 'linkedin' },
  { platform: 'github', href: 'https://github.com', icon: 'github' },
]
