import { ReactNode } from 'react'

/**
 * Homepage-specific type definitions.
 *
 * This file is created by the first scenario builder and
 * should contain types used by homepage components.
 */

export interface Feature {
  icon: ReactNode
  title: string
  description: string
}

export interface Step {
  number: number
  title: string
  description: string
  icon: ReactNode
}

export interface NavLink {
  label: string
  href: string
  isAnchor?: boolean
}
