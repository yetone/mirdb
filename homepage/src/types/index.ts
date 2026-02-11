/**
 * Shared type definitions for the MirDB homepage.
 *
 * This file is created by the first scenario builder and
 * should contain types used across multiple components.
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
