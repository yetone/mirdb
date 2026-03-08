/**
 * Shared type definitions for the MirDB Homepage.
 *
 * This file is created by the first scenario builder and
 * should contain types used across multiple components.
 */

export interface Feature {
  title: string
  description: string
  icon: string
}

export interface Step {
  number: number
  title: string
  content: string
  code?: string
}

export interface RoadmapItem {
  title: string
  status: 'complete' | 'in-progress' | 'planned'
  description: string
}

export interface ExternalLink {
  label: string
  url: string
  type: 'github' | 'docs' | 'external' | 'protocol' | 'other'
}

export interface StatusBadge {
  name: string
  imageUrl: string
  linkUrl: string
  altText: string
}
