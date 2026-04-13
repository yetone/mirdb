/**
 * Shared type definitions for MirDB Homepage.
 */

export interface Feature {
  title: string;
  description: string;
  icon?: string;
}

export interface RoadmapItem {
  title: string;
  status: 'completed' | 'planned';
  description?: string;
}

export interface ExternalLinkProps {
  href: string;
  children: React.ReactNode;
  className?: string;
}

export interface CodeBlockProps {
  code: string;
  language?: string;
  title?: string;
}

export interface StatusBadgeProps {
  src: string;
  alt: string;
  href: string;
}
