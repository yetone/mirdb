/**
 * Shared type definitions for the MirDB Homepage.
 *
 * This file is created by the first scenario builder and
 * should contain types used across multiple components.
 */

export type FeatureStatus = 'implemented' | 'planned';

export type Theme = 'light' | 'dark';

export type Breakpoint = 'mobile' | 'tablet' | 'desktop';

export interface Feature {
  id: string;
  name: string;
  title?: string;
  description: string;
  status: FeatureStatus;
  icon?: string;
}

export interface NavItem {
  id: string;
  label: string;
  href: string;
}

export interface RoadmapItem {
  id: string;
  title: string;
  description: string;
  status: 'completed' | 'in-progress' | 'planned';
  quarter?: string;
}

export interface CodeBlockProps {
  code: string;
  language?: 'bash' | 'rust' | 'json' | 'text';
  showCopyButton?: boolean;
  title?: string;
}
