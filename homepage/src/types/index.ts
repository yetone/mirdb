/**
 * Shared type definitions for MirDB Homepage.
 */

export type Theme = 'light' | 'dark';

export interface FeatureData {
  icon: string;
  title: string;
  description: string;
}

export interface ComparisonItem {
  product: string;
  features: Record<string, boolean | string>;
}

export interface CodeExample {
  language: string;
  code: string;
  title: string;
}

export interface NavItem {
  label: string;
  href: string;
  external?: boolean;
}

export interface ButtonProps {
  children: React.ReactNode;
  variant?: 'primary' | 'secondary' | 'outline';
  size?: 'sm' | 'md' | 'lg';
  href?: string;
  onClick?: () => void;
  external?: boolean;
  className?: string;
  'data-testid'?: string;
}

export interface ContainerProps {
  children: React.ReactNode;
  className?: string;
}

export interface HeroProps {
  title?: string;
  tagline?: string;
  ctaText?: string;
  ctaHref?: string;
}
