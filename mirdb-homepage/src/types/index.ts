/**
 * Shared type definitions for MirDB Homepage.
 */

export interface Feature {
  id: string;
  title: string;
  description: string;
  icon?: string;
}

export interface NavigationLink {
  id?: string;
  label: string;
  href: string;
  external?: boolean;
  description?: string;
}

export interface CodeExample {
  id?: string;
  title?: string;
  language: string;
  code: string;
}

export interface PerformanceMetric {
  id?: string;
  label: string;
  value: number;
  unit: string;
  comparison?: {
    label?: string;
    name?: string;
    value: number;
  };
}

export type Theme = 'light' | 'dark';

export interface ButtonProps {
  children: React.ReactNode;
  href?: string;
  onClick?: () => void;
  variant?: 'primary' | 'secondary' | 'outline';
  size?: 'sm' | 'md' | 'lg';
  className?: string;
}

/**
 * Badge component types
 * Added by Scenario 8 - Rust Crate Badge Display
 */
export interface BadgeProps {
  label: string;
  value: string;
  href?: string;
  variant?: 'default' | 'crates' | 'success' | 'info';
  className?: string;
  testId?: string;
}

export interface CratesBadgeProps {
  version: string;
  crateName?: string;
  className?: string;
}
