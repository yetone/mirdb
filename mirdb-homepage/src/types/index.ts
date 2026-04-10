/**
 * Shared type definitions for MirDB Homepage.
 */

export interface Feature {
  id: string;
  title: string;
  description: string;
  icon: string;
}

export interface NavigationLink {
  label: string;
  href: string;
  external?: boolean;
}

export interface CodeExample {
  language: string;
  code: string;
  title?: string;
}

export interface PerformanceMetric {
  label: string;
  value: number;
  unit: string;
  comparison?: {
    name: string;
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
