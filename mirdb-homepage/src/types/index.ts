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
  id: string;
  label: string;
  href: string;
  external?: boolean;
  description?: string;
}

export interface CodeExample {
  id: string;
  title: string;
  language: string;
  code: string;
}

export interface PerformanceMetric {
  id: string;
  label: string;
  value: number;
  unit: string;
  comparison?: {
    label: string;
    value: number;
  };
}

export type Theme = 'light' | 'dark';
