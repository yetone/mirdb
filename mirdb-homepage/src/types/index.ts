/**
 * Shared type definitions for MirDB Homepage.
 */

export interface Feature {
  title: string;
  description: string;
  icon: string;
}

export interface QuickStartStep {
  step: number;
  title: string;
  code: string;
}

export interface CodeExample {
  language: string;
  code: string;
  filename?: string;
}

export type Theme = 'light' | 'dark';

export interface NavItem {
  label: string;
  href: string;
}
