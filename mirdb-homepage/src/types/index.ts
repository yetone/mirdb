/**
 * Shared type definitions for MirDB Homepage.
 *
 * This file is created by the first scenario builder and
 * should contain types used across multiple components.
 */

export interface Feature {
  id: string;
  title: string;
  description: string;
  icon: string;
}

export interface CodeExample {
  command: string;
  description: string;
  response?: string;
}

export interface NavigationItem {
  label: string;
  href: string;
}

export interface Badge {
  type: 'ci' | 'version' | 'license';
  url: string;
  alt: string;
  link: string;
}
