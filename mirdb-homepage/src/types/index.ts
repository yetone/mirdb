/**
 * Shared type definitions for the MirDB Homepage.
 */

export interface Feature {
  title: string;
  description: string;
  icon: string;
  status: 'completed' | 'planned';
}

export type ThemeMode = 'light' | 'dark';

export interface NavigationLink {
  label: string;
  href: string;
  isExternal?: boolean;
}

export interface ProjectStatusItem {
  title: string;
  completed: boolean;
}
