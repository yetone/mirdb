/**
 * Shared type definitions for the MirDB Homepage.
 */

export type Theme = 'light' | 'dark';

export interface Feature {
  icon: string;
  title: string;
  description: string;
}

export interface RoadmapItem {
  name: string;
  completed: boolean;
  description?: string;
}

export interface NavLink {
  label: string;
  href: string;
}

export interface ThemeContextValue {
  theme: Theme;
  toggleTheme: () => void;
}
