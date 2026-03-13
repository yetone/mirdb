/**
 * Shared type definitions for the MirDB homepage.
 *
 * This file is created by the first scenario builder and
 * should contain types used across multiple components.
 */

export interface Feature {
  id: string;
  title: string;
  description: string;
  icon?: string;
}

export interface NavLink {
  label: string;
  href: string;
  external?: boolean;
}

export type Theme = 'light' | 'dark';

export interface ThemeContextValue {
  theme: Theme;
  toggleTheme: () => void;
}
