/**
 * Shared type definitions for the MirDB Homepage.
 */

export interface Feature {
  id: string;
  title: string;
  description: string;
  icon?: string;
}

export interface NavigationLink {
  label: string;
  href: string;
  isExternal?: boolean;
}

export interface InstallationStep {
  id: number;
  label: string;
  command: string;
}
