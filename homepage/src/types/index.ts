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
  id: string | number;
  label?: string;
  title?: string;
  command: string;
  description?: string;
}

export interface CodeBlockProps {
  code: string;
  language?: string;
  showCopyButton?: boolean;
}
