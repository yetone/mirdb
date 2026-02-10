/**
 * Shared type definitions for the MirDB Homepage.
 *
 * This file is created by the first scenario builder and
 * should contain types used across multiple modules.
 */

/**
 * Interface for feature card data
 */
export interface Feature {
  id: string;
  title: string;
  description: string;
  icon: React.ReactNode;
}

/**
 * Theme type for dark/light mode
 */
export type Theme = 'light' | 'dark';

/**
 * Interface for code block/snippet data
 */
export interface CodeSnippet {
  language: string;
  code: string;
  label?: string;
}

/**
 * Navigation link interface
 */
export interface NavLink {
  label: string;
  href: string;
  isExternal?: boolean;
}
