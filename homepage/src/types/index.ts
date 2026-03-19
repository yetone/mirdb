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

export interface ProjectFeature {
  name: string;
  implemented: boolean;
}

export interface Resource {
  title: string;
  description: string;
  href: string;
  external: boolean;
}

export type Theme = 'light' | 'dark' | 'system';

export interface ComparisonRow {
  feature: string;
  mirdb: string;
  memcached: string;
}
