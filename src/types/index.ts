/**
 * Shared type definitions for the MirDB homepage.
 */

export interface MirDBFeature {
  id: string;
  title: string;
  description: string;
  icon: string;
}

export interface ComparisonRow {
  dimension: string;
  mirdb: string | boolean;
  memcached: string | boolean;
  redis: string | boolean;
}

export interface RoadmapItem {
  id: string;
  title: string;
  status: 'completed' | 'planned' | 'in-progress';
  description: string;
}

export interface CodeExample {
  id: string;
  title: string;
  language: string;
  code: string;
}

export interface DocLink {
  id: string;
  title: string;
  description: string;
  url: string;
  icon: string;
}

export type Theme = 'light' | 'dark';

export interface DemoCaption {
  id: string;
  text: string;
}
