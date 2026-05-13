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

export type Theme = 'light' | 'dark';
