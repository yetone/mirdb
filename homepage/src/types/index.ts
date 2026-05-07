/**
 * Shared TypeScript type definitions for the MirDB homepage.
 */

export interface Feature {
  title: string;
  description: string;
  icon: string;
}

export interface ConfigOption {
  name: string;
  default: string;
  description: string;
}

export interface ProtocolCommand {
  name: string;
  syntax: string;
  description: string;
  example: string;
}

export interface RoadmapItem {
  title: string;
  status: 'implemented' | 'planned';
}

export type Theme = 'light' | 'dark';

export interface QuickStartStep {
  title: string;
  description: string;
  code: string;
  language: string;
}
