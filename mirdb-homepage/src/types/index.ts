/**
 * Shared type definitions for MirDB Homepage.
 *
 * This file is created by the first scenario builder and
 * should contain types used across multiple components.
 */

// Feature card data structure
export interface Feature {
  id: string;
  title: string;
  description: string;
  icon: string;
}

// Command table entry
export interface Command {
  name: string;
  description: string;
  syntax: string;
}

// Roadmap item
export interface RoadmapItem {
  title: string;
  description: string;
  status: 'completed' | 'in-progress' | 'planned';
}

// Configuration option
export interface ConfigOption {
  name: string;
  type: string;
  default: string;
  description: string;
}

// Terminal tab content
export interface TerminalTab {
  id: string;
  label: string;
  code: string;
  language: string;
}
