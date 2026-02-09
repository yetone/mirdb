/**
 * Shared type definitions for the MirDB Homepage.
 *
 * This file is created by the first scenario builder and
 * should contain types used across multiple components.
 */

export interface Feature {
  icon: string;
  title: string;
  description: string;
}

export interface Command {
  name: string;
  syntax: string;
  description: string;
  example?: string;
}

export interface ConfigOption {
  name: string;
  default: string;
  description: string;
}

export type Theme = 'light' | 'dark';

export interface CodeBlock {
  code: string;
  language: string;
  label?: string;
}
