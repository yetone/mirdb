/**
 * Shared type definitions for the MirDB homepage.
 *
 * This file is created by the first scenario builder and
 * should contain types used across multiple modules.
 *
 * Expected exports:
 * - Feature: { id: string; name: string; description: string; icon?: string; technicalDetails?: string[] }
 * - NavItem: { id: string; label: string; href: string; }
 * - FAQItem: { id: string; question: string; answer: string; }
 * - CodeSnippet: { id: string; language: string; code: string; label?: string; }
 * - InstallMethod: { id: string; name: string; commands: string[]; }
 * - SEOProps: { title: string; description: string; ogImage?: string; ogType?: string; canonical?: string; }
 */

export interface Feature {
  id: string;
  name: string;
  description: string;
  icon?: string;
  technicalDetails?: string[];
}

export interface NavItem {
  id: string;
  label: string;
  href: string;
}

export interface FAQItem {
  id: string;
  question: string;
  answer: string;
}

export interface CodeSnippet {
  id: string;
  language: string;
  code: string;
  label?: string;
}

export interface InstallMethod {
  id: string;
  name: string;
  commands: string[];
}

export interface SEOProps {
  title: string;
  description: string;
  ogImage?: string;
  ogType?: string;
  canonical?: string;
}
