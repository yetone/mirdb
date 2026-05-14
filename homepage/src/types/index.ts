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
 * - UsageExample: { id: string; operation: string; description: string; language: string; code: string; }
 * - QuickStartData: { sectionTitle: string; sectionDescription: string; installMethods: InstallMethod[]; usageExamples: UsageExample[]; }
 * - SEOProps: { title: string; description: string; ogImage?: string; ogType?: string; canonical?: string; }
 * - ArchitectureComponent: { id: string; name: string; description: string; }
 * - ArchitectureLink: { label: string; url: string; }
 * - ArchitectureData: { sectionTitle: string; sectionDescription: string; diagramSrc: string; diagramAlt: string; components: ArchitectureComponent[]; documentationLinks: ArchitectureLink[]; }
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

export interface UsageExample {
  id: string;
  operation: string;
  description: string;
  language: string;
  code: string;
}

export interface QuickStartData {
  sectionTitle: string;
  sectionDescription: string;
  installMethods: InstallMethod[];
  usageExamples: UsageExample[];
}

export interface SEOProps {
  title: string;
  description: string;
  ogImage?: string;
  ogType?: string;
  canonical?: string;
}

export interface ArchitectureComponent {
  id: string;
  name: string;
  description: string;
}

export interface ArchitectureLink {
  label: string;
  url: string;
}

export interface ArchitectureData {
  sectionTitle: string;
  sectionDescription: string;
  diagramSrc: string;
  diagramAlt: string;
  components: ArchitectureComponent[];
  documentationLinks: ArchitectureLink[];
}
