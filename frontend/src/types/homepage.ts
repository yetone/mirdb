/**
 * Homepage Type Definitions
 * Owner: First builder (Scenario 1)
 *
 * Shared types for homepage components.
 */

import type { LucideIcon } from 'lucide-react';

export interface Feature {
  icon: LucideIcon;
  title: string;
  description: string;
}

export interface Step {
  number: number;
  icon: LucideIcon;
  title: string;
  description: string;
}

export interface PlatformStats {
  userCount: number;
  linksCreated: number;
}

export interface HeroSectionProps {
  isAuthenticated?: boolean;
}

export interface CTASectionProps {
  isAuthenticated?: boolean;
}
