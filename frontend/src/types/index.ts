/**
 * Shared type definitions for the homepage.
 *
 * This file is created by the first scenario builder and
 * should contain types used across multiple homepage modules.
 */

// Homepage-specific types
export interface FeatureCard {
  title: string;
  description: string;
  icon: React.ComponentType;
  comingSoon?: boolean;
}

export interface SocialMetric {
  value: string;
  label: string;
}