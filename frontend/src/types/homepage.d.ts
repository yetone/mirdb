/**
 * Homepage-specific type definitions.
 *
 * This file is created by the first scenario builder and
 * should contain types used across homepage components.
 */

export interface Feature {
  icon: string;
  title: string;
  description: string;
}

export interface NavLink {
  label: string;
  href: string;
  isExternal?: boolean;
}

export interface HeroContent {
  headline: string;
  subheadline: string;
  ctaText: string;
  secondaryCtaText?: string;
}

export interface FooterLink {
  label: string;
  href: string;
  isExternal?: boolean;
}

export interface SocialProofItem {
  icon: string;
  title: string;
  description: string;
}
