/**
 * Homepage-specific type definitions.
 *
 * This file is created by the first scenario builder and
 * should contain types used across homepage components.
 */

import type { ReactNode } from 'react';

/** Feature type for feature cards */
export interface Feature {
  id: string;
  title: string;
  description: string;
  icon: ReactNode;
}

/** Hero content configuration */
export interface HeroContent {
  headline: string;
  subheadline: string;
  primaryCTA: {
    label: string;
    href: string;
  };
  secondaryCTA?: {
    label: string;
    href: string;
  };
}

/** Footer link configuration */
export interface FooterLink {
  label: string;
  href: string;
}

/** CTA section configuration */
export interface CTAContent {
  headline: string;
  ctaLabel: string;
  ctaHref: string;
}
