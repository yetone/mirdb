/**
 * Homepage Type Definitions
 * Owner: Scenario 1 (Shared resource)
 *
 * Type definitions specific to homepage components.
 */

import { ReactNode } from 'react';

export interface Feature {
  icon: ReactNode;
  title: string;
  description: string;
}

export interface Step {
  number: number;
  title: string;
  description: string;
  icon?: ReactNode;
}

export interface ShortenResult {
  originalUrl: string;
  shortUrl: string;
  shortCode: string;
}

export interface QuickShortenFormProps {
  onSuccess?: (shortUrl: string) => void;
  onError?: (error: string) => void;
}

export interface HeroSectionProps {
  onGetStarted?: () => void;
  onLearnMore?: () => void;
}

export interface FooterProps {
  brandName?: string;
}
