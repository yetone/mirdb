/**
 * Homepage Type Definitions
 * Owner: Scenario 1 (Shared resource)
 *
 * Type definitions specific to homepage components.
 */

import { ReactNode } from 'react';

/** Feature item for the features section */
export interface Feature {
  icon: ReactNode;
  title: string;
  description: string;
}

/** Step item for how-it-works section */
export interface Step {
  number: number;
  title: string;
  description: string;
  icon?: ReactNode;
}

/** Result from URL shortening operation */
export interface ShortenResult {
  originalUrl: string;
  shortUrl: string;
  shortCode: string;
}

/** Props for QuickShortenForm component */
export interface QuickShortenFormProps {
  onSuccess?: (shortUrl: string) => void;
  onError?: (error: string) => void;
}

/** Props for HeroSection component */
export interface HeroSectionProps {
  onGetStarted?: () => void;
  onLearnMore?: () => void;
}

/** Props for Footer component */
export interface FooterProps {
  brandName?: string;
}

/** State returned by useGuestShorten hook */
export interface UseGuestShortenReturn {
  shortenUrl: (url: string) => Promise<ShortenResult>;
  isLoading: boolean;
  error: string | null;
  result: ShortenResult | null;
  reset: () => void;
}
