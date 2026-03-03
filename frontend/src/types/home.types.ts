/**
 * Type definitions for homepage components.
 *
 * This file is created by the first scenario builder and
 * should contain types used across homepage modules.
 */

export interface Feature {
  icon: string;
  title: string;
  description: string;
}

export interface ShortenResult {
  shortUrl: string;
  shortCode: string;
}

export interface ShortenError {
  message: string;
  code: string;
}

export interface HeroSectionProps {
  serviceName?: string;
  tagline?: string;
}
