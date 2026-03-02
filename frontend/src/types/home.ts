/**
 * Homepage-specific type definitions.
 *
 * This file is created by the first scenario builder and
 * should contain types used across homepage components.
 */

export interface UrlFormData {
  originalUrl: string;
}

export interface ShortenedUrlResult {
  shortCode: string;
  shortUrl: string;
  originalUrl: string;
  isGuest: boolean;
}

export interface ProcessStepData {
  stepNumber: number;
  title: string;
  description: string;
  icon: React.ReactNode;
}

export interface FeatureCardData {
  title: string;
  description: string;
  visualization: React.ReactNode;
}
