/**
 * Shared type definitions for the landing page.
 *
 * This file is created by the first scenario builder and
 * should contain types used across landing page components.
 */

// URL shortening types
export interface ShortenUrlRequest {
  url: string;
}

export interface ShortenUrlResponse {
  short_url: string;
  short_code: string;
  original_url: string;
}

// URL validation types
export interface ValidationResult {
  valid: boolean;
  error?: string;
  normalizedUrl?: string;
}

// Feature card types
export interface Feature {
  id: string;
  icon: React.ComponentType;
  title: string;
  description: string;
}

// How it works step types
export interface Step {
  number: number;
  icon: React.ComponentType;
  title: string;
  description: string;
}

// API error types
export interface ApiError {
  message: string;
  code?: string;
  status?: number;
}
