import { ReactNode } from 'react';

/**
 * Homepage-specific type definitions.
 *
 * This file is created by the first scenario builder and
 * should contain types used by homepage components.
 *
 * Expected exports:
 * - Feature: { id, icon, title, description }
 * - ShortenedUrl: { shortCode, originalUrl, fullShortUrl }
 * - UrlValidationResult: { isValid, errorMessage? }
 */

export interface Feature {
  id: string;
  icon: ReactNode;
  title: string;
  description: string;
}

export interface ShortenedUrl {
  shortCode: string;
  originalUrl: string;
  fullShortUrl: string;
}

export interface UrlValidationResult {
  isValid: boolean;
  errorMessage?: string;
}

export interface FeatureCardProps {
  icon: ReactNode;
  title: string;
  description: string;
}

export interface ShortenUrlResponse {
  id: number;
  original_url: string;
  short_code: string;
  created_at: string;
  user_id: number | null;
  click_count: number;
}
