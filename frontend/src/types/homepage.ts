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
  icon: string;
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
