/**
 * Shared type definitions used across modules.
 */

export interface User {
  id: string;
  email: string;
  name: string;
}

export interface ShortUrlRequest {
  original_url: string;
}

export interface ShortUrlResponse {
  short_code: string;
  original_url: string;
  created_at: string;
}

export type Theme = 'light' | 'dark' | 'cyberpunk' | 'synthwave';
