/**
 * Homepage-specific type definitions.
 * Owner: First scenario builder
 */

export interface Feature {
  id: string;
  icon: string;
  title: string;
  description: string;
}

export interface Testimonial {
  id: string;
  quote: string;
  author: string;
  role: string;
  company?: string;
  avatar?: string;
}

export interface FAQItem {
  id: string;
  question: string;
  answer: string;
}

export interface PublicStats {
  total_urls: number;
  total_clicks: number;
  active_users?: number;
}

export interface ShortenResponse {
  short_code: string;
  short_url: string;
  original_url: string;
}

export interface SEOConfig {
  title: string;
  description: string;
  canonicalUrl: string;
  ogImage?: string;
}
