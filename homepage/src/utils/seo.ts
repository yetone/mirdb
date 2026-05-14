import type { SEOProps } from '../types';
import { APP_NAME, APP_DESCRIPTION, SITE_URL } from './constants';

export interface MetaTag {
  tag: string;
  attributes: Record<string, string>;
}

export interface SEOData {
  title: string;
  description: string;
  ogTitle: string;
  ogDescription: string;
  ogImage: string;
  ogUrl: string;
  ogType: string;
  canonical: string;
}

export function generateSEOTags(props: Partial<SEOProps> = {}): MetaTag[] {
  const title = props.title || `${APP_NAME} - Persistent Key-Value Store`;
  const description = props.description || APP_DESCRIPTION;
  const ogImage = props.ogImage || '/assets/logo.gif';
  const ogType = props.ogType || 'website';
  const canonical = props.canonical || SITE_URL;

  return [
    { tag: 'meta', attributes: { charset: 'UTF-8' } },
    { tag: 'meta', attributes: { name: 'viewport', content: 'width=device-width, initial-scale=1' } },
    { tag: 'title', attributes: { innerHTML: title } },
    { tag: 'meta', attributes: { name: 'description', content: description } },
    { tag: 'meta', attributes: { property: 'og:title', content: title } },
    { tag: 'meta', attributes: { property: 'og:description', content: description } },
    { tag: 'meta', attributes: { property: 'og:image', content: ogImage } },
    { tag: 'meta', attributes: { property: 'og:url', content: canonical } },
    { tag: 'meta', attributes: { property: 'og:type', content: ogType } },
    { tag: 'link', attributes: { rel: 'canonical', href: canonical } },
  ];
}

export function validateTitle(title: string): { valid: boolean; length: number; reason?: string } {
  const length = title.length;
  if (length < 30) {
    return { valid: false, length, reason: `Title too short (${length} chars). Minimum 30 recommended.` };
  }
  if (length > 70) {
    return { valid: false, length, reason: `Title too long (${length} chars). Maximum 70 recommended.` };
  }
  return { valid: true, length };
}

export function validateDescription(desc: string): { valid: boolean; length: number; reason?: string } {
  const length = desc.length;
  if (length < 50) {
    return { valid: false, length, reason: `Description too short (${length} chars). Minimum 50 recommended.` };
  }
  if (length > 160) {
    return { valid: false, length, reason: `Description too long (${length} chars). Maximum 160 recommended.` };
  }
  return { valid: true, length };
}
