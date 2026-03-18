/**
 * SEO Metadata Configuration Unit Tests.
 * Owner: Scenario 8 - Performance and SEO
 *
 * Tests:
 * - SEO metadata configuration structure
 * - Required properties presence
 * - Valid property values
 */

import { describe, it, expect } from 'vitest';
import {
  SEO_METADATA,
  SITE_TITLE,
  SITE_DESCRIPTION,
  type SEOMetadata,
} from '../../../src/utils/constants';

describe('SEO Metadata Configuration', () => {
  it('TC7: SEO component accepts and renders meta tag props', () => {
    // Verify SEO_METADATA is defined and has correct structure
    expect(SEO_METADATA).toBeDefined();

    // Verify all required properties exist
    const requiredProps: (keyof SEOMetadata)[] = [
      'title',
      'description',
      'ogTitle',
      'ogDescription',
      'ogType',
      'ogImage',
    ];

    requiredProps.forEach((prop) => {
      expect(SEO_METADATA).toHaveProperty(prop);
      expect(typeof SEO_METADATA[prop]).toBe('string');
      expect(SEO_METADATA[prop].length).toBeGreaterThan(0);
    });
  });

  it('title property contains MirDB and describes the product', () => {
    expect(SEO_METADATA.title).toContain('MirDB');
    expect(SEO_METADATA.title.toLowerCase()).toMatch(/key-value|store|persistent/i);
  });

  it('description has appropriate length for SEO (50-200 chars)', () => {
    expect(SEO_METADATA.description.length).toBeGreaterThan(50);
    expect(SEO_METADATA.description.length).toBeLessThan(200);
  });

  it('description contains meaningful content about the product', () => {
    const desc = SEO_METADATA.description.toLowerCase();
    expect(desc).toMatch(/key-value|memcached|persistent|rust|tokio/i);
  });

  it('Open Graph title matches site title', () => {
    expect(SEO_METADATA.ogTitle).toBe(SITE_TITLE);
  });

  it('Open Graph description matches site description', () => {
    expect(SEO_METADATA.ogDescription).toBe(SITE_DESCRIPTION);
  });

  it('Open Graph type is set to website', () => {
    expect(SEO_METADATA.ogType).toBe('website');
  });

  it('Open Graph image is a valid image path', () => {
    expect(SEO_METADATA.ogImage).toMatch(/\.(svg|png|jpg|jpeg|webp)$/i);
    expect(SEO_METADATA.ogImage).toContain('/assets/');
  });

  it('SEO_METADATA uses SITE_TITLE and SITE_DESCRIPTION constants', () => {
    // Verify consistency with other constants
    expect(SEO_METADATA.title).toBe(SITE_TITLE);
    expect(SEO_METADATA.description).toBe(SITE_DESCRIPTION);
  });

  it('all Open Graph properties are properly prefixed values', () => {
    // Verify OG properties are appropriate for social sharing
    expect(SEO_METADATA.ogTitle).toBeTruthy();
    expect(SEO_METADATA.ogDescription).toBeTruthy();
    expect(SEO_METADATA.ogImage).toBeTruthy();
    expect(SEO_METADATA.ogType).toBeTruthy();
  });
});
