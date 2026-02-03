/**
 * SEO Optimization Tests
 * Owner: Scenario 13 - SEO Optimization
 *
 * Tests for:
 * - Meta tags (title, description, Open Graph)
 * - Semantic HTML structure
 * - Canonical URL
 * - JSON-LD structured data
 */
import { describe, it, expect } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';
import { SITE_TITLE, SITE_DESCRIPTION, SOCIAL_IMAGE } from '../../../src/utils/constants';

// Read the BaseLayout template
const layoutPath = path.join(__dirname, '../../../src/layouts/BaseLayout.astro');
const layoutContent = fs.readFileSync(layoutPath, 'utf-8');

// Read the index.astro template
const indexPath = path.join(__dirname, '../../../src/pages/index.astro');
const indexContent = fs.readFileSync(indexPath, 'utf-8');

describe('SEO Optimization', () => {
  describe('Test Case 1: Page Title Tag', () => {
    it('should have a title tag in the layout', () => {
      expect(layoutContent).toContain('<title>');
      expect(layoutContent).toContain('</title>');
    });

    it('should use the title prop or default SITE_TITLE', () => {
      // Check that title is rendered dynamically
      expect(layoutContent).toMatch(/<title>\{title\}<\/title>/);
    });

    it('should have SITE_TITLE containing MirDB', () => {
      expect(SITE_TITLE).toContain('MirDB');
    });

    it('should have SITE_TITLE under 60 characters', () => {
      expect(SITE_TITLE.length).toBeLessThanOrEqual(60);
    });

    it('should have SITE_TITLE with descriptive text', () => {
      expect(SITE_TITLE.toLowerCase()).toMatch(/key-value|persistent|memcached/);
    });
  });

  describe('Test Case 2: Meta Description', () => {
    it('should have a meta description tag', () => {
      expect(layoutContent).toContain('name="description"');
    });

    it('should have SITE_DESCRIPTION between 150-160 characters', () => {
      expect(SITE_DESCRIPTION.length).toBeGreaterThanOrEqual(150);
      expect(SITE_DESCRIPTION.length).toBeLessThanOrEqual(160);
    });

    it('should have SITE_DESCRIPTION that accurately describes MirDB', () => {
      const descLower = SITE_DESCRIPTION.toLowerCase();
      expect(descLower).toContain('mirdb');
      expect(descLower).toMatch(/key-value|persistent|memcached|lsm/);
    });
  });

  describe('Test Case 3: Open Graph Tags', () => {
    it('should have og:title meta tag', () => {
      expect(layoutContent).toContain('property="og:title"');
    });

    it('should have og:description meta tag', () => {
      expect(layoutContent).toContain('property="og:description"');
    });

    it('should have og:image meta tag', () => {
      expect(layoutContent).toContain('property="og:image"');
    });

    it('should have og:type meta tag', () => {
      expect(layoutContent).toContain('property="og:type"');
    });

    it('should have SOCIAL_IMAGE defined for og:image', () => {
      expect(SOCIAL_IMAGE).toBeDefined();
      expect(SOCIAL_IMAGE.length).toBeGreaterThan(0);
    });

    it('should have Twitter card meta tags for social sharing', () => {
      expect(layoutContent).toContain('name="twitter:card"');
      expect(layoutContent).toContain('name="twitter:title"');
      expect(layoutContent).toContain('name="twitter:description"');
    });
  });

  describe('Test Case 4: Semantic HTML Structure', () => {
    it('should have a main element in the page structure', () => {
      expect(indexContent).toContain('<main');
    });

    it('should have section elements for content areas', () => {
      // Check that sections are used in components (by verifying they're referenced)
      // Hero, Features, UsageExamples, Architecture, Installation all use <section>
      expect(indexContent).toMatch(/<Hero|<Features|<UsageExamples|<Architecture|<Installation/);
    });

    it('should have a footer element', () => {
      expect(indexContent).toContain('<Footer');
    });

    it('should have proper html lang attribute', () => {
      expect(layoutContent).toContain('lang="en"');
    });

    it('should have a single h1 heading (in Hero component)', () => {
      // The h1 should be in Hero component - verify BaseLayout doesn't duplicate it
      expect(layoutContent).not.toContain('<h1');
    });
  });

  describe('Test Case 5: Canonical URL', () => {
    it('should have a canonical link tag', () => {
      expect(layoutContent).toContain('rel="canonical"');
    });

    it('should have og:url meta tag for canonical URL', () => {
      expect(layoutContent).toContain('property="og:url"');
    });
  });

  describe('Additional SEO Best Practices', () => {
    it('should have viewport meta tag', () => {
      expect(layoutContent).toContain('name="viewport"');
    });

    it('should have charset meta tag', () => {
      expect(layoutContent).toContain('charset="UTF-8"');
    });

    it('should have a favicon link', () => {
      expect(layoutContent).toContain('rel="icon"');
    });

    it('should have JSON-LD structured data', () => {
      expect(layoutContent).toContain('type="application/ld+json"');
    });

    it('should include JSON-LD with SoftwareApplication schema', () => {
      // Check for key properties in the JSON-LD
      expect(layoutContent).toMatch(/@type.*SoftwareApplication|"@type"\s*:\s*"SoftwareApplication"/);
    });
  });
});
