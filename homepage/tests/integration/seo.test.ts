/**
 * SEO Integration Tests
 * Owner: Scenario 14 - SEO and Meta Tags
 *
 * Tests that verify SEO elements are correctly rendered in the built HTML output.
 * These tests validate the actual generated HTML contains proper meta tags,
 * structured data, and semantic HTML elements.
 */

import { execSync } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';

const ROOT_DIR = path.resolve(__dirname, '../..');
const OUT_DIR = path.join(ROOT_DIR, 'out');
const INDEX_HTML = path.join(OUT_DIR, 'index.html');

describe('SEO Integration Tests', () => {
  let htmlContent: string;

  beforeAll(() => {
    // Build the static site if not already built
    if (!fs.existsSync(INDEX_HTML)) {
      console.log('Building static site for SEO tests...');
      execSync('npm run build', { cwd: ROOT_DIR, stdio: 'inherit' });
    }

    // Read the generated HTML
    if (fs.existsSync(INDEX_HTML)) {
      htmlContent = fs.readFileSync(INDEX_HTML, 'utf-8');
    } else {
      throw new Error('Build output not found. Run `npm run build` first.');
    }
  });

  describe('Test Case 1: Document Title', () => {
    it('should have title tag containing MirDB', () => {
      const titleMatch = htmlContent.match(/<title[^>]*>([^<]+)<\/title>/i);
      expect(titleMatch).not.toBeNull();
      expect(titleMatch![1]).toContain('MirDB');
    });

    it('should have title under 60 characters', () => {
      const titleMatch = htmlContent.match(/<title[^>]*>([^<]+)<\/title>/i);
      expect(titleMatch).not.toBeNull();
      expect(titleMatch![1].length).toBeLessThan(60);
    });
  });

  describe('Test Case 2: Meta Description', () => {
    it('should have meta description tag', () => {
      const descMatch = htmlContent.match(
        /<meta\s+name=["']description["']\s+content=["']([^"']+)["']/i
      );
      expect(descMatch).not.toBeNull();
    });

    it('should have description between 120-160 characters', () => {
      const descMatch = htmlContent.match(
        /<meta\s+name=["']description["']\s+content=["']([^"']+)["']/i
      );
      expect(descMatch).not.toBeNull();
      const description = descMatch![1];
      expect(description.length).toBeGreaterThanOrEqual(120);
      expect(description.length).toBeLessThanOrEqual(160);
    });
  });

  describe('Test Case 3: Open Graph Tags', () => {
    it('should have og:title tag', () => {
      const ogTitleMatch = htmlContent.match(
        /<meta\s+property=["']og:title["']\s+content=["']([^"']+)["']/i
      );
      expect(ogTitleMatch).not.toBeNull();
      expect(ogTitleMatch![1]).toContain('MirDB');
    });

    it('should have og:description tag', () => {
      const ogDescMatch = htmlContent.match(
        /<meta\s+property=["']og:description["']\s+content=["']([^"']+)["']/i
      );
      expect(ogDescMatch).not.toBeNull();
    });

    it('should have og:image tag', () => {
      const ogImageMatch = htmlContent.match(
        /<meta\s+property=["']og:image["']\s+content=["']([^"']+)["']/i
      );
      expect(ogImageMatch).not.toBeNull();
    });

    it('should have og:type tag', () => {
      const ogTypeMatch = htmlContent.match(
        /<meta\s+property=["']og:type["']\s+content=["']([^"']+)["']/i
      );
      expect(ogTypeMatch).not.toBeNull();
      expect(ogTypeMatch![1]).toBe('website');
    });
  });

  describe('Test Case 4: Semantic HTML Structure', () => {
    it('should contain header element', () => {
      expect(htmlContent).toMatch(/<header[\s>]/i);
    });

    it('should contain nav element', () => {
      expect(htmlContent).toMatch(/<nav[\s>]/i);
    });

    it('should contain main element', () => {
      expect(htmlContent).toMatch(/<main[\s>]/i);
    });

    it('should contain footer element', () => {
      expect(htmlContent).toMatch(/<footer[\s>]/i);
    });
  });

  describe('Test Case 5: JSON-LD Structured Data', () => {
    it('should contain JSON-LD script tag', () => {
      const jsonLdMatch = htmlContent.match(
        /<script[^>]*type=["']application\/ld\+json["'][^>]*>([^<]+)<\/script>/i
      );
      expect(jsonLdMatch).not.toBeNull();
    });

    it('should have valid JSON-LD with SoftwareApplication schema', () => {
      const jsonLdMatch = htmlContent.match(
        /<script[^>]*type=["']application\/ld\+json["'][^>]*>([^<]+)<\/script>/i
      );
      expect(jsonLdMatch).not.toBeNull();

      const jsonLd = JSON.parse(jsonLdMatch![1]);
      expect(jsonLd['@context']).toBe('https://schema.org');
      expect(['SoftwareApplication', 'WebSite']).toContain(jsonLd['@type']);
      expect(jsonLd.name).toBe('MirDB');
    });

    it('should have required JSON-LD properties', () => {
      const jsonLdMatch = htmlContent.match(
        /<script[^>]*type=["']application\/ld\+json["'][^>]*>([^<]+)<\/script>/i
      );
      expect(jsonLdMatch).not.toBeNull();

      const jsonLd = JSON.parse(jsonLdMatch![1]);
      expect(jsonLd.description).toBeDefined();
      expect(jsonLd.url).toBeDefined();
    });
  });

  describe('Test Case 6: Canonical URL', () => {
    it('should have canonical link element', () => {
      const canonicalMatch = htmlContent.match(
        /<link\s+rel=["']canonical["']\s+href=["']([^"']+)["']/i
      );
      expect(canonicalMatch).not.toBeNull();
    });

    it('should have canonical URL pointing to primary URL', () => {
      const canonicalMatch = htmlContent.match(
        /<link\s+rel=["']canonical["']\s+href=["']([^"']+)["']/i
      );
      expect(canonicalMatch).not.toBeNull();
      expect(canonicalMatch![1]).toMatch(/https?:\/\/mirdb\.io\/?/);
    });
  });

  describe('Additional SEO Elements', () => {
    it('should have lang attribute on html element', () => {
      expect(htmlContent).toMatch(/<html[^>]*lang=["']en["']/i);
    });

    it('should have viewport meta tag', () => {
      expect(htmlContent).toMatch(
        /<meta\s+name=["']viewport["']/i
      );
    });

    it('should have charset meta tag', () => {
      expect(htmlContent).toMatch(
        /<meta\s+charset=["']utf-8["']/i
      );
    });
  });
});
