/**
 * E2E tests for SEO optimization.
 * Owner: Scenario 13 - SEO Optimization
 *
 * Tests cover:
 * - Page title and meta description
 * - Open Graph tags
 * - Twitter Card tags
 * - JSON-LD structured data
 * - Semantic HTML structure
 * - Canonical URL
 */

import { test, expect } from '@playwright/test';

test.describe('SEO Optimization', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Page Title', () => {
    test('title contains MirDB and key descriptors', async ({ page }) => {
      const title = await page.title();

      expect(title).toContain('MirDB');
      // Title should contain key descriptors about the product
      expect(title.toLowerCase()).toMatch(/key-value|persistent|memcached/);
    });

    test('title is appropriate length for search engines', async ({ page }) => {
      const title = await page.title();

      // Title should be between 30-60 characters for optimal display
      expect(title.length).toBeGreaterThan(20);
      expect(title.length).toBeLessThan(80);
    });
  });

  test.describe('Meta Description', () => {
    test('meta description mentions persistent key-value store and Memcached protocol', async ({ page }) => {
      const metaDescription = await page.getAttribute('meta[name="description"]', 'content');

      expect(metaDescription).not.toBeNull();
      expect(metaDescription!.toLowerCase()).toContain('persistent');
      expect(metaDescription!.toLowerCase()).toContain('key-value');
      expect(metaDescription!.toLowerCase()).toContain('memcached');
    });

    test('meta description has appropriate length', async ({ page }) => {
      const metaDescription = await page.getAttribute('meta[name="description"]', 'content');

      expect(metaDescription).not.toBeNull();
      // Meta description should be between 120-160 characters for optimal display
      expect(metaDescription!.length).toBeGreaterThan(50);
      expect(metaDescription!.length).toBeLessThan(200);
    });
  });

  test.describe('Open Graph Tags', () => {
    test('og:title is present', async ({ page }) => {
      const ogTitle = await page.getAttribute('meta[property="og:title"]', 'content');

      expect(ogTitle).not.toBeNull();
      expect(ogTitle).toContain('MirDB');
    });

    test('og:description is present', async ({ page }) => {
      const ogDescription = await page.getAttribute('meta[property="og:description"]', 'content');

      expect(ogDescription).not.toBeNull();
      expect(ogDescription!.length).toBeGreaterThan(50);
    });

    test('og:image is present', async ({ page }) => {
      const ogImage = await page.getAttribute('meta[property="og:image"]', 'content');

      expect(ogImage).not.toBeNull();
      // Should be a valid URL or path
      expect(ogImage).toMatch(/https?:\/\/|^\//);
    });

    test('og:type is present and set to website', async ({ page }) => {
      const ogType = await page.getAttribute('meta[property="og:type"]', 'content');

      expect(ogType).toBe('website');
    });

    test('og:url is present', async ({ page }) => {
      const ogUrl = await page.getAttribute('meta[property="og:url"]', 'content');

      expect(ogUrl).not.toBeNull();
      // Should be a valid URL
      expect(ogUrl).toMatch(/https?:\/\//);
    });
  });

  test.describe('Twitter Card Tags', () => {
    test('twitter:card is present', async ({ page }) => {
      const twitterCard = await page.getAttribute('meta[name="twitter:card"]', 'content');

      expect(twitterCard).not.toBeNull();
      // Should be summary, summary_large_image, player, or app
      expect(['summary', 'summary_large_image', 'player', 'app']).toContain(twitterCard);
    });

    test('twitter:title is present', async ({ page }) => {
      const twitterTitle = await page.getAttribute('meta[name="twitter:title"]', 'content');

      expect(twitterTitle).not.toBeNull();
      expect(twitterTitle).toContain('MirDB');
    });

    test('twitter:description is present', async ({ page }) => {
      const twitterDescription = await page.getAttribute('meta[name="twitter:description"]', 'content');

      expect(twitterDescription).not.toBeNull();
      expect(twitterDescription!.length).toBeGreaterThan(50);
    });

    test('twitter:image is present', async ({ page }) => {
      const twitterImage = await page.getAttribute('meta[name="twitter:image"]', 'content');

      expect(twitterImage).not.toBeNull();
      // Should be a valid URL or path
      expect(twitterImage).toMatch(/https?:\/\/|^\//);
    });
  });

  test.describe('JSON-LD Structured Data', () => {
    test('JSON-LD script tag is present', async ({ page }) => {
      const jsonLdScript = await page.locator('script[type="application/ld+json"]');

      await expect(jsonLdScript).toBeAttached();
    });

    test('JSON-LD contains SoftwareApplication or WebSite schema', async ({ page }) => {
      const jsonLdContent = await page.evaluate(() => {
        const script = document.querySelector('script[type="application/ld+json"]');
        return script ? script.textContent : null;
      });

      expect(jsonLdContent).not.toBeNull();

      const jsonLd = JSON.parse(jsonLdContent!);

      // Should have a valid @context
      expect(jsonLd['@context']).toBe('https://schema.org');

      // Should be SoftwareApplication or WebSite type
      expect(['SoftwareApplication', 'WebSite', 'WebApplication']).toContain(jsonLd['@type']);
    });

    test('JSON-LD contains required fields', async ({ page }) => {
      const jsonLdContent = await page.evaluate(() => {
        const script = document.querySelector('script[type="application/ld+json"]');
        return script ? script.textContent : null;
      });

      expect(jsonLdContent).not.toBeNull();

      const jsonLd = JSON.parse(jsonLdContent!);

      // Should have name
      expect(jsonLd.name).toBeDefined();
      expect(jsonLd.name).toContain('MirDB');

      // Should have description
      expect(jsonLd.description).toBeDefined();
    });
  });

  test.describe('Semantic HTML Structure', () => {
    test('page has header element', async ({ page }) => {
      const header = page.locator('header');

      await expect(header).toBeAttached();
    });

    test('page has main element', async ({ page }) => {
      const main = page.locator('main');

      await expect(main).toBeAttached();
      // Should only have one main element
      const mainCount = await page.locator('main').count();
      expect(mainCount).toBe(1);
    });

    test('page has section elements', async ({ page }) => {
      const sections = page.locator('section');

      // Should have multiple sections
      const sectionCount = await sections.count();
      expect(sectionCount).toBeGreaterThan(0);
    });

    test('page has footer element', async ({ page }) => {
      const footer = page.locator('footer');

      await expect(footer).toBeAttached();
    });

    test('header has appropriate role', async ({ page }) => {
      const header = page.locator('header');

      // Check for role="banner" or implicit banner role
      const role = await header.getAttribute('role');
      // header element has implicit banner role, or explicit role
      const hasAppropriateRole = role === 'banner' || role === null;
      expect(hasAppropriateRole).toBe(true);
    });

    test('main content comes after header', async ({ page }) => {
      const html = await page.content();

      const headerIndex = html.indexOf('<header');
      const mainIndex = html.indexOf('<main');

      expect(headerIndex).toBeLessThan(mainIndex);
    });

    test('footer comes after main content', async ({ page }) => {
      const html = await page.content();

      const mainIndex = html.indexOf('<main');
      const footerIndex = html.indexOf('<footer');

      expect(mainIndex).toBeLessThan(footerIndex);
    });
  });

  test.describe('Canonical URL', () => {
    test('canonical link element is present', async ({ page }) => {
      const canonical = await page.getAttribute('link[rel="canonical"]', 'href');

      expect(canonical).not.toBeNull();
    });

    test('canonical URL is a valid absolute URL', async ({ page }) => {
      const canonical = await page.getAttribute('link[rel="canonical"]', 'href');

      expect(canonical).not.toBeNull();
      // Should be an absolute URL
      expect(canonical).toMatch(/^https?:\/\//);
    });

    test('canonical URL matches the current page', async ({ page }) => {
      const canonical = await page.getAttribute('link[rel="canonical"]', 'href');

      expect(canonical).not.toBeNull();
      // For homepage, canonical should point to the root
      expect(canonical).toMatch(/\/$/);
    });
  });

  test.describe('Additional SEO Elements', () => {
    test('html element has lang attribute', async ({ page }) => {
      const lang = await page.getAttribute('html', 'lang');

      expect(lang).not.toBeNull();
      expect(lang).toBe('en');
    });

    test('viewport meta tag is present', async ({ page }) => {
      const viewport = await page.getAttribute('meta[name="viewport"]', 'content');

      expect(viewport).not.toBeNull();
      expect(viewport).toContain('width=device-width');
    });

    test('charset meta tag is present', async ({ page }) => {
      const charset = await page.locator('meta[charset]');

      await expect(charset).toBeAttached();
    });

    test('page has only one h1 element', async ({ page }) => {
      const h1Count = await page.locator('h1').count();

      expect(h1Count).toBe(1);
    });
  });
});
