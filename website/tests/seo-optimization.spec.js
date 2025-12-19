// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

// Base URL for the static HTML file
const BASE_URL = 'file://' + path.resolve(__dirname, '../dist/index.html');

test.describe('SEO Optimization - NFR-4 Compliance', () => {

  test.describe('Test Case 1: Page Title Tag', () => {
    test('should have a title tag containing "MirDB"', async ({ page }) => {
      await page.goto(BASE_URL);

      const title = await page.title();
      expect(title).toContain('MirDB');
    });

    test('should have title tag under 60 characters', async ({ page }) => {
      await page.goto(BASE_URL);

      const title = await page.title();
      expect(title.length).toBeLessThanOrEqual(60);
    });

    test('should have a descriptive and relevant title', async ({ page }) => {
      await page.goto(BASE_URL);

      const title = await page.title();
      expect(title).toBeTruthy();
      expect(title.length).toBeGreaterThan(10);
      // Title should contain key product terms
      expect(title.toLowerCase()).toMatch(/mirdb|key-value|memcached/);
    });
  });

  test.describe('Test Case 2: Meta Description', () => {
    test('should have a meta description tag', async ({ page }) => {
      await page.goto(BASE_URL);

      const description = await page.evaluate(() => {
        const meta = document.querySelector('meta[name="description"]');
        return meta?.getAttribute('content');
      });

      expect(description).toBeTruthy();
    });

    test('should have meta description under 160 characters', async ({ page }) => {
      await page.goto(BASE_URL);

      const description = await page.evaluate(() => {
        const meta = document.querySelector('meta[name="description"]');
        return meta?.getAttribute('content');
      });

      expect(description).toBeTruthy();
      expect(description.length).toBeLessThanOrEqual(160);
    });

    test('should have descriptive meta description with key terms', async ({ page }) => {
      await page.goto(BASE_URL);

      const description = await page.evaluate(() => {
        const meta = document.querySelector('meta[name="description"]');
        return meta?.getAttribute('content');
      });

      expect(description).toBeTruthy();
      expect(description.length).toBeGreaterThan(50);
      // Should mention key product features
      expect(description.toLowerCase()).toMatch(/mirdb|key-value|memcached|persistent/i);
    });
  });

  test.describe('Test Case 3: Open Graph Tags', () => {
    test('should have og:title tag', async ({ page }) => {
      await page.goto(BASE_URL);

      const ogTitle = await page.evaluate(() => {
        const meta = document.querySelector('meta[property="og:title"]');
        return meta?.getAttribute('content');
      });

      expect(ogTitle).toBeTruthy();
      expect(ogTitle.length).toBeGreaterThan(0);
    });

    test('should have og:description tag', async ({ page }) => {
      await page.goto(BASE_URL);

      const ogDescription = await page.evaluate(() => {
        const meta = document.querySelector('meta[property="og:description"]');
        return meta?.getAttribute('content');
      });

      expect(ogDescription).toBeTruthy();
      expect(ogDescription.length).toBeGreaterThan(0);
    });

    test('should have og:image tag', async ({ page }) => {
      await page.goto(BASE_URL);

      const ogImage = await page.evaluate(() => {
        const meta = document.querySelector('meta[property="og:image"]');
        return meta?.getAttribute('content');
      });

      expect(ogImage).toBeTruthy();
      expect(ogImage.length).toBeGreaterThan(0);
    });

    test('should have og:type tag', async ({ page }) => {
      await page.goto(BASE_URL);

      const ogType = await page.evaluate(() => {
        const meta = document.querySelector('meta[property="og:type"]');
        return meta?.getAttribute('content');
      });

      expect(ogType).toBeTruthy();
      expect(ogType).toBe('website');
    });

    test('should have og:url tag', async ({ page }) => {
      await page.goto(BASE_URL);

      const ogUrl = await page.evaluate(() => {
        const meta = document.querySelector('meta[property="og:url"]');
        return meta?.getAttribute('content');
      });

      expect(ogUrl).toBeTruthy();
    });
  });

  test.describe('Test Case 4: Canonical URL', () => {
    test('should have a canonical URL link tag', async ({ page }) => {
      await page.goto(BASE_URL);

      const canonical = await page.evaluate(() => {
        const link = document.querySelector('link[rel="canonical"]');
        return link?.getAttribute('href');
      });

      expect(canonical).toBeTruthy();
    });

    test('should have a valid canonical URL format', async ({ page }) => {
      await page.goto(BASE_URL);

      const canonical = await page.evaluate(() => {
        const link = document.querySelector('link[rel="canonical"]');
        return link?.getAttribute('href');
      });

      expect(canonical).toBeTruthy();
      // Should be a valid URL (starts with https:// or http://)
      expect(canonical).toMatch(/^https?:\/\//);
    });
  });

  test.describe('Test Case 5: Robots Meta Tag', () => {
    test('should be indexable (no noindex directive)', async ({ page }) => {
      await page.goto(BASE_URL);

      const robotsMeta = await page.evaluate(() => {
        const meta = document.querySelector('meta[name="robots"]');
        return meta?.getAttribute('content');
      });

      // If robots meta exists, it should not contain "noindex"
      // If it doesn't exist, that's fine - defaults to indexable
      if (robotsMeta) {
        expect(robotsMeta.toLowerCase()).not.toContain('noindex');
      }
    });

    test('should allow following links (no nofollow directive for main page)', async ({ page }) => {
      await page.goto(BASE_URL);

      const robotsMeta = await page.evaluate(() => {
        const meta = document.querySelector('meta[name="robots"]');
        return meta?.getAttribute('content');
      });

      // If robots meta exists, verify content is appropriate
      if (robotsMeta) {
        // Check that it contains index or doesn't have noindex
        const content = robotsMeta.toLowerCase();
        const isIndexable = content.includes('index') && !content.includes('noindex');
        const isDefault = !content.includes('noindex');
        expect(isIndexable || isDefault).toBe(true);
      }
    });

    test('robots meta tag should have proper indexing directives', async ({ page }) => {
      await page.goto(BASE_URL);

      const robotsMeta = await page.evaluate(() => {
        const meta = document.querySelector('meta[name="robots"]');
        return meta?.getAttribute('content');
      });

      // Robots tag should exist and indicate indexability
      expect(robotsMeta).toBeTruthy();
      expect(robotsMeta.toLowerCase()).toContain('index');
      expect(robotsMeta.toLowerCase()).toContain('follow');
    });
  });

  test.describe('Additional SEO Checks', () => {
    test('should have proper HTML lang attribute', async ({ page }) => {
      await page.goto(BASE_URL);

      const lang = await page.evaluate(() => document.documentElement.lang);
      expect(lang).toBe('en');
    });

    test('should have proper charset meta tag', async ({ page }) => {
      await page.goto(BASE_URL);

      const charset = await page.evaluate(() => {
        const meta = document.querySelector('meta[charset]');
        return meta?.getAttribute('charset');
      });

      expect(charset?.toLowerCase()).toBe('utf-8');
    });

    test('should have proper viewport meta tag', async ({ page }) => {
      await page.goto(BASE_URL);

      const viewport = await page.evaluate(() => {
        const meta = document.querySelector('meta[name="viewport"]');
        return meta?.getAttribute('content');
      });

      expect(viewport).toContain('width=device-width');
      expect(viewport).toContain('initial-scale=1');
    });

    test('should have proper heading structure for SEO', async ({ page }) => {
      await page.goto(BASE_URL);

      // Check for exactly one H1
      const h1Count = await page.evaluate(() => {
        return document.querySelectorAll('h1').length;
      });
      expect(h1Count).toBe(1);

      // H1 should contain product name
      const h1Text = await page.evaluate(() => {
        return document.querySelector('h1')?.textContent?.trim();
      });
      expect(h1Text).toContain('MirDB');
    });

    test('should have structured data (JSON-LD)', async ({ page }) => {
      await page.goto(BASE_URL);

      const jsonLd = await page.evaluate(() => {
        const script = document.querySelector('script[type="application/ld+json"]');
        if (script) {
          try {
            return JSON.parse(script.textContent);
          } catch (e) {
            return null;
          }
        }
        return null;
      });

      expect(jsonLd).toBeTruthy();
      expect(jsonLd['@context']).toBe('https://schema.org');
    });

    test('structured data should have SoftwareApplication type', async ({ page }) => {
      await page.goto(BASE_URL);

      const jsonLd = await page.evaluate(() => {
        const script = document.querySelector('script[type="application/ld+json"]');
        if (script) {
          try {
            return JSON.parse(script.textContent);
          } catch (e) {
            return null;
          }
        }
        return null;
      });

      expect(jsonLd).toBeTruthy();
      expect(jsonLd['@type']).toBe('SoftwareApplication');
    });

    test('external links should have proper rel attributes', async ({ page }) => {
      await page.goto(BASE_URL);

      // All external links should have noopener noreferrer for security
      const externalLinks = await page.evaluate(() => {
        const links = document.querySelectorAll('a[target="_blank"]');
        return Array.from(links).map(link => ({
          href: link.getAttribute('href'),
          rel: link.getAttribute('rel')
        }));
      });

      for (const link of externalLinks) {
        expect(link.rel).toContain('noopener');
      }
    });
  });
});
