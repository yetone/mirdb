// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('SEO Optimization - Meta Tags and Structured Data (NFR-5)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Essential Meta Tags', () => {
    test('should have a title tag containing MirDB', async ({ page }) => {
      const title = await page.title();
      expect(title).toContain('MirDB');
      expect(title.length).toBeGreaterThan(10);
      expect(title.length).toBeLessThan(70); // Recommended title length
    });

    test('should have a meta description that describes MirDB', async ({ page }) => {
      const metaDescription = await page.locator('meta[name="description"]').getAttribute('content');
      expect(metaDescription).toBeTruthy();
      expect(metaDescription).toContain('MirDB');
      expect(metaDescription.length).toBeGreaterThan(50);
      expect(metaDescription.length).toBeLessThan(160); // Recommended description length
    });

    test('should have a viewport meta tag configured for responsive design', async ({ page }) => {
      const viewport = await page.locator('meta[name="viewport"]').getAttribute('content');
      expect(viewport).toBeTruthy();
      expect(viewport).toContain('width=device-width');
      expect(viewport).toContain('initial-scale=1');
    });

    test('should have proper charset meta tag', async ({ page }) => {
      const charset = await page.locator('meta[charset]').getAttribute('charset');
      expect(charset?.toLowerCase()).toBe('utf-8');
    });
  });

  test.describe('Open Graph Tags', () => {
    test('should have og:title meta tag', async ({ page }) => {
      const ogTitle = await page.locator('meta[property="og:title"]').getAttribute('content');
      expect(ogTitle).toBeTruthy();
      expect(ogTitle).toContain('MirDB');
    });

    test('should have og:description meta tag', async ({ page }) => {
      const ogDescription = await page.locator('meta[property="og:description"]').getAttribute('content');
      expect(ogDescription).toBeTruthy();
      expect(ogDescription.length).toBeGreaterThan(10);
    });

    test('should have og:image meta tag', async ({ page }) => {
      const ogImage = await page.locator('meta[property="og:image"]').getAttribute('content');
      expect(ogImage).toBeTruthy();
      expect(ogImage).toMatch(/\.(gif|png|jpg|jpeg|webp)$/i);
    });

    test('should have og:type meta tag', async ({ page }) => {
      const ogType = await page.locator('meta[property="og:type"]').getAttribute('content');
      expect(ogType).toBeTruthy();
      expect(ogType).toBe('website');
    });

    test('should have og:url meta tag', async ({ page }) => {
      const ogUrl = await page.locator('meta[property="og:url"]').getAttribute('content');
      expect(ogUrl).toBeTruthy();
      expect(ogUrl).toMatch(/^https?:\/\//);
    });
  });

  test.describe('Canonical URL', () => {
    test('should have a canonical link tag', async ({ page }) => {
      const canonical = await page.locator('link[rel="canonical"]').getAttribute('href');
      expect(canonical).toBeTruthy();
      expect(canonical).toMatch(/^https?:\/\//);
    });
  });

  test.describe('JSON-LD Structured Data', () => {
    test('should have valid JSON-LD structured data', async ({ page }) => {
      const jsonLdScript = await page.locator('script[type="application/ld+json"]').textContent();
      expect(jsonLdScript).toBeTruthy();

      // Parse and validate JSON structure
      const jsonLd = JSON.parse(jsonLdScript);
      expect(jsonLd).toBeTruthy();
    });

    test('should have SoftwareApplication schema type', async ({ page }) => {
      const jsonLdScript = await page.locator('script[type="application/ld+json"]').textContent();
      const jsonLd = JSON.parse(jsonLdScript);

      expect(jsonLd['@context']).toBe('https://schema.org');
      expect(jsonLd['@type']).toBe('SoftwareApplication');
    });

    test('should have required SoftwareApplication properties', async ({ page }) => {
      const jsonLdScript = await page.locator('script[type="application/ld+json"]').textContent();
      const jsonLd = JSON.parse(jsonLdScript);

      // Required properties for SoftwareApplication
      expect(jsonLd.name).toBeTruthy();
      expect(jsonLd.name).toContain('MirDB');
      expect(jsonLd.description).toBeTruthy();
      expect(jsonLd.applicationCategory).toBeTruthy();
    });

    test('should have optional but recommended SoftwareApplication properties', async ({ page }) => {
      const jsonLdScript = await page.locator('script[type="application/ld+json"]').textContent();
      const jsonLd = JSON.parse(jsonLdScript);

      // Recommended properties
      expect(jsonLd.operatingSystem).toBeTruthy();
      expect(jsonLd.offers).toBeTruthy();
      expect(jsonLd.offers.price).toBe('0');
    });
  });

  test.describe('Additional SEO Best Practices', () => {
    test('should have proper HTML lang attribute', async ({ page }) => {
      const lang = await page.locator('html').getAttribute('lang');
      expect(lang).toBe('en');
    });

    test('should have robots meta tag or default to index', async ({ page }) => {
      // If robots meta exists, it should allow indexing
      const robotsMeta = page.locator('meta[name="robots"]');
      const count = await robotsMeta.count();

      if (count > 0) {
        const content = await robotsMeta.getAttribute('content');
        expect(content).not.toContain('noindex');
      }
      // If no robots meta, that's also acceptable (defaults to index)
    });

    test('should have proper heading hierarchy starting with h1', async ({ page }) => {
      const h1Elements = await page.locator('h1').count();
      expect(h1Elements).toBe(1); // Should have exactly one h1
    });

    test('should have all images with alt attributes', async ({ page }) => {
      const images = await page.locator('img').all();
      for (const img of images) {
        const alt = await img.getAttribute('alt');
        expect(alt).toBeTruthy();
      }
    });
  });
});
