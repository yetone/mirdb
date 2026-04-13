/**
 * SEO and Metadata E2E Tests
 * Owner: Scenario 11 - SEO and Metadata
 *
 * Tests:
 * - Page title contains MirDB
 * - Meta description exists
 * - Single h1 element
 * - Heading hierarchy
 * - Meta viewport tag
 * - Open Graph tags
 */
import { test, expect } from '@playwright/test';

test.describe('SEO and Metadata', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('page title contains MirDB and is descriptive', async ({ page }) => {
    const title = await page.title();

    // Title should contain MirDB
    expect(title.toLowerCase()).toContain('mirdb');

    // Title should be descriptive (mention key-value store or similar)
    expect(title.toLowerCase()).toMatch(/key-value|persistent|store|memcached/);

    // Title should be reasonably long (descriptive)
    expect(title.length).toBeGreaterThan(10);
  });

  test('meta description tag exists with relevant keywords', async ({ page }) => {
    const metaDescription = await page.locator('meta[name="description"]').getAttribute('content');

    // Meta description should exist
    expect(metaDescription).toBeTruthy();

    // Meta description should contain relevant keywords
    const keywords = ['mirdb', 'key-value', 'persistent', 'memcached'];
    const descLower = metaDescription!.toLowerCase();
    const hasKeyword = keywords.some(keyword => descLower.includes(keyword));
    expect(hasKeyword).toBe(true);

    // Meta description should be reasonable length (50-300 chars is typical)
    expect(metaDescription!.length).toBeGreaterThan(50);
    expect(metaDescription!.length).toBeLessThan(300);
  });

  test('exactly one h1 element exists on the page', async ({ page }) => {
    const h1Elements = await page.locator('h1').all();

    // There should be exactly one h1 element
    expect(h1Elements.length).toBe(1);

    // The h1 should contain MirDB
    const h1Text = await h1Elements[0].textContent();
    expect(h1Text?.toLowerCase()).toContain('mirdb');
  });

  test('headings follow proper hierarchy (no skipping from h1 to h3)', async ({ page }) => {
    // Get all heading elements
    const h1Count = await page.locator('h1').count();
    const h2Count = await page.locator('h2').count();
    const h3Count = await page.locator('h3').count();

    // Should have at least one h1 and h2
    expect(h1Count).toBe(1);
    expect(h2Count).toBeGreaterThan(0);

    // If there are h3 elements, there should be h2 elements
    // (no skipping from h1 to h3)
    if (h3Count > 0) {
      expect(h2Count).toBeGreaterThan(0);
    }

    // Check that h1 appears before h2 in document order
    const firstH1 = page.locator('h1').first();
    const firstH2 = page.locator('h2').first();

    // Both should exist
    await expect(firstH1).toBeVisible();
    await expect(firstH2).toBeVisible();

    // Verify h1 comes before h2 using DOM position
    const h1Box = await firstH1.boundingBox();
    const h2Box = await firstH2.boundingBox();

    // h1 should be above h2 (smaller y coordinate)
    expect(h1Box!.y).toBeLessThan(h2Box!.y);
  });

  test('meta viewport tag with width=device-width exists', async ({ page }) => {
    const viewportMeta = await page.locator('meta[name="viewport"]').getAttribute('content');

    // Viewport meta should exist
    expect(viewportMeta).toBeTruthy();

    // Should contain width=device-width
    expect(viewportMeta!.toLowerCase()).toContain('width=device-width');

    // Should also contain initial-scale=1 for proper mobile rendering
    expect(viewportMeta!.toLowerCase()).toMatch(/initial-scale\s*=\s*1/);
  });

  test('Open Graph meta tags (og:title and og:description) exist', async ({ page }) => {
    // Check for og:title
    const ogTitle = await page.locator('meta[property="og:title"]').getAttribute('content');
    expect(ogTitle).toBeTruthy();
    expect(ogTitle!.length).toBeGreaterThan(0);

    // og:title should mention MirDB
    expect(ogTitle!.toLowerCase()).toContain('mirdb');

    // Check for og:description
    const ogDescription = await page.locator('meta[property="og:description"]').getAttribute('content');
    expect(ogDescription).toBeTruthy();
    expect(ogDescription!.length).toBeGreaterThan(0);

    // og:description should contain relevant content
    const keywords = ['key-value', 'persistent', 'memcached', 'store'];
    const descLower = ogDescription!.toLowerCase();
    const hasKeyword = keywords.some(keyword => descLower.includes(keyword));
    expect(hasKeyword).toBe(true);
  });
});
