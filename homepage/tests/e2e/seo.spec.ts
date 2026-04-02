import { test, expect } from '@playwright/test';

test.describe('SEO and Meta Tags', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('page title contains MirDB and describes the product', async ({ page }) => {
    const title = await page.title();

    // Title should contain 'MirDB'
    expect(title.toLowerCase()).toContain('mirdb');

    // Title should describe the product (key-value store)
    expect(title.toLowerCase()).toMatch(/key-value|kv|store|persistent/);

    // Title should be reasonable length for SEO (under 60 chars ideally)
    expect(title.length).toBeLessThanOrEqual(70);
    expect(title.length).toBeGreaterThan(10);
  });

  test('meta description is present with appropriate length', async ({ page }) => {
    const metaDescription = page.locator('meta[name="description"]');

    await expect(metaDescription).toHaveCount(1);

    const content = await metaDescription.getAttribute('content');
    expect(content).toBeTruthy();

    // Meta description should be between 150-160 characters for optimal SEO
    // We allow a bit of flexibility (120-170) for practical purposes
    expect(content!.length).toBeGreaterThanOrEqual(120);
    expect(content!.length).toBeLessThanOrEqual(170);

    // Should describe MirDB's purpose
    expect(content!.toLowerCase()).toMatch(/mirdb|key-value|memcached|persistent/);
  });

  test('og:title tag is present', async ({ page }) => {
    const ogTitle = page.locator('meta[property="og:title"]');

    await expect(ogTitle).toHaveCount(1);

    const content = await ogTitle.getAttribute('content');
    expect(content).toBeTruthy();
    expect(content!.length).toBeGreaterThan(0);

    // Should contain MirDB
    expect(content!.toLowerCase()).toContain('mirdb');
  });

  test('og:description tag is present', async ({ page }) => {
    const ogDescription = page.locator('meta[property="og:description"]');

    await expect(ogDescription).toHaveCount(1);

    const content = await ogDescription.getAttribute('content');
    expect(content).toBeTruthy();
    expect(content!.length).toBeGreaterThan(50);
  });

  test('og:image tag points to valid image URL', async ({ page }) => {
    const ogImage = page.locator('meta[property="og:image"]');

    await expect(ogImage).toHaveCount(1);

    const content = await ogImage.getAttribute('content');
    expect(content).toBeTruthy();

    // Should be a valid URL (absolute or relative path)
    expect(content).toMatch(/^(https?:\/\/|\/)/);

    // Should point to an image file
    expect(content!.toLowerCase()).toMatch(/\.(png|jpg|jpeg|gif|webp|svg)$/);
  });

  test('canonical URL tag is present', async ({ page }) => {
    const canonical = page.locator('link[rel="canonical"]');

    await expect(canonical).toHaveCount(1);

    const href = await canonical.getAttribute('href');
    expect(href).toBeTruthy();

    // Should be a valid URL
    expect(href).toMatch(/^https?:\/\//);
  });

  test('viewport meta tag is properly configured for responsive design', async ({ page }) => {
    const viewport = page.locator('meta[name="viewport"]');

    await expect(viewport).toHaveCount(1);

    const content = await viewport.getAttribute('content');
    expect(content).toBeTruthy();

    // Should contain essential viewport settings
    expect(content!.toLowerCase()).toContain('width=device-width');
    expect(content!.toLowerCase()).toContain('initial-scale=1');
  });

  test('og:type tag is present for better social sharing', async ({ page }) => {
    const ogType = page.locator('meta[property="og:type"]');

    await expect(ogType).toHaveCount(1);

    const content = await ogType.getAttribute('content');
    expect(content).toBeTruthy();
    expect(content).toBe('website');
  });

  test('og:url tag is present', async ({ page }) => {
    const ogUrl = page.locator('meta[property="og:url"]');

    await expect(ogUrl).toHaveCount(1);

    const content = await ogUrl.getAttribute('content');
    expect(content).toBeTruthy();
    expect(content).toMatch(/^https?:\/\//);
  });
});
