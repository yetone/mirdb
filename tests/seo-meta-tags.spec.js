const { test, expect } = require('@playwright/test');

test.describe('SEO - Meta Tags', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Page has title tag containing MirDB and relevant keywords', async ({ page }) => {
    const title = await page.title();

    // Title should contain 'MirDB'
    expect(title.toLowerCase()).toContain('mirdb');

    // Title should be descriptive and contain relevant keywords
    expect(title.length).toBeGreaterThan(10);
    expect(title.length).toBeLessThanOrEqual(60); // Optimal SEO title length

    // Title should mention key functionality
    expect(title.toLowerCase()).toMatch(/key-value|memcached|persistent/i);
  });

  test('TC2: Page has meta description under 160 characters describing MirDB', async ({ page }) => {
    const metaDescription = await page.locator('meta[name="description"]').getAttribute('content');

    // Meta description should exist
    expect(metaDescription).not.toBeNull();
    expect(metaDescription).toBeTruthy();

    // Meta description should be under 160 characters (optimal for SEO)
    expect(metaDescription.length).toBeLessThanOrEqual(160);

    // Meta description should be meaningful (at least 50 chars)
    expect(metaDescription.length).toBeGreaterThan(50);

    // Meta description should mention MirDB
    expect(metaDescription.toLowerCase()).toContain('mirdb');

    // Meta description should describe the value proposition
    expect(metaDescription.toLowerCase()).toMatch(/memcached|persistent|key-value/i);
  });

  test('TC3: Page has viewport meta tag for responsive design', async ({ page }) => {
    const viewport = await page.locator('meta[name="viewport"]').getAttribute('content');

    // Viewport meta tag should exist
    expect(viewport).not.toBeNull();
    expect(viewport).toBeTruthy();

    // Viewport should contain width=device-width
    expect(viewport).toContain('width=device-width');

    // Viewport should contain initial-scale=1
    expect(viewport.toLowerCase()).toMatch(/initial-scale\s*=\s*1/);
  });

  test('TC4: Page has og:title meta tag', async ({ page }) => {
    const ogTitle = await page.locator('meta[property="og:title"]').getAttribute('content');

    // og:title should exist
    expect(ogTitle).not.toBeNull();
    expect(ogTitle).toBeTruthy();

    // og:title should contain MirDB
    expect(ogTitle.toLowerCase()).toContain('mirdb');

    // og:title should be descriptive
    expect(ogTitle.length).toBeGreaterThan(10);
  });

  test('Page has og:description meta tag', async ({ page }) => {
    const ogDescription = await page.locator('meta[property="og:description"]').getAttribute('content');

    // og:description should exist
    expect(ogDescription).not.toBeNull();
    expect(ogDescription).toBeTruthy();

    // og:description should mention key value proposition
    expect(ogDescription.toLowerCase()).toMatch(/memcached|persistent|key-value/i);
  });

  test('Page has og:image meta tag', async ({ page }) => {
    const ogImage = await page.locator('meta[property="og:image"]').getAttribute('content');

    // og:image should exist
    expect(ogImage).not.toBeNull();
    expect(ogImage).toBeTruthy();

    // og:image should be a valid URL or path
    expect(ogImage).toMatch(/^(https?:\/\/|\/|assets\/)/);
  });

  test('Page has og:type meta tag', async ({ page }) => {
    const ogType = await page.locator('meta[property="og:type"]').getAttribute('content');

    // og:type should exist
    expect(ogType).not.toBeNull();
    expect(ogType).toBeTruthy();

    // og:type should be 'website' for a homepage
    expect(ogType).toBe('website');
  });

  test('Page has charset meta tag set to UTF-8', async ({ page }) => {
    const charset = await page.locator('meta[charset]').getAttribute('charset');

    // charset should exist and be UTF-8
    expect(charset).not.toBeNull();
    expect(charset.toUpperCase()).toBe('UTF-8');
  });

  test('Page has keywords meta tag', async ({ page }) => {
    const keywords = await page.locator('meta[name="keywords"]').getAttribute('content');

    // keywords should exist
    expect(keywords).not.toBeNull();
    expect(keywords).toBeTruthy();

    // keywords should contain relevant terms
    expect(keywords.toLowerCase()).toContain('mirdb');
    expect(keywords.toLowerCase()).toMatch(/memcached|key-value|database|persistent/i);
  });
});
