// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('SEO Meta Tags', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Page title tag contains MirDB and is descriptive (under 60 chars)', async ({ page }) => {
    // Get the page title
    const title = await page.title();

    // Verify title contains 'MirDB'
    expect(title).toContain('MirDB');

    // Verify title is descriptive (not just 'MirDB')
    expect(title.length).toBeGreaterThan(5);

    // Verify title is under 60 characters (SEO best practice)
    expect(title.length).toBeLessThanOrEqual(60);

    // Verify title mentions key functionality
    expect(title.toLowerCase()).toMatch(/key-value|persistent|memcached/i);
  });

  test('TC2: Meta description describes MirDB as persistent key-value store (under 160 chars)', async ({ page }) => {
    // Get the meta description content
    const metaDescription = await page.locator('meta[name="description"]').getAttribute('content');

    // Verify meta description exists
    expect(metaDescription).not.toBeNull();
    expect(metaDescription).toBeTruthy();

    // Verify it mentions MirDB or key product features
    const description = metaDescription.toLowerCase();
    expect(description).toMatch(/mirdb|persistent|key-value|memcached/i);

    // Verify it describes the product as a persistent key-value store
    expect(description).toContain('key-value');

    // Verify description is under 160 characters (SEO best practice)
    expect(metaDescription.length).toBeLessThanOrEqual(160);
  });

  test('TC3: Open Graph title tag is present with appropriate content', async ({ page }) => {
    // Get the og:title content
    const ogTitle = await page.locator('meta[property="og:title"]').getAttribute('content');

    // Verify og:title exists
    expect(ogTitle).not.toBeNull();
    expect(ogTitle).toBeTruthy();

    // Verify og:title contains MirDB
    expect(ogTitle).toContain('MirDB');

    // Verify og:title is descriptive
    expect(ogTitle.length).toBeGreaterThan(5);
  });

  test('TC4: Open Graph description tag is present', async ({ page }) => {
    // Get the og:description content
    const ogDescription = await page.locator('meta[property="og:description"]').getAttribute('content');

    // Verify og:description exists
    expect(ogDescription).not.toBeNull();
    expect(ogDescription).toBeTruthy();

    // Verify og:description has meaningful content
    expect(ogDescription.length).toBeGreaterThan(10);

    // Verify it describes the product
    const description = ogDescription.toLowerCase();
    expect(description).toMatch(/persistent|key-value|memcached|database|storage/i);
  });

  test('TC5: Open Graph image tag is present for social sharing preview', async ({ page }) => {
    // Get the og:image content
    const ogImage = await page.locator('meta[property="og:image"]').getAttribute('content');

    // Verify og:image exists
    expect(ogImage).not.toBeNull();
    expect(ogImage).toBeTruthy();

    // Verify it's a valid URL or path
    expect(ogImage.length).toBeGreaterThan(0);

    // Verify it points to an image file (common extensions or URL)
    expect(ogImage).toMatch(/\.(png|jpg|jpeg|gif|webp|svg)|https?:\/\//i);
  });

  test('TC6: Canonical link tag is present pointing to the correct URL', async ({ page }) => {
    // Get the canonical link href
    const canonicalLink = await page.locator('link[rel="canonical"]').getAttribute('href');

    // Verify canonical link exists
    expect(canonicalLink).not.toBeNull();
    expect(canonicalLink).toBeTruthy();

    // Verify it's a valid URL format
    expect(canonicalLink).toMatch(/^https?:\/\//);
  });
});
