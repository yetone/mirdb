// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('SEO Meta Tags', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Page title includes MirDB and describes the product', async ({ page }) => {
    // Get the page title
    const title = await page.title();

    // Verify title includes 'MirDB'
    expect(title).toContain('MirDB');

    // Verify title describes the product (key-value store, database, etc.)
    expect(title.toLowerCase()).toMatch(/key-value|database|store|memcached|persistent/);

    // Verify title is a reasonable length (50-60 characters is optimal for SEO)
    expect(title.length).toBeGreaterThan(10);
    expect(title.length).toBeLessThan(100);
  });

  test('TC2: Meta description accurately describes MirDB in under 160 characters', async ({ page }) => {
    // Get the meta description element
    const metaDescription = page.locator('meta[name="description"]');

    // Verify meta description exists
    await expect(metaDescription).toHaveCount(1);

    // Get the content attribute
    const content = await metaDescription.getAttribute('content');

    // Verify content is not empty
    expect(content).toBeTruthy();

    // Verify description includes MirDB
    expect(content).toContain('MirDB');

    // Verify description is under 160 characters (SEO best practice)
    expect(content.length).toBeLessThanOrEqual(160);

    // Verify description accurately describes the product
    expect(content.toLowerCase()).toMatch(/key-value|persistent|memcached|rust/);
  });

  test('TC3: Open Graph tags are present (og:title, og:description, og:image)', async ({ page }) => {
    // Check for og:title
    const ogTitle = page.locator('meta[property="og:title"]');
    await expect(ogTitle).toHaveCount(1);
    const ogTitleContent = await ogTitle.getAttribute('content');
    expect(ogTitleContent).toBeTruthy();
    expect(ogTitleContent).toContain('MirDB');

    // Check for og:description
    const ogDescription = page.locator('meta[property="og:description"]');
    await expect(ogDescription).toHaveCount(1);
    const ogDescContent = await ogDescription.getAttribute('content');
    expect(ogDescContent).toBeTruthy();
    expect(ogDescContent.length).toBeGreaterThan(10);

    // Check for og:image
    const ogImage = page.locator('meta[property="og:image"]');
    await expect(ogImage).toHaveCount(1);
    const ogImageContent = await ogImage.getAttribute('content');
    expect(ogImageContent).toBeTruthy();
    // Image should have a valid path or URL
    expect(ogImageContent).toMatch(/\.(gif|png|jpg|jpeg|webp)$/i);

    // Additional OG tags that are good to have
    const ogType = page.locator('meta[property="og:type"]');
    await expect(ogType).toHaveCount(1);

    const ogUrl = page.locator('meta[property="og:url"]');
    await expect(ogUrl).toHaveCount(1);
  });

  test('TC4: Canonical link element is present with correct URL', async ({ page }) => {
    // Check for canonical link
    const canonical = page.locator('link[rel="canonical"]');
    await expect(canonical).toHaveCount(1);

    // Get the href attribute
    const canonicalUrl = await canonical.getAttribute('href');

    // Verify the URL is present and valid
    expect(canonicalUrl).toBeTruthy();

    // Verify it's a valid URL format (absolute URL with https)
    expect(canonicalUrl).toMatch(/^https:\/\//);

    // Verify it contains the project domain/path
    expect(canonicalUrl).toMatch(/mirdb|github/i);
  });
});
