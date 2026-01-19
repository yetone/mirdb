// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '../index.html');

/**
 * SEO Meta Tags Test Suite
 * Verifies proper meta tags for search engine optimization
 */

test.describe('SEO Meta Tags', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  test('TC1: Page has descriptive title tag containing MirDB', async ({ page }) => {
    // Check that the title tag exists and contains 'MirDB'
    const title = await page.title();

    expect(title).toBeTruthy();
    expect(title.toLowerCase()).toContain('mirdb');

    // Verify title is descriptive (not just the product name)
    expect(title.length).toBeGreaterThan(6); // More than just "MirDB"
  });

  test('TC2: Meta description exists and describes MirDB purpose', async ({ page }) => {
    // Check for meta description tag
    const metaDescription = await page.locator('meta[name="description"]');
    await expect(metaDescription).toHaveCount(1);

    // Get the content attribute
    const content = await metaDescription.getAttribute('content');

    expect(content).toBeTruthy();
    expect(content.toLowerCase()).toContain('mirdb');

    // Verify description is meaningful (describes key-value store or memcached functionality)
    const lowerContent = content.toLowerCase();
    const hasRelevantDescription =
      lowerContent.includes('key-value') ||
      lowerContent.includes('memcached') ||
      lowerContent.includes('persistent') ||
      lowerContent.includes('storage');

    expect(hasRelevantDescription).toBe(true);

    // Verify description has reasonable length (optimal: 50-160 characters)
    expect(content.length).toBeGreaterThanOrEqual(50);
    expect(content.length).toBeLessThanOrEqual(200);
  });

  test('TC3: Open Graph tags are present (og:title, og:description, og:image)', async ({ page }) => {
    // Check og:title
    const ogTitle = await page.locator('meta[property="og:title"]');
    await expect(ogTitle).toHaveCount(1);
    const ogTitleContent = await ogTitle.getAttribute('content');
    expect(ogTitleContent).toBeTruthy();
    expect(ogTitleContent.toLowerCase()).toContain('mirdb');

    // Check og:description
    const ogDescription = await page.locator('meta[property="og:description"]');
    await expect(ogDescription).toHaveCount(1);
    const ogDescContent = await ogDescription.getAttribute('content');
    expect(ogDescContent).toBeTruthy();
    expect(ogDescContent.length).toBeGreaterThan(10);

    // Check og:image
    const ogImage = await page.locator('meta[property="og:image"]');
    await expect(ogImage).toHaveCount(1);
    const ogImageContent = await ogImage.getAttribute('content');
    expect(ogImageContent).toBeTruthy();
    // Verify it's a valid URL or path
    expect(ogImageContent.length).toBeGreaterThan(0);
  });

  test('TC4: Viewport meta tag is set for responsive design', async ({ page }) => {
    // Check for viewport meta tag
    const viewport = await page.locator('meta[name="viewport"]');
    await expect(viewport).toHaveCount(1);

    // Get the content attribute
    const content = await viewport.getAttribute('content');

    expect(content).toBeTruthy();

    // Verify it contains essential viewport settings
    expect(content).toContain('width=device-width');
    expect(content).toContain('initial-scale=1');
  });
});
