// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * SEO and Meta Tags Unit Tests
 * Tests for proper meta tags for SEO and social sharing
 *
 * This file contains unit tests for:
 * - Title tag (TC1)
 * - Meta description (TC2)
 * - Viewport meta tag (TC3)
 * - Open Graph tags (TC4)
 * - Canonical URL (TC5)
 */

test.describe('SEO and Meta Tags - Unit Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Check title tag
  test('TC1: Title tag - Page has <title> tag containing "MirDB"', async ({ page }) => {
    // Get the page title
    const title = await page.title();

    // Verify title exists and is not empty
    expect(title).toBeTruthy();
    expect(title.length).toBeGreaterThan(0);

    // Verify title contains 'MirDB'
    expect(title).toContain('MirDB');

    // Verify title is descriptive (not just "MirDB" alone)
    expect(title.length).toBeGreaterThan(5);

    // Verify there's exactly one title tag
    const titleElements = await page.locator('head title').count();
    expect(titleElements).toBe(1);
  });

  // Test Case 2: Check meta description
  test('TC2: Meta description - Page has meta description tag with meaningful content', async ({ page }) => {
    // Get the meta description
    const metaDescription = await page.locator('meta[name="description"]');

    // Verify meta description exists
    await expect(metaDescription).toHaveCount(1);

    // Get the content attribute
    const content = await metaDescription.getAttribute('content');

    // Verify content exists and is not empty
    expect(content).toBeTruthy();
    expect(content.length).toBeGreaterThan(0);

    // Verify content is meaningful (at least 50 characters for a proper description)
    expect(content.length).toBeGreaterThan(50);

    // Verify content mentions key product terms
    const lowerContent = content.toLowerCase();
    expect(
      lowerContent.includes('mirdb') ||
      lowerContent.includes('key-value') ||
      lowerContent.includes('memcached') ||
      lowerContent.includes('persistent')
    ).toBeTruthy();
  });

  // Test Case 3: Check viewport meta tag
  test('TC3: Viewport meta tag - Page has viewport meta tag for responsive design', async ({ page }) => {
    // Get the viewport meta tag
    const viewport = await page.locator('meta[name="viewport"]');

    // Verify viewport meta tag exists
    await expect(viewport).toHaveCount(1);

    // Get the content attribute
    const content = await viewport.getAttribute('content');

    // Verify content exists and is not empty
    expect(content).toBeTruthy();

    // Verify viewport has essential properties for responsive design
    expect(content).toContain('width=device-width');
    expect(content).toContain('initial-scale=1');
  });

  // Test Case 4: Check Open Graph tags
  test('TC4: Open Graph tags - Page has og:title, og:description, og:type meta tags', async ({ page }) => {
    // Check og:title
    const ogTitle = await page.locator('meta[property="og:title"]');
    await expect(ogTitle).toHaveCount(1);
    const ogTitleContent = await ogTitle.getAttribute('content');
    expect(ogTitleContent).toBeTruthy();
    expect(ogTitleContent.length).toBeGreaterThan(0);

    // Check og:description
    const ogDescription = await page.locator('meta[property="og:description"]');
    await expect(ogDescription).toHaveCount(1);
    const ogDescriptionContent = await ogDescription.getAttribute('content');
    expect(ogDescriptionContent).toBeTruthy();
    expect(ogDescriptionContent.length).toBeGreaterThan(0);

    // Check og:type
    const ogType = await page.locator('meta[property="og:type"]');
    await expect(ogType).toHaveCount(1);
    const ogTypeContent = await ogType.getAttribute('content');
    expect(ogTypeContent).toBeTruthy();
    // Common og:type values: website, article, product
    expect(['website', 'article', 'product', 'software']).toContain(ogTypeContent);
  });

  // Test Case 5: Check canonical URL
  test('TC5: Canonical URL - Page has canonical link tag for SEO', async ({ page }) => {
    // Get the canonical link tag
    const canonical = await page.locator('link[rel="canonical"]');

    // Verify canonical link tag exists
    await expect(canonical).toHaveCount(1);

    // Get the href attribute
    const href = await canonical.getAttribute('href');

    // Verify href exists and is a valid URL format
    expect(href).toBeTruthy();
    expect(href.length).toBeGreaterThan(0);

    // Verify it's a valid URL (starts with https:// or http://)
    expect(href.startsWith('https://') || href.startsWith('http://')).toBeTruthy();
  });
});
