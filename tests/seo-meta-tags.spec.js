const { test, expect } = require('@playwright/test');

/**
 * SEO - Meta Tags Tests
 * Scenario: Validate the page has proper meta tags for search engine optimization
 *
 * This test suite verifies:
 * 1. Title tag contains 'MirDB' and relevant keywords
 * 2. Meta description under 160 characters describing MirDB
 * 3. Viewport meta tag for responsive design
 * 4. Open Graph title meta tag
 */

test.describe('SEO - Meta Tags', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Page has title tag containing MirDB and relevant keywords', async ({ page }) => {
    // Get the page title
    const title = await page.title();

    // Title should exist and not be empty
    expect(title).toBeTruthy();
    expect(title.trim().length).toBeGreaterThan(0);

    // Title should contain 'MirDB'
    expect(title.toLowerCase()).toContain('mirdb');

    // Title should contain relevant keywords (at least one of these)
    const relevantKeywords = ['key-value', 'persistent', 'memcached', 'store', 'database'];
    const titleLower = title.toLowerCase();
    const hasRelevantKeyword = relevantKeywords.some(keyword => titleLower.includes(keyword));
    expect(hasRelevantKeyword).toBe(true);

    // Title should be a reasonable length for SEO (typically 50-60 characters max)
    expect(title.length).toBeLessThanOrEqual(70);

    console.log(`Page title: "${title}" (${title.length} characters)`);
  });

  test('Test Case 2: Page has meta description under 160 characters describing MirDB', async ({ page }) => {
    // Get the meta description tag
    const metaDescription = page.locator('meta[name="description"]');
    const descCount = await metaDescription.count();

    // Meta description should exist
    expect(descCount).toBe(1);

    // Get the content attribute
    const description = await metaDescription.getAttribute('content');

    // Description should exist and not be empty
    expect(description).toBeTruthy();
    expect(description.trim().length).toBeGreaterThan(0);

    // Description should be under 160 characters for optimal SEO
    expect(description.length).toBeLessThanOrEqual(160);

    // Description should mention MirDB or describe its purpose
    const descLower = description.toLowerCase();
    const describesMirDB = descLower.includes('mirdb') ||
      (descLower.includes('key-value') && descLower.includes('persistent')) ||
      (descLower.includes('memcached') && descLower.includes('persistent'));
    expect(describesMirDB).toBe(true);

    // Description should be meaningful (minimum length)
    expect(description.length).toBeGreaterThan(50);

    console.log(`Meta description: "${description}" (${description.length} characters)`);
  });

  test('Test Case 3: Page has viewport meta tag for responsive design', async ({ page }) => {
    // Get the viewport meta tag
    const viewportMeta = page.locator('meta[name="viewport"]');
    const viewportCount = await viewportMeta.count();

    // Viewport meta tag should exist
    expect(viewportCount).toBe(1);

    // Get the content attribute
    const viewportContent = await viewportMeta.getAttribute('content');

    // Viewport content should exist
    expect(viewportContent).toBeTruthy();

    // Viewport should include width=device-width for responsive design
    expect(viewportContent.toLowerCase()).toContain('width=device-width');

    // Viewport should include initial-scale
    expect(viewportContent.toLowerCase()).toContain('initial-scale');

    console.log(`Viewport meta tag content: "${viewportContent}"`);
  });

  test('Test Case 4: Page has og:title meta tag', async ({ page }) => {
    // Get the Open Graph title meta tag
    const ogTitle = page.locator('meta[property="og:title"]');
    const ogTitleCount = await ogTitle.count();

    // og:title meta tag should exist
    expect(ogTitleCount).toBe(1);

    // Get the content attribute
    const ogTitleContent = await ogTitle.getAttribute('content');

    // og:title content should exist and not be empty
    expect(ogTitleContent).toBeTruthy();
    expect(ogTitleContent.trim().length).toBeGreaterThan(0);

    // og:title should contain MirDB
    expect(ogTitleContent.toLowerCase()).toContain('mirdb');

    console.log(`og:title meta tag content: "${ogTitleContent}"`);
  });

  test('Verify og:description meta tag exists', async ({ page }) => {
    // Get the Open Graph description meta tag
    const ogDesc = page.locator('meta[property="og:description"]');
    const ogDescCount = await ogDesc.count();

    // og:description meta tag should exist
    expect(ogDescCount).toBe(1);

    // Get the content attribute
    const ogDescContent = await ogDesc.getAttribute('content');

    // og:description content should exist and not be empty
    expect(ogDescContent).toBeTruthy();
    expect(ogDescContent.trim().length).toBeGreaterThan(0);

    console.log(`og:description meta tag content: "${ogDescContent}"`);
  });

  test('Verify og:image meta tag exists', async ({ page }) => {
    // Get the Open Graph image meta tag
    const ogImage = page.locator('meta[property="og:image"]');
    const ogImageCount = await ogImage.count();

    // og:image meta tag should exist
    expect(ogImageCount).toBe(1);

    // Get the content attribute
    const ogImageContent = await ogImage.getAttribute('content');

    // og:image content should exist and not be empty
    expect(ogImageContent).toBeTruthy();
    expect(ogImageContent.trim().length).toBeGreaterThan(0);

    console.log(`og:image meta tag content: "${ogImageContent}"`);
  });

  test('Verify og:type meta tag exists', async ({ page }) => {
    // Get the Open Graph type meta tag
    const ogType = page.locator('meta[property="og:type"]');
    const ogTypeCount = await ogType.count();

    // og:type meta tag should exist
    expect(ogTypeCount).toBe(1);

    // Get the content attribute
    const ogTypeContent = await ogType.getAttribute('content');

    // og:type content should exist and not be empty
    expect(ogTypeContent).toBeTruthy();
    expect(ogTypeContent.trim().length).toBeGreaterThan(0);

    console.log(`og:type meta tag content: "${ogTypeContent}"`);
  });

  test('Verify charset meta tag exists', async ({ page }) => {
    // Get the charset meta tag
    const charsetMeta = page.locator('meta[charset]');
    const charsetCount = await charsetMeta.count();

    // charset meta tag should exist
    expect(charsetCount).toBe(1);

    // Get the charset attribute
    const charsetValue = await charsetMeta.getAttribute('charset');

    // charset should be UTF-8
    expect(charsetValue.toLowerCase()).toBe('utf-8');

    console.log(`Charset meta tag: "${charsetValue}"`);
  });
});
