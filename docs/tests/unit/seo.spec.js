// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Unit tests for SEO
 * Owner: Scenario 10 - Performance and SEO
 *
 * Test cases:
 * - HTML title tag content
 * - Meta description presence
 * - OpenGraph tags
 * - Semantic HTML structure
 * - Robots meta tag
 */

test.describe('SEO Meta Tags', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('');
  });

  test('HTML title tag contains MirDB and describes the product', async ({ page }) => {
    const title = await page.title();

    // Title should contain 'MirDB'
    expect(title.toLowerCase()).toContain('mirdb');

    // Title should describe the product - should mention key concepts
    const titleLower = title.toLowerCase();
    const hasProductDescription =
      titleLower.includes('key-value') ||
      titleLower.includes('memcached') ||
      titleLower.includes('persistent') ||
      titleLower.includes('store');
    expect(hasProductDescription).toBeTruthy();
  });

  test('Meta description exists and describes MirDB as persistent key-value store', async ({ page }) => {
    const metaDescription = await page.locator('meta[name="description"]');
    await expect(metaDescription).toHaveCount(1);

    const content = await metaDescription.getAttribute('content');
    expect(content).toBeTruthy();

    // Description should mention key aspects of MirDB
    const contentLower = content.toLowerCase();
    expect(contentLower).toContain('mirdb');

    const describesPersistentStore =
      contentLower.includes('persistent') ||
      contentLower.includes('key-value') ||
      contentLower.includes('store');
    expect(describesPersistentStore).toBeTruthy();
  });

  test('OpenGraph tags are present (og:title, og:description, og:image)', async ({ page }) => {
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

    // Check og:image
    const ogImage = await page.locator('meta[property="og:image"]');
    await expect(ogImage).toHaveCount(1);
    const ogImageContent = await ogImage.getAttribute('content');
    expect(ogImageContent).toBeTruthy();
    // Image should have a valid URL or path
    expect(ogImageContent.length).toBeGreaterThan(0);
  });

  test('Page uses semantic HTML structure (header, main, section, nav, footer)', async ({ page }) => {
    // Check for header element
    const header = page.locator('header');
    await expect(header).toHaveCount(1);

    // Check for main element
    const main = page.locator('main');
    await expect(main).toHaveCount(1);

    // Check for nav element (should be within header)
    const nav = page.locator('nav');
    const navCount = await nav.count();
    expect(navCount).toBeGreaterThanOrEqual(1);

    // Check for footer element
    const footer = page.locator('footer');
    await expect(footer).toHaveCount(1);

    // Check for section elements (multiple sections expected)
    const sections = page.locator('section');
    const sectionCount = await sections.count();
    expect(sectionCount).toBeGreaterThanOrEqual(1);
  });

  test('Page is indexable (no noindex robots directive)', async ({ page }) => {
    // Check for robots meta tag
    const robotsMeta = await page.locator('meta[name="robots"]');
    const robotsCount = await robotsMeta.count();

    if (robotsCount > 0) {
      const content = await robotsMeta.getAttribute('content');
      // If robots meta exists, it should NOT contain 'noindex'
      expect(content.toLowerCase()).not.toContain('noindex');
    }
    // If no robots meta exists, page is indexable by default (pass)
  });

  test('Canonical URL is present', async ({ page }) => {
    const canonical = await page.locator('link[rel="canonical"]');
    await expect(canonical).toHaveCount(1);

    const href = await canonical.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href.length).toBeGreaterThan(0);
  });

  test('OpenGraph type is set to website', async ({ page }) => {
    const ogType = await page.locator('meta[property="og:type"]');
    await expect(ogType).toHaveCount(1);

    const content = await ogType.getAttribute('content');
    expect(content).toBe('website');
  });

  test('OpenGraph URL is present', async ({ page }) => {
    const ogUrl = await page.locator('meta[property="og:url"]');
    await expect(ogUrl).toHaveCount(1);

    const content = await ogUrl.getAttribute('content');
    expect(content).toBeTruthy();
  });

  test('Charset is UTF-8', async ({ page }) => {
    const charset = await page.locator('meta[charset]');
    await expect(charset).toHaveCount(1);

    const charsetValue = await charset.getAttribute('charset');
    expect(charsetValue.toUpperCase()).toBe('UTF-8');
  });

  test('Viewport meta tag is properly set', async ({ page }) => {
    const viewport = await page.locator('meta[name="viewport"]');
    await expect(viewport).toHaveCount(1);

    const content = await viewport.getAttribute('content');
    expect(content).toContain('width=device-width');
    expect(content).toContain('initial-scale=1');
  });

  test('Favicon is linked', async ({ page }) => {
    const favicon = await page.locator('link[rel="icon"], link[rel="shortcut icon"]');
    const count = await favicon.count();
    expect(count).toBeGreaterThanOrEqual(1);
  });
});
