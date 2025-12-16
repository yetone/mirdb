const { test, expect } = require('@playwright/test');

/**
 * SEO Requirements Test Suite
 *
 * This test suite verifies the homepage has proper SEO meta tags and structure
 * as specified in scenario 12: SEO Requirements
 */

test.describe('SEO Requirements', () => {

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Page has a descriptive title containing "MirDB"', async ({ page }) => {
    // Test Case 1: Check page title
    // Expected: Page has a descriptive title containing 'MirDB'

    const title = await page.title();

    // Title should exist and not be empty
    expect(title, 'Page should have a title').toBeTruthy();

    // Title should contain 'MirDB'
    expect(title.toLowerCase()).toContain('mirdb');

    // Title should be descriptive (more than just the product name)
    expect(title.length, 'Title should be descriptive').toBeGreaterThan(10);

    // Title should not be too long (SEO best practice: under 60 chars)
    expect(title.length, 'Title should not exceed 60 characters for optimal SEO').toBeLessThanOrEqual(60);
  });

  test('TC2: Meta description is present and describes the product', async ({ page }) => {
    // Test Case 2: Check meta description
    // Expected: Meta description is present and describes the product

    const metaDescription = await page.locator('meta[name="description"]');

    // Meta description should exist
    await expect(metaDescription, 'Meta description tag should exist').toHaveCount(1);

    const content = await metaDescription.getAttribute('content');

    // Content should not be empty
    expect(content, 'Meta description should have content').toBeTruthy();

    // Content should describe the product (contain relevant keywords)
    const lowerContent = content.toLowerCase();
    expect(lowerContent, 'Meta description should mention key-value store').toContain('key-value');

    // Meta description should be descriptive enough (best practice: 120-160 chars)
    expect(content.length, 'Meta description should be at least 50 characters').toBeGreaterThanOrEqual(50);
    expect(content.length, 'Meta description should not exceed 160 characters').toBeLessThanOrEqual(160);
  });

  test('TC3: OpenGraph tags (og:title, og:description, og:image) are present', async ({ page }) => {
    // Test Case 3: Check OpenGraph tags
    // Expected: og:title, og:description, og:image are present

    // Check og:title
    const ogTitle = await page.locator('meta[property="og:title"]');
    await expect(ogTitle, 'og:title meta tag should exist').toHaveCount(1);
    const ogTitleContent = await ogTitle.getAttribute('content');
    expect(ogTitleContent, 'og:title should have content').toBeTruthy();
    expect(ogTitleContent.length, 'og:title should have meaningful content').toBeGreaterThan(5);

    // Check og:description
    const ogDescription = await page.locator('meta[property="og:description"]');
    await expect(ogDescription, 'og:description meta tag should exist').toHaveCount(1);
    const ogDescContent = await ogDescription.getAttribute('content');
    expect(ogDescContent, 'og:description should have content').toBeTruthy();
    expect(ogDescContent.length, 'og:description should have meaningful content').toBeGreaterThan(20);

    // Check og:image
    const ogImage = await page.locator('meta[property="og:image"]');
    await expect(ogImage, 'og:image meta tag should exist').toHaveCount(1);
    const ogImageContent = await ogImage.getAttribute('content');
    expect(ogImageContent, 'og:image should have content').toBeTruthy();
    // Image URL should be a valid URL or path
    expect(ogImageContent).toMatch(/^(https?:\/\/|\/)/);
  });

  test('TC4: Canonical URL is specified', async ({ page }) => {
    // Test Case 4: Check canonical URL
    // Expected: Canonical URL is specified

    const canonical = await page.locator('link[rel="canonical"]');

    // Canonical link should exist
    await expect(canonical, 'Canonical link should exist').toHaveCount(1);

    const href = await canonical.getAttribute('href');

    // Canonical URL should have a value
    expect(href, 'Canonical URL should have href').toBeTruthy();

    // Canonical URL should be a valid URL
    expect(href).toMatch(/^https?:\/\//);
  });

  test('TC5: Page has exactly one h1 tag', async ({ page }) => {
    // Test Case 5: Verify single h1 tag
    // Expected: Page has exactly one h1 tag

    const h1Tags = await page.locator('h1');

    // There should be exactly one h1 tag
    await expect(h1Tags, 'Page should have exactly one h1 tag').toHaveCount(1);

    // The h1 should have content
    const h1Text = await h1Tags.textContent();
    expect(h1Text.trim(), 'h1 should have content').toBeTruthy();

    // The h1 should contain the product name
    expect(h1Text.toLowerCase()).toContain('mirdb');
  });

  test('TC6: Page allows indexing (no noindex)', async ({ page }) => {
    // Test Case 6: Check for robots meta tag
    // Expected: Page allows indexing (no noindex)

    // Check if there's a robots meta tag
    const robotsMeta = await page.locator('meta[name="robots"]');
    const robotsCount = await robotsMeta.count();

    if (robotsCount > 0) {
      // If robots meta exists, it should not contain 'noindex'
      const robotsContent = await robotsMeta.getAttribute('content');
      expect(robotsContent.toLowerCase(), 'Robots meta should not contain noindex').not.toContain('noindex');
    }
    // If no robots meta tag exists, indexing is allowed by default (pass)

    // Also check X-Robots-Tag meta (less common but should also be checked)
    const xRobotsMeta = await page.locator('meta[http-equiv="X-Robots-Tag"]');
    const xRobotsCount = await xRobotsMeta.count();

    if (xRobotsCount > 0) {
      const xRobotsContent = await xRobotsMeta.getAttribute('content');
      expect(xRobotsContent.toLowerCase(), 'X-Robots-Tag should not contain noindex').not.toContain('noindex');
    }
  });

  // Additional SEO best practices tests
  test.describe('Additional SEO Best Practices', () => {

    test('Page has proper HTML lang attribute', async ({ page }) => {
      const html = await page.locator('html');
      const lang = await html.getAttribute('lang');
      expect(lang, 'HTML should have lang attribute').toBeTruthy();
      expect(lang).toBe('en');
    });

    test('Page has viewport meta tag for mobile', async ({ page }) => {
      const viewport = await page.locator('meta[name="viewport"]');
      await expect(viewport, 'Viewport meta tag should exist').toHaveCount(1);
      const content = await viewport.getAttribute('content');
      expect(content).toContain('width=device-width');
    });

    test('Page has charset meta tag', async ({ page }) => {
      const charset = await page.locator('meta[charset]');
      await expect(charset, 'Charset meta tag should exist').toHaveCount(1);
      const charsetValue = await charset.getAttribute('charset');
      expect(charsetValue.toLowerCase()).toBe('utf-8');
    });

    test('OpenGraph type is specified', async ({ page }) => {
      const ogType = await page.locator('meta[property="og:type"]');
      await expect(ogType, 'og:type meta tag should exist').toHaveCount(1);
      const content = await ogType.getAttribute('content');
      expect(content, 'og:type should have content').toBeTruthy();
    });

    test('OpenGraph URL is specified', async ({ page }) => {
      const ogUrl = await page.locator('meta[property="og:url"]');
      await expect(ogUrl, 'og:url meta tag should exist').toHaveCount(1);
      const content = await ogUrl.getAttribute('content');
      expect(content, 'og:url should have content').toBeTruthy();
      expect(content).toMatch(/^https?:\/\//);
    });
  });
});
