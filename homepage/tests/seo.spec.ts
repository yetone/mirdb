import { test, expect } from '@playwright/test';

test.describe('SEO Optimization', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Page title contains MirDB and relevant keywords', async ({ page }) => {
    // Inspect page title tag
    // Expected: Title contains 'MirDB' and relevant keywords

    const title = await page.title();

    // Title should contain MirDB
    expect(title.toLowerCase()).toContain('mirdb');

    // Title should contain relevant keywords related to the product
    const titleLower = title.toLowerCase();
    const hasKeyValueKeyword = titleLower.includes('key-value') || titleLower.includes('key value');
    const hasDatabaseKeyword = titleLower.includes('database') || titleLower.includes('store');

    expect(hasKeyValueKeyword || hasDatabaseKeyword).toBeTruthy();
  });

  test('TC2: Meta description mentions key-value database, Rust, memcached compatibility', async ({ page }) => {
    // Inspect meta description
    // Expected: Description mentions key-value database, Rust, memcached compatibility

    const metaDescription = page.locator('meta[name="description"]');
    await expect(metaDescription).toHaveCount(1);

    const content = await metaDescription.getAttribute('content');
    expect(content).not.toBeNull();

    const contentLower = content!.toLowerCase();

    // Check for key-value database reference
    const hasKeyValue = contentLower.includes('key-value') || contentLower.includes('key value');
    expect(hasKeyValue).toBeTruthy();

    // Check for Rust reference
    expect(contentLower).toContain('rust');

    // Check for memcached compatibility reference
    expect(contentLower).toContain('memcached');
  });

  test('TC3: Open Graph og:title is present and descriptive', async ({ page }) => {
    // Check Open Graph og:title
    // Expected: OG title is present and descriptive

    const ogTitle = page.locator('meta[property="og:title"]');
    await expect(ogTitle).toHaveCount(1);

    const content = await ogTitle.getAttribute('content');
    expect(content).not.toBeNull();
    expect(content!.length).toBeGreaterThan(10);

    // OG title should contain MirDB
    expect(content!.toLowerCase()).toContain('mirdb');
  });

  test('TC4: Open Graph og:description is present and compelling', async ({ page }) => {
    // Check Open Graph og:description
    // Expected: OG description is present and compelling

    const ogDescription = page.locator('meta[property="og:description"]');
    await expect(ogDescription).toHaveCount(1);

    const content = await ogDescription.getAttribute('content');
    expect(content).not.toBeNull();

    // Description should be substantive (at least 50 characters)
    expect(content!.length).toBeGreaterThan(50);

    // Should mention key features
    const contentLower = content!.toLowerCase();
    const hasRelevantContent =
      contentLower.includes('key-value') ||
      contentLower.includes('database') ||
      contentLower.includes('memcached') ||
      contentLower.includes('rust');
    expect(hasRelevantContent).toBeTruthy();
  });

  test('TC5: Open Graph og:image is present for social sharing preview', async ({ page }) => {
    // Check Open Graph og:image
    // Expected: OG image is present for social sharing preview

    const ogImage = page.locator('meta[property="og:image"]');
    await expect(ogImage).toHaveCount(1);

    const content = await ogImage.getAttribute('content');
    expect(content).not.toBeNull();

    // Image URL should be a valid URL or path
    expect(content!.length).toBeGreaterThan(0);

    // Should be an absolute URL or start with / for relative paths
    const isValidPath = content!.startsWith('http') || content!.startsWith('/');
    expect(isValidPath).toBeTruthy();
  });

  test('TC6: Canonical URL link element is present', async ({ page }) => {
    // Verify canonical URL
    // Expected: Canonical link element is present

    const canonicalLink = page.locator('link[rel="canonical"]');
    await expect(canonicalLink).toHaveCount(1);

    const href = await canonicalLink.getAttribute('href');
    expect(href).not.toBeNull();

    // Canonical URL should be a valid URL
    expect(href!.length).toBeGreaterThan(0);
  });

  test('og:type meta tag is present', async ({ page }) => {
    // Additional SEO test for og:type
    const ogType = page.locator('meta[property="og:type"]');
    await expect(ogType).toHaveCount(1);

    const content = await ogType.getAttribute('content');
    expect(content).not.toBeNull();
    expect(content).toBe('website');
  });

  test('og:url meta tag is present', async ({ page }) => {
    // Additional SEO test for og:url
    const ogUrl = page.locator('meta[property="og:url"]');
    await expect(ogUrl).toHaveCount(1);

    const content = await ogUrl.getAttribute('content');
    expect(content).not.toBeNull();
    expect(content!.length).toBeGreaterThan(0);
  });

  test('Semantic HTML elements are used correctly', async ({ page }) => {
    // Verify proper use of semantic HTML elements
    // Should have header, main, section, footer elements

    // Check for header (navigation area)
    const header = page.locator('header, nav');
    await expect(header.first()).toBeVisible();

    // Check for main content area
    const main = page.locator('main');
    await expect(main).toHaveCount(1);
    await expect(main).toBeVisible();

    // Check for footer
    const footer = page.locator('footer');
    await expect(footer).toHaveCount(1);
    await expect(footer).toBeVisible();

    // Check for section elements (homepage should have multiple sections)
    const sections = page.locator('main section, main [data-testid*="section"]');
    const sectionCount = await sections.count();
    expect(sectionCount).toBeGreaterThan(0);
  });

  test('HTML lang attribute is set', async ({ page }) => {
    // Verify html element has lang attribute for accessibility and SEO
    const html = page.locator('html');
    await expect(html).toHaveAttribute('lang', 'en');
  });

  test('Viewport meta tag is properly configured', async ({ page }) => {
    // Verify viewport meta tag for responsive design
    const viewport = page.locator('meta[name="viewport"]');
    await expect(viewport).toHaveCount(1);

    const content = await viewport.getAttribute('content');
    expect(content).toContain('width=device-width');
  });

  test('Charset is properly set to UTF-8', async ({ page }) => {
    // Verify charset for proper character encoding
    const charset = page.locator('meta[charset]');
    await expect(charset).toHaveCount(1);

    const charsetValue = await charset.getAttribute('charset');
    expect(charsetValue?.toUpperCase()).toBe('UTF-8');
  });
});
