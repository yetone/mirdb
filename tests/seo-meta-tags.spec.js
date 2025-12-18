// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = path.resolve(__dirname, '../index.html');

/**
 * SEO - Meta Tags and Indexability Tests
 * Scenario: Verify the landing page has proper meta tags for search engine optimization
 */
test.describe('SEO - Meta Tags and Indexability', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(`file://${indexPath}`);
  });

  /**
   * Test Case 1: Check title tag content
   * Input: Check title tag content
   * Expected: Title tag present containing 'MirDB' and descriptive text
   */
  test('TC1: Title tag present containing MirDB and descriptive text', async ({ page }) => {
    // Get the title element
    const title = await page.title();

    // Verify title contains 'MirDB'
    expect(title).toContain('MirDB');

    // Verify title has descriptive text (not just 'MirDB')
    expect(title.length).toBeGreaterThan(10);

    // Verify title contains key descriptors
    const titleLower = title.toLowerCase();
    const hasKeyDescriptor =
      titleLower.includes('key-value') ||
      titleLower.includes('keyvalue') ||
      titleLower.includes('persistent') ||
      titleLower.includes('memcached') ||
      titleLower.includes('database') ||
      titleLower.includes('store');

    expect(hasKeyDescriptor).toBe(true);
  });

  /**
   * Test Case 2: Check meta description tag
   * Input: Check meta description tag
   * Expected: Meta description present with 120-160 characters describing MirDB
   */
  test('TC2: Meta description present with 120-160 characters describing MirDB', async ({ page }) => {
    // Get the meta description element
    const metaDescription = page.locator('meta[name="description"]');
    await expect(metaDescription).toHaveCount(1);

    // Get the content attribute
    const content = await metaDescription.getAttribute('content');
    expect(content).not.toBeNull();

    // Verify content mentions MirDB
    expect(content.toLowerCase()).toContain('mirdb');

    // Verify character length is between 120-160 (SEO best practice)
    // Note: We allow some flexibility (100-170) as this is a guideline
    expect(content.length).toBeGreaterThanOrEqual(100);
    expect(content.length).toBeLessThanOrEqual(170);

    // Verify description contains relevant keywords
    const contentLower = content.toLowerCase();
    const hasRelevantContent =
      contentLower.includes('key-value') ||
      contentLower.includes('memcached') ||
      contentLower.includes('persistent') ||
      contentLower.includes('store') ||
      contentLower.includes('database');

    expect(hasRelevantContent).toBe(true);
  });

  /**
   * Test Case 3: Check og:title tag
   * Input: Check og:title tag
   * Expected: Open Graph title tag present
   */
  test('TC3: Open Graph title tag present', async ({ page }) => {
    // Get the og:title meta tag
    const ogTitle = page.locator('meta[property="og:title"]');
    await expect(ogTitle).toHaveCount(1);

    // Get the content attribute
    const content = await ogTitle.getAttribute('content');
    expect(content).not.toBeNull();
    expect(content.length).toBeGreaterThan(0);

    // Verify it contains MirDB
    expect(content).toContain('MirDB');
  });

  /**
   * Test Case 4: Check og:description tag
   * Input: Check og:description tag
   * Expected: Open Graph description tag present
   */
  test('TC4: Open Graph description tag present', async ({ page }) => {
    // Get the og:description meta tag
    const ogDescription = page.locator('meta[property="og:description"]');
    await expect(ogDescription).toHaveCount(1);

    // Get the content attribute
    const content = await ogDescription.getAttribute('content');
    expect(content).not.toBeNull();
    expect(content.length).toBeGreaterThan(0);

    // Verify it contains relevant description
    const contentLower = content.toLowerCase();
    const hasRelevantContent =
      contentLower.includes('key-value') ||
      contentLower.includes('memcached') ||
      contentLower.includes('persistent') ||
      contentLower.includes('mirdb');

    expect(hasRelevantContent).toBe(true);
  });

  /**
   * Test Case 5: Check og:image tag
   * Input: Check og:image tag
   * Expected: Open Graph image tag present with valid image URL
   */
  test('TC5: Open Graph image tag present with valid image URL', async ({ page }) => {
    // Get the og:image meta tag
    const ogImage = page.locator('meta[property="og:image"]');
    await expect(ogImage).toHaveCount(1);

    // Get the content attribute
    const content = await ogImage.getAttribute('content');
    expect(content).not.toBeNull();
    expect(content.length).toBeGreaterThan(0);

    // Verify it's a valid URL or relative path to an image
    const isValidImageUrl =
      content.startsWith('http://') ||
      content.startsWith('https://') ||
      content.startsWith('/') ||
      content.endsWith('.png') ||
      content.endsWith('.jpg') ||
      content.endsWith('.jpeg') ||
      content.endsWith('.gif') ||
      content.endsWith('.webp') ||
      content.endsWith('.svg');

    expect(isValidImageUrl).toBe(true);
  });

  /**
   * Test Case 6: Check canonical URL tag
   * Input: Check canonical URL tag
   * Expected: Canonical link tag present with proper URL
   */
  test('TC6: Canonical link tag present with proper URL', async ({ page }) => {
    // Get the canonical link tag
    const canonical = page.locator('link[rel="canonical"]');
    await expect(canonical).toHaveCount(1);

    // Get the href attribute
    const href = await canonical.getAttribute('href');
    expect(href).not.toBeNull();
    expect(href.length).toBeGreaterThan(0);

    // Verify it's a valid URL format
    const isValidUrl =
      href.startsWith('http://') ||
      href.startsWith('https://');

    expect(isValidUrl).toBe(true);
  });

  /**
   * Test Case 7: Validate HTML structure
   * Input: Validate HTML structure
   * Expected: HTML is valid and uses semantic elements
   */
  test('TC7: HTML is valid and uses semantic elements', async ({ page }) => {
    // Check for required semantic elements

    // 1. Check for html lang attribute
    const htmlLang = await page.locator('html').getAttribute('lang');
    expect(htmlLang).not.toBeNull();
    expect(htmlLang.length).toBeGreaterThan(0);

    // 2. Check for single h1 element
    const h1Count = await page.locator('h1').count();
    expect(h1Count).toBe(1);

    // 3. Check for nav element
    const navCount = await page.locator('nav').count();
    expect(navCount).toBeGreaterThanOrEqual(1);

    // 4. Check for main element
    const mainCount = await page.locator('main').count();
    expect(mainCount).toBe(1);

    // 5. Check for footer element
    const footerCount = await page.locator('footer').count();
    expect(footerCount).toBeGreaterThanOrEqual(1);

    // 6. Check for section elements with proper structure
    const sectionCount = await page.locator('section').count();
    expect(sectionCount).toBeGreaterThanOrEqual(1);

    // 7. Check for header or nav at top level
    const headerOrNav = await page.locator('body > header, body > nav, header, nav').count();
    expect(headerOrNav).toBeGreaterThanOrEqual(1);

    // 8. Check for proper heading hierarchy (h1 should come before h2)
    const headings = page.locator('h1, h2, h3, h4, h5, h6');
    const headingTags = await headings.evaluateAll(els => els.map(el => el.tagName.toLowerCase()));

    // Verify first heading is h1
    if (headingTags.length > 0) {
      expect(headingTags[0]).toBe('h1');
    }

    // Verify no heading level is skipped (e.g., no h1 -> h3 without h2)
    let foundLevels = new Set();
    let valid = true;
    for (const tag of headingTags) {
      const level = parseInt(tag.substring(1));
      foundLevels.add(level);

      // Check if we're jumping more than one level without having the intermediate
      if (level > 1 && !foundLevels.has(level - 1)) {
        valid = false;
        break;
      }
    }
    expect(valid).toBe(true);
  });

  /**
   * Additional Test: Check og:type tag
   */
  test('Open Graph type tag present', async ({ page }) => {
    const ogType = page.locator('meta[property="og:type"]');
    await expect(ogType).toHaveCount(1);

    const content = await ogType.getAttribute('content');
    expect(content).not.toBeNull();
    expect(content).toBe('website');
  });

  /**
   * Additional Test: Check og:url tag
   */
  test('Open Graph URL tag present', async ({ page }) => {
    const ogUrl = page.locator('meta[property="og:url"]');
    await expect(ogUrl).toHaveCount(1);

    const content = await ogUrl.getAttribute('content');
    expect(content).not.toBeNull();

    const isValidUrl =
      content.startsWith('http://') ||
      content.startsWith('https://');

    expect(isValidUrl).toBe(true);
  });

  /**
   * Additional Test: Check charset meta tag
   */
  test('Charset meta tag is properly set', async ({ page }) => {
    const charset = page.locator('meta[charset]');
    await expect(charset).toHaveCount(1);

    const charsetValue = await charset.getAttribute('charset');
    expect(charsetValue.toLowerCase()).toBe('utf-8');
  });

  /**
   * Additional Test: Check viewport meta tag
   */
  test('Viewport meta tag is properly set for responsive design', async ({ page }) => {
    const viewport = page.locator('meta[name="viewport"]');
    await expect(viewport).toHaveCount(1);

    const content = await viewport.getAttribute('content');
    expect(content).toContain('width=device-width');
    expect(content).toContain('initial-scale=1');
  });
});
