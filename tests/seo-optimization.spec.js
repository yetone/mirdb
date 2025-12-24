// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * SEO Optimization Tests (NFR-6)
 * Verifies the page has proper SEO meta tags and semantic HTML
 */

test.describe('SEO Optimization - NFR-6', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  /**
   * Test Case 1: Check for title meta tag
   * Input: Check for title meta tag
   * Expected: Page has descriptive title tag containing 'MirDB'
   */
  test('TC1: Page has descriptive title tag containing MirDB', async ({ page }) => {
    // Get the page title
    const title = await page.title();

    // Verify title exists and is not empty
    expect(title).toBeTruthy();
    expect(title.length).toBeGreaterThan(0);

    // Verify title contains 'MirDB'
    expect(title.toLowerCase()).toContain('mirdb');

    // Verify title is descriptive (more than just 'MirDB')
    expect(title.length).toBeGreaterThan(5);

    // Log the title for debugging
    console.log(`Page title: "${title}"`);
  });

  /**
   * Test Case 2: Check for meta description
   * Input: Check for meta description
   * Expected: Page has meta description describing MirDB's value proposition
   */
  test('TC2: Page has meta description describing MirDB value proposition', async ({ page }) => {
    // Get the meta description
    const metaDescription = await page.locator('meta[name="description"]').getAttribute('content');

    // Verify meta description exists
    expect(metaDescription).toBeTruthy();

    // Verify meta description is not empty
    expect(metaDescription.length).toBeGreaterThan(0);

    // Verify meta description mentions key value propositions
    const description = metaDescription.toLowerCase();
    const hasRelevantContent =
      description.includes('mirdb') ||
      description.includes('key-value') ||
      description.includes('memcached') ||
      description.includes('persistent');

    expect(hasRelevantContent).toBe(true);

    // Verify meta description is of appropriate length (50-160 characters is recommended)
    expect(metaDescription.length).toBeGreaterThanOrEqual(50);
    expect(metaDescription.length).toBeLessThanOrEqual(200);

    // Log the description for debugging
    console.log(`Meta description: "${metaDescription}"`);
  });

  /**
   * Test Case 3: Check for viewport meta tag
   * Input: Check for viewport meta tag
   * Expected: Page has proper viewport meta tag for responsive design
   */
  test('TC3: Page has proper viewport meta tag for responsive design', async ({ page }) => {
    // Get the viewport meta tag
    const viewportMeta = await page.locator('meta[name="viewport"]').getAttribute('content');

    // Verify viewport meta tag exists
    expect(viewportMeta).toBeTruthy();

    // Verify viewport contains essential settings for responsive design
    expect(viewportMeta).toContain('width=device-width');
    expect(viewportMeta).toContain('initial-scale=1');

    // Log the viewport settings for debugging
    console.log(`Viewport meta: "${viewportMeta}"`);
  });

  /**
   * Test Case 4: Check for Open Graph tags
   * Input: Check for Open Graph tags
   * Expected: Page has og:title, og:description, and og:type meta tags
   */
  test('TC4: Page has Open Graph meta tags (og:title, og:description, og:type)', async ({ page }) => {
    // Check for og:title
    const ogTitle = await page.locator('meta[property="og:title"]').getAttribute('content');
    expect(ogTitle).toBeTruthy();
    expect(ogTitle.length).toBeGreaterThan(0);
    console.log(`og:title: "${ogTitle}"`);

    // Check for og:description
    const ogDescription = await page.locator('meta[property="og:description"]').getAttribute('content');
    expect(ogDescription).toBeTruthy();
    expect(ogDescription.length).toBeGreaterThan(0);
    console.log(`og:description: "${ogDescription}"`);

    // Check for og:type
    const ogType = await page.locator('meta[property="og:type"]').getAttribute('content');
    expect(ogType).toBeTruthy();
    expect(ogType.length).toBeGreaterThan(0);
    // Common types: website, article, product
    expect(['website', 'article', 'product']).toContain(ogType);
    console.log(`og:type: "${ogType}"`);
  });

  /**
   * Test Case 5: Check for single h1 element
   * Input: Check for single h1 element
   * Expected: Page has exactly one h1 element
   */
  test('TC5: Page has exactly one h1 element', async ({ page }) => {
    // Count h1 elements
    const h1Count = await page.locator('h1').count();

    // Verify exactly one h1 exists
    expect(h1Count).toBe(1);

    // Get the h1 text for verification
    const h1Text = await page.locator('h1').textContent();
    expect(h1Text).toBeTruthy();
    expect(h1Text.trim().length).toBeGreaterThan(0);

    // Log the h1 content for debugging
    console.log(`H1 content: "${h1Text.trim()}"`);
  });

  /**
   * Test Case 6: Check for semantic landmark elements
   * Input: Check for semantic landmark elements
   * Expected: Page uses semantic HTML5 landmark elements
   */
  test('TC6: Page uses semantic HTML5 landmark elements', async ({ page }) => {
    // Check for semantic landmark elements
    const semanticElements = await page.evaluate(() => {
      return {
        header: document.querySelectorAll('header').length,
        nav: document.querySelectorAll('nav').length,
        main: document.querySelectorAll('main').length,
        section: document.querySelectorAll('section').length,
        article: document.querySelectorAll('article').length,
        footer: document.querySelectorAll('footer').length
      };
    });

    console.log('Semantic elements found:', semanticElements);

    // Verify essential landmark elements are present
    // Navigation element is required
    expect(semanticElements.nav).toBeGreaterThanOrEqual(1);

    // Footer element is required
    expect(semanticElements.footer).toBeGreaterThanOrEqual(1);

    // At least one section or article for content structure
    const hasContentStructure = semanticElements.section > 0 || semanticElements.article > 0;
    expect(hasContentStructure).toBe(true);

    // Count total semantic elements used
    const totalSemanticElements =
      semanticElements.header +
      semanticElements.nav +
      semanticElements.main +
      semanticElements.section +
      semanticElements.article +
      semanticElements.footer;

    // Page should use multiple semantic elements for good structure
    expect(totalSemanticElements).toBeGreaterThanOrEqual(3);
  });

  /**
   * Additional Test: Check charset meta tag
   * Ensures proper character encoding is specified
   */
  test('TC-Additional: Page has charset meta tag', async ({ page }) => {
    // Check for charset meta tag
    const charsetMeta = await page.locator('meta[charset]').getAttribute('charset');

    // If charset attribute exists, verify it's UTF-8
    if (charsetMeta) {
      expect(charsetMeta.toLowerCase()).toBe('utf-8');
    } else {
      // Alternatively check for http-equiv content-type
      const contentTypeMeta = await page.locator('meta[http-equiv="Content-Type"]').getAttribute('content');
      expect(contentTypeMeta).toBeTruthy();
      expect(contentTypeMeta.toLowerCase()).toContain('utf-8');
    }
  });

  /**
   * Additional Test: Check for language attribute
   * Ensures proper language declaration for SEO
   */
  test('TC-Additional: HTML element has lang attribute', async ({ page }) => {
    // Get the lang attribute from html element
    const htmlLang = await page.locator('html').getAttribute('lang');

    // Verify lang attribute exists and is valid
    expect(htmlLang).toBeTruthy();
    expect(htmlLang.length).toBeGreaterThanOrEqual(2);

    // Common valid language codes
    const validLangCodes = ['en', 'en-US', 'en-GB', 'en-AU'];
    expect(validLangCodes).toContain(htmlLang);

    console.log(`HTML lang attribute: "${htmlLang}"`);
  });
});
