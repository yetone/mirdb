// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * SEO and Meta Tags Test Suite
 * Verifies proper SEO meta tags and semantic HTML structure
 */

test.describe('SEO and Meta Tags', () => {

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  /**
   * Test Case 1: Check page title tag
   * Expected: Title contains 'MirDB' and describes the product
   */
  test('TC1: Page title contains MirDB and describes the product', async ({ page }) => {
    const title = await page.title();

    // Title should contain 'MirDB'
    expect(title.toLowerCase()).toContain('mirdb');

    // Title should be descriptive (more than just the product name)
    expect(title.length).toBeGreaterThan(10);

    // Title should describe the product
    expect(title.toLowerCase()).toMatch(/key-value|persistent|memcached/);

    // Verify the title tag exists in HTML
    const titleTag = await page.locator('head title');
    await expect(titleTag).toHaveCount(1);

    // Title should be reasonable length for SEO (under 60 chars is optimal)
    expect(title.length).toBeLessThan(100);
  });

  /**
   * Test Case 2: Check meta description
   * Expected: Description meta tag present with relevant content
   */
  test('TC2: Meta description tag is present with relevant content', async ({ page }) => {
    // Get meta description
    const metaDescription = await page.locator('meta[name="description"]');

    // Meta description should exist
    await expect(metaDescription).toHaveCount(1);

    // Get the content attribute
    const content = await metaDescription.getAttribute('content');

    // Description should not be empty
    expect(content).toBeTruthy();
    expect(content.length).toBeGreaterThan(50);

    // Description should be reasonable length for SEO (under 160 chars is optimal)
    expect(content.length).toBeLessThan(200);

    // Description should contain relevant keywords
    expect(content.toLowerCase()).toMatch(/mirdb|key-value|memcached|persistent/);

    // Description should be descriptive of the product
    expect(content.toLowerCase()).toMatch(/store|database|storage/);
  });

  /**
   * Test Case 3: Check Open Graph tags
   * Expected: og:title, og:description, og:image tags present for social sharing
   */
  test('TC3: Open Graph tags are present for social sharing', async ({ page }) => {
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
    expect(ogDescContent.length).toBeGreaterThan(20);

    // Check og:image
    const ogImage = await page.locator('meta[property="og:image"]');
    await expect(ogImage).toHaveCount(1);
    const ogImageContent = await ogImage.getAttribute('content');
    expect(ogImageContent).toBeTruthy();
    // Image URL should be absolute or relative path
    expect(ogImageContent).toMatch(/\.(png|jpg|jpeg|gif|webp|svg)$/i);

    // Additional OG tags that are recommended
    // Check og:type
    const ogType = await page.locator('meta[property="og:type"]');
    await expect(ogType).toHaveCount(1);
    const ogTypeContent = await ogType.getAttribute('content');
    expect(ogTypeContent).toBe('website');

    // Check og:url (optional but recommended)
    const ogUrl = await page.locator('meta[property="og:url"]');
    if (await ogUrl.count() > 0) {
      const ogUrlContent = await ogUrl.getAttribute('content');
      expect(ogUrlContent).toBeTruthy();
    }
  });

  /**
   * Test Case 4: Check semantic HTML structure
   * Expected: Page uses proper semantic elements (header, main, nav, section, article, footer)
   */
  test('TC4: Page uses proper semantic HTML structure', async ({ page }) => {
    // Check for header element
    const header = await page.locator('header');
    await expect(header).toHaveCount(1);

    // Check for main element
    const main = await page.locator('main');
    await expect(main).toHaveCount(1);

    // Check for nav element (navigation)
    const nav = await page.locator('nav');
    expect(await nav.count()).toBeGreaterThanOrEqual(1);

    // Check for section elements (multiple sections expected)
    const sections = await page.locator('section');
    expect(await sections.count()).toBeGreaterThan(1);

    // Check for footer element
    const footer = await page.locator('footer');
    await expect(footer).toHaveCount(1);

    // Verify semantic structure order in the DOM
    // Note: header may be fixed positioned at the top, so we check DOM order instead
    const semanticOrder = await page.evaluate(() => {
      const body = document.body;
      const children = Array.from(body.children);
      const headerIndex = children.findIndex(el => el.tagName === 'HEADER');
      const mainIndex = children.findIndex(el => el.tagName === 'MAIN');
      const footerIndex = children.findIndex(el => el.tagName === 'FOOTER');
      return { headerIndex, mainIndex, footerIndex };
    });

    // Header should come before main in DOM
    expect(semanticOrder.headerIndex).toBeLessThan(semanticOrder.mainIndex);

    // Main should come before footer in DOM
    expect(semanticOrder.mainIndex).toBeLessThan(semanticOrder.footerIndex);

    // All elements should be direct children of body
    expect(semanticOrder.headerIndex).toBeGreaterThanOrEqual(0);
    expect(semanticOrder.mainIndex).toBeGreaterThanOrEqual(0);
    expect(semanticOrder.footerIndex).toBeGreaterThanOrEqual(0);

    // Verify sections are inside main
    const sectionsInMain = await main.locator('section').count();
    expect(sectionsInMain).toBeGreaterThan(0);
  });

  /**
   * Additional SEO checks
   */
  test('Viewport meta tag is present and correctly configured', async ({ page }) => {
    const viewport = await page.locator('meta[name="viewport"]');
    await expect(viewport).toHaveCount(1);

    const content = await viewport.getAttribute('content');
    expect(content).toContain('width=device-width');
    expect(content).toContain('initial-scale=1');
  });

  test('HTML lang attribute is set', async ({ page }) => {
    const htmlLang = await page.locator('html').getAttribute('lang');
    expect(htmlLang).toBe('en');
  });

  test('Charset meta tag is present', async ({ page }) => {
    const charset = await page.locator('meta[charset]');
    await expect(charset).toHaveCount(1);

    const charsetValue = await charset.getAttribute('charset');
    expect(charsetValue.toLowerCase()).toBe('utf-8');
  });

  test('Canonical URL is optionally present and valid', async ({ page }) => {
    const canonical = await page.locator('link[rel="canonical"]');

    // Canonical is optional but recommended
    if (await canonical.count() > 0) {
      const href = await canonical.getAttribute('href');
      expect(href).toBeTruthy();
      // Should be a valid URL pattern
      expect(href).toMatch(/^https?:\/\//);
    }
  });

  test('Twitter card meta tags are optionally present', async ({ page }) => {
    const twitterCard = await page.locator('meta[name="twitter:card"]');

    // Twitter cards are optional but good for social sharing
    if (await twitterCard.count() > 0) {
      const content = await twitterCard.getAttribute('content');
      expect(['summary', 'summary_large_image']).toContain(content);
    }
  });
});
