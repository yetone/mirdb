/**
 * SEO and Semantic HTML Tests
 *
 * This test suite verifies that the MirDB landing page has proper SEO meta tags
 * and uses semantic HTML5 elements appropriately.
 *
 * Requirements covered:
 * - NFR-4: HTML must be semantic and SEO-optimized
 * - Page should use header, main, section, article, footer elements
 * - Page should display well when shared on social media
 */

const { test, expect } = require('@playwright/test');

const BASE_URL = 'http://localhost:3000';

test.describe('SEO and Semantic HTML', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(BASE_URL);
  });

  /**
   * Test Case 1: Check page title tag
   *
   * Verifies that the page title contains 'MirDB' and describes the product
   */
  test('TC1: Page title contains MirDB and describes the product', async ({ page }) => {
    const title = await page.title();

    // Verify title exists and is not empty
    expect(title).toBeTruthy();
    expect(title.length).toBeGreaterThan(0);

    // Verify title contains 'MirDB'
    expect(title).toContain('MirDB');

    // Verify title describes the product (should contain key descriptors)
    const lowerTitle = title.toLowerCase();
    const hasKeyValueStore = lowerTitle.includes('key-value') || lowerTitle.includes('key value');
    const hasPersistent = lowerTitle.includes('persistent');
    const hasMemcached = lowerTitle.includes('memcached');

    // Title should describe the product's nature
    expect(hasKeyValueStore || hasPersistent || hasMemcached).toBeTruthy();

    // Verify the specific expected format
    expect(title).toMatch(/MirDB.*Persistent.*Key-Value.*Memcached/i);
  });

  /**
   * Test Case 2: Check meta description
   *
   * Verifies that meta description is present and describes MirDB's value proposition
   */
  test('TC2: Meta description is present and describes value proposition', async ({ page }) => {
    // Get the meta description element
    const metaDescription = page.locator('meta[name="description"]');

    // Verify the meta description exists
    await expect(metaDescription).toHaveCount(1);

    // Get the content attribute
    const content = await metaDescription.getAttribute('content');

    // Verify content exists and is not empty
    expect(content).toBeTruthy();
    expect(content.length).toBeGreaterThan(50); // Should be meaningful description

    // Verify content describes MirDB's value proposition
    const lowerContent = content.toLowerCase();
    expect(lowerContent).toContain('mirdb');

    // Should mention key aspects of the product
    const mentionsKeyValue = lowerContent.includes('key-value') || lowerContent.includes('key value');
    const mentionsPersistent = lowerContent.includes('persistent') || lowerContent.includes('durability');
    const mentionsMemcached = lowerContent.includes('memcached');

    // At least some value proposition keywords should be present
    const valuePropositionKeywords = mentionsKeyValue || mentionsPersistent || mentionsMemcached;
    expect(valuePropositionKeywords).toBeTruthy();
  });

  /**
   * Test Case 3: Verify single h1 element
   *
   * Verifies that the page has exactly one h1 element containing the main headline
   */
  test('TC3: Page has exactly one h1 element with main headline', async ({ page }) => {
    // Get all h1 elements
    const h1Elements = page.locator('h1');

    // Verify there is exactly one h1
    await expect(h1Elements).toHaveCount(1);

    // Get the h1 text content
    const h1Text = await h1Elements.textContent();

    // Verify h1 has meaningful content
    expect(h1Text).toBeTruthy();
    expect(h1Text.trim().length).toBeGreaterThan(10);

    // Verify h1 is visible
    await expect(h1Elements).toBeVisible();

    // Verify h1 relates to the product (should be the main headline)
    const lowerH1 = h1Text.toLowerCase();
    const isProductRelated =
      lowerH1.includes('mirdb') ||
      lowerH1.includes('persistent') ||
      lowerH1.includes('memcached') ||
      lowerH1.includes('key-value') ||
      lowerH1.includes('application');

    expect(isProductRelated).toBeTruthy();
  });

  /**
   * Test Case 4: Check for semantic HTML5 elements
   *
   * Verifies that the page uses header, nav, main, section, and footer elements appropriately
   */
  test('TC4: Page uses semantic HTML5 elements appropriately', async ({ page }) => {
    // Check for header element
    const header = page.locator('header');
    await expect(header).toHaveCount(1);
    await expect(header).toBeVisible();

    // Check for nav element (should be inside header or standalone)
    const nav = page.locator('nav');
    await expect(nav.first()).toBeVisible();
    expect(await nav.count()).toBeGreaterThanOrEqual(1);

    // Check for main element
    const main = page.locator('main');
    await expect(main).toHaveCount(1);
    await expect(main).toBeVisible();

    // Check for section elements (should have multiple sections)
    const sections = page.locator('main section');
    expect(await sections.count()).toBeGreaterThanOrEqual(2);

    // Check for footer element
    const footer = page.locator('footer');
    await expect(footer).toHaveCount(1);
    await expect(footer).toBeVisible();

    // Verify proper document structure:
    // - header should come before main
    // - main should contain sections
    // - footer should come after main

    // Check that header comes before main in DOM
    const headerBoundingBox = await header.boundingBox();
    const mainBoundingBox = await main.boundingBox();
    const footerBoundingBox = await footer.boundingBox();

    expect(headerBoundingBox.y).toBeLessThan(mainBoundingBox.y);
    expect(mainBoundingBox.y).toBeLessThan(footerBoundingBox.y);

    // Verify sections have proper headings for accessibility
    // Note: Hero section uses h1 (the page's main heading), other sections should use h2+
    for (let i = 0; i < await sections.count(); i++) {
      const section = sections.nth(i);
      const heading = section.locator('h1, h2, h3, h4').first();
      await expect(heading).toBeVisible();
    }
  });

  /**
   * Test Case 5: Verify Open Graph meta tags
   *
   * Verifies that the page includes og:title, og:description, og:type meta tags
   */
  test('TC5: Page includes Open Graph meta tags for social sharing', async ({ page }) => {
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
    expect(ogDescContent.length).toBeGreaterThan(30);

    // Check for og:type
    const ogType = page.locator('meta[property="og:type"]');
    await expect(ogType).toHaveCount(1);
    const ogTypeContent = await ogType.getAttribute('content');
    expect(ogTypeContent).toBeTruthy();
    // og:type should be 'website' for a landing page
    expect(ogTypeContent).toBe('website');
  });

  /**
   * Test Case 6: Check canonical URL
   *
   * Verifies that the page includes a canonical link element
   */
  test('TC6: Page includes canonical link element', async ({ page }) => {
    // Check for canonical link
    const canonicalLink = page.locator('link[rel="canonical"]');
    await expect(canonicalLink).toHaveCount(1);

    // Get the href attribute
    const href = await canonicalLink.getAttribute('href');
    expect(href).toBeTruthy();

    // Verify it's a valid URL format
    expect(href).toMatch(/^https?:\/\//);

    // Verify it points to the MirDB domain
    expect(href.toLowerCase()).toContain('mirdb');
  });

  /**
   * Additional comprehensive SEO tests
   */
  test('Page has proper HTML lang attribute', async ({ page }) => {
    const htmlLang = await page.locator('html').getAttribute('lang');
    expect(htmlLang).toBeTruthy();
    expect(htmlLang).toBe('en');
  });

  test('Page has viewport meta tag for responsive design', async ({ page }) => {
    const viewport = page.locator('meta[name="viewport"]');
    await expect(viewport).toHaveCount(1);

    const content = await viewport.getAttribute('content');
    expect(content).toContain('width=device-width');
  });

  test('Page has proper charset declaration', async ({ page }) => {
    const charset = page.locator('meta[charset]');
    await expect(charset).toHaveCount(1);

    const charsetValue = await charset.getAttribute('charset');
    expect(charsetValue.toLowerCase()).toBe('utf-8');
  });

  test('All sections have unique IDs for navigation', async ({ page }) => {
    const sections = page.locator('main section');
    const sectionCount = await sections.count();
    const ids = new Set();

    for (let i = 0; i < sectionCount; i++) {
      const id = await sections.nth(i).getAttribute('id');
      expect(id).toBeTruthy();
      expect(ids.has(id)).toBeFalsy(); // No duplicate IDs
      ids.add(id);
    }
  });

  test('Header contains proper navigation links', async ({ page }) => {
    const navLinks = page.locator('header nav a');
    expect(await navLinks.count()).toBeGreaterThanOrEqual(3);

    // Check that links have meaningful text
    for (let i = 0; i < await navLinks.count(); i++) {
      const linkText = await navLinks.nth(i).textContent();
      expect(linkText.trim().length).toBeGreaterThan(0);
    }
  });

  test('External links have proper security attributes', async ({ page }) => {
    const externalLinks = page.locator('a[target="_blank"]');
    const count = await externalLinks.count();

    for (let i = 0; i < count; i++) {
      const rel = await externalLinks.nth(i).getAttribute('rel');
      expect(rel).toBeTruthy();
      expect(rel).toContain('noopener');
    }
  });
});
