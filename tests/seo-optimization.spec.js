// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: SEO Optimization
 * Scenario: Verify page has proper SEO meta tags and semantic HTML (NFR-4)
 */

test.describe('SEO Optimization', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage before each test
    await page.goto('/');
  });

  /**
   * Test Case 1: Check for title tag
   * Input: Check for title tag
   * Expected: Page has descriptive title tag containing 'MirDB'
   */
  test('TC1: Page has descriptive title tag containing MirDB', async ({ page }) => {
    // Get the page title
    const pageTitle = await page.title();

    // Verify the title contains 'MirDB'
    expect(pageTitle).toContain('MirDB');

    // Verify the title is descriptive (not just 'MirDB')
    expect(pageTitle.length).toBeGreaterThan(5);

    // Verify the title tag exists in the HTML
    const titleElement = page.locator('head title');
    await expect(titleElement).toHaveCount(1);

    // Verify the title is descriptive - should mention what the product does
    expect(pageTitle.toLowerCase()).toMatch(/key-value|store|persistent|database/i);
  });

  /**
   * Test Case 2: Check for meta description
   * Input: Check for meta description
   * Expected: Page has meta description describing MirDB
   */
  test('TC2: Page has meta description describing MirDB', async ({ page }) => {
    // Find the meta description element
    const metaDescription = page.locator('meta[name="description"]');
    await expect(metaDescription).toHaveCount(1);

    // Get the content attribute
    const descriptionContent = await metaDescription.getAttribute('content');

    // Verify it contains MirDB
    expect(descriptionContent).toBeTruthy();
    expect(descriptionContent).toContain('MirDB');

    // Verify it's a proper description (reasonable length)
    expect(descriptionContent.length).toBeGreaterThan(50);
    expect(descriptionContent.length).toBeLessThan(200);

    // Verify it describes what MirDB is
    expect(descriptionContent.toLowerCase()).toMatch(/key-value|persistent|memcached/i);
  });

  /**
   * Test Case 3: Check for Open Graph tags
   * Input: Check for Open Graph tags
   * Expected: Open Graph tags (og:title, og:description, og:image) are present
   */
  test('TC3: Open Graph tags are present', async ({ page }) => {
    // Check for og:title
    const ogTitle = page.locator('meta[property="og:title"]');
    await expect(ogTitle).toHaveCount(1);
    const ogTitleContent = await ogTitle.getAttribute('content');
    expect(ogTitleContent).toBeTruthy();
    expect(ogTitleContent).toContain('MirDB');

    // Check for og:description
    const ogDescription = page.locator('meta[property="og:description"]');
    await expect(ogDescription).toHaveCount(1);
    const ogDescriptionContent = await ogDescription.getAttribute('content');
    expect(ogDescriptionContent).toBeTruthy();
    expect(ogDescriptionContent.length).toBeGreaterThan(20);

    // Check for og:image
    const ogImage = page.locator('meta[property="og:image"]');
    await expect(ogImage).toHaveCount(1);
    const ogImageContent = await ogImage.getAttribute('content');
    expect(ogImageContent).toBeTruthy();

    // Optional: Check for og:type (commonly used)
    const ogType = page.locator('meta[property="og:type"]');
    const ogTypeCount = await ogType.count();
    if (ogTypeCount > 0) {
      const ogTypeContent = await ogType.getAttribute('content');
      expect(ogTypeContent).toBeTruthy();
    }
  });

  /**
   * Test Case 4: Verify semantic HTML elements
   * Input: Verify semantic HTML elements
   * Expected: Page uses header, main, footer, nav, section, article elements appropriately
   */
  test('TC4: Page uses semantic HTML elements appropriately', async ({ page }) => {
    // Check for header element
    const header = page.locator('header');
    await expect(header).toHaveCount(1);
    await expect(header).toBeVisible();

    // Check for main element
    const main = page.locator('main');
    await expect(main).toHaveCount(1);
    await expect(main).toBeVisible();

    // Check for footer element
    const footer = page.locator('footer');
    await expect(footer).toHaveCount(1);
    await expect(footer).toBeVisible();

    // Check for nav element (typically inside header)
    const nav = page.locator('nav');
    await expect(nav).toHaveCount(1);
    await expect(nav).toBeVisible();

    // Check for section elements (multiple sections expected on this page)
    const sections = page.locator('section');
    const sectionCount = await sections.count();
    expect(sectionCount).toBeGreaterThan(0);

    // Check for article elements (feature cards use article)
    const articles = page.locator('article');
    const articleCount = await articles.count();
    expect(articleCount).toBeGreaterThan(0);

    // Verify semantic structure: nav should be inside header
    const navInHeader = page.locator('header nav');
    await expect(navInHeader).toHaveCount(1);

    // Verify semantic structure: main should contain sections
    const sectionsInMain = page.locator('main section');
    const sectionsInMainCount = await sectionsInMain.count();
    expect(sectionsInMainCount).toBeGreaterThan(0);
  });

  /**
   * Test Case 5: Check for canonical URL
   * Input: Check for canonical URL
   * Expected: Canonical URL meta tag is present
   */
  test('TC5: Canonical URL meta tag is present', async ({ page }) => {
    // Check for canonical link element
    const canonical = page.locator('link[rel="canonical"]');
    await expect(canonical).toHaveCount(1);

    // Get the href attribute
    const canonicalHref = await canonical.getAttribute('href');

    // Verify it's a valid URL
    expect(canonicalHref).toBeTruthy();
    expect(canonicalHref).toMatch(/^https?:\/\//);

    // Verify it contains the domain (for MirDB, it should be the project URL)
    expect(canonicalHref.toLowerCase()).toMatch(/mirdb|github/);
  });
});
