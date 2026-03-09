/**
 * SEO and Meta Tags Unit Tests
 * Owner: Scenario 12 - SEO and Meta Tags
 *
 * Tests:
 * - Title contains "MirDB"
 * - Meta description present
 * - Open Graph tags present
 * - Twitter Card tags present
 * - Semantic HTML structure
 * - Canonical URL present
 */

const { test, expect } = require('@playwright/test');

test.describe('SEO and Meta Tags', () => {

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: document title contains MirDB and is descriptive', async ({ page }) => {
    const title = await page.title();

    // Title should contain 'MirDB'
    expect(title).toContain('MirDB');

    // Title should be descriptive (contain additional information)
    expect(title).toMatch(/MirDB\s*[-–—]\s*.+/);

    // Verify it contains the expected descriptive text
    expect(title).toContain('Persistent Key-Value Store');
  });

  test('TC2: meta description present with compelling description', async ({ page }) => {
    const metaDescription = await page.locator('meta[name="description"]').getAttribute('content');

    // Meta description should exist
    expect(metaDescription).toBeTruthy();

    // Meta description should contain MirDB
    expect(metaDescription).toContain('MirDB');

    // Meta description should describe key features
    expect(metaDescription).toMatch(/persistent|key-value|memcached/i);

    // Meta description should be an appropriate length (50-160 chars recommended)
    expect(metaDescription.length).toBeGreaterThan(50);
    expect(metaDescription.length).toBeLessThan(200);
  });

  test('TC3: Open Graph meta tags present', async ({ page }) => {
    // Check og:title
    const ogTitle = await page.locator('meta[property="og:title"]').getAttribute('content');
    expect(ogTitle).toBeTruthy();
    expect(ogTitle).toContain('MirDB');

    // Check og:description
    const ogDescription = await page.locator('meta[property="og:description"]').getAttribute('content');
    expect(ogDescription).toBeTruthy();
    expect(ogDescription.length).toBeGreaterThan(20);

    // Check og:type
    const ogType = await page.locator('meta[property="og:type"]').getAttribute('content');
    expect(ogType).toBeTruthy();
    expect(ogType).toBe('website');

    // Check og:image
    const ogImage = await page.locator('meta[property="og:image"]').getAttribute('content');
    expect(ogImage).toBeTruthy();
  });

  test('TC4: Twitter Card meta tags present', async ({ page }) => {
    // Check twitter:card
    const twitterCard = await page.locator('meta[name="twitter:card"]').getAttribute('content');
    expect(twitterCard).toBeTruthy();
    expect(['summary', 'summary_large_image']).toContain(twitterCard);

    // Check twitter:title
    const twitterTitle = await page.locator('meta[name="twitter:title"]').getAttribute('content');
    expect(twitterTitle).toBeTruthy();
    expect(twitterTitle).toContain('MirDB');

    // Check twitter:description
    const twitterDescription = await page.locator('meta[name="twitter:description"]').getAttribute('content');
    expect(twitterDescription).toBeTruthy();
    expect(twitterDescription.length).toBeGreaterThan(20);
  });

  test('TC5: semantic HTML structure used appropriately', async ({ page }) => {
    // Check for header element
    const header = page.locator('header');
    await expect(header).toBeVisible();

    // Check for main element
    const main = page.locator('main');
    await expect(main).toBeVisible();

    // Check for section elements within main
    const sections = page.locator('main > section');
    const sectionsCount = await sections.count();
    expect(sectionsCount).toBeGreaterThan(0);

    // Check for article elements (feature cards, etc.)
    const articles = page.locator('article');
    const articlesCount = await articles.count();
    expect(articlesCount).toBeGreaterThan(0);

    // Check for footer element
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Verify proper hierarchy - header before main
    const headerIndex = await page.evaluate(() => {
      const body = document.body;
      const header = body.querySelector('header');
      const main = body.querySelector('main');
      return Array.from(body.children).indexOf(header) < Array.from(body.children).indexOf(main);
    });
    expect(headerIndex).toBe(true);

    // Verify footer comes after main
    const footerAfterMain = await page.evaluate(() => {
      const body = document.body;
      const main = body.querySelector('main');
      const footer = body.querySelector('footer');
      return Array.from(body.children).indexOf(footer) > Array.from(body.children).indexOf(main);
    });
    expect(footerAfterMain).toBe(true);
  });

  test('TC6: canonical URL link present', async ({ page }) => {
    // Check for canonical link element
    const canonical = await page.locator('link[rel="canonical"]').getAttribute('href');
    expect(canonical).toBeTruthy();

    // Canonical URL should be a valid URL or relative path
    expect(canonical).toMatch(/^(https?:\/\/|\/)/);
  });

});
