import { test, expect } from '@playwright/test';

test.describe('SEO Requirements', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Page has title tag with MirDB', async ({ page }) => {
    // Query for title tag and verify it contains MirDB
    const title = await page.title();

    expect(title).toBeTruthy();
    expect(title.length).toBeGreaterThan(0);
    expect(title).toContain('MirDB');
  });

  test('TC2: Page has meta description tag with product summary', async ({ page }) => {
    // Query for meta description tag
    const metaDescription = page.locator('meta[name="description"]');

    await expect(metaDescription).toHaveCount(1);

    const content = await metaDescription.getAttribute('content');
    expect(content).toBeTruthy();
    expect(content!.length).toBeGreaterThan(10);
    // Should contain key product information
    expect(content).toContain('MirDB');
  });

  test('TC3: Page has Open Graph meta tags', async ({ page }) => {
    // Query for og:title meta tag
    const ogTitle = page.locator('meta[property="og:title"]');
    await expect(ogTitle).toHaveCount(1);
    const ogTitleContent = await ogTitle.getAttribute('content');
    expect(ogTitleContent).toBeTruthy();
    expect(ogTitleContent).toContain('MirDB');

    // Query for og:description meta tag
    const ogDescription = page.locator('meta[property="og:description"]');
    await expect(ogDescription).toHaveCount(1);
    const ogDescContent = await ogDescription.getAttribute('content');
    expect(ogDescContent).toBeTruthy();
    expect(ogDescContent!.length).toBeGreaterThan(10);

    // Query for og:image meta tag
    const ogImage = page.locator('meta[property="og:image"]');
    await expect(ogImage).toHaveCount(1);
    const ogImageContent = await ogImage.getAttribute('content');
    expect(ogImageContent).toBeTruthy();
  });

  test('TC4: Page has exactly one h1 element', async ({ page }) => {
    // Query for all h1 elements
    const h1Elements = page.locator('h1');

    // Verify exactly one h1 exists
    await expect(h1Elements).toHaveCount(1);

    // Verify the h1 has meaningful content
    const h1Text = await h1Elements.textContent();
    expect(h1Text).toBeTruthy();
    expect(h1Text!.trim().length).toBeGreaterThan(0);
  });

  test('TC5: Page has canonical link tag', async ({ page }) => {
    // Query for canonical link tag
    const canonicalLink = page.locator('link[rel="canonical"]');

    await expect(canonicalLink).toHaveCount(1);

    const href = await canonicalLink.getAttribute('href');
    expect(href).toBeTruthy();
    // Canonical URL should be a valid URL
    expect(href).toMatch(/^https?:\/\//);
  });
});
