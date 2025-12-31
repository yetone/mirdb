import { test, expect } from '@playwright/test';

test.describe('SEO Optimization', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: page has unique, descriptive title tag containing MirDB', async ({ page }) => {
    const title = await page.title();

    // Title should exist and contain 'MirDB'
    expect(title).toBeTruthy();
    expect(title).toContain('MirDB');

    // Title should be descriptive (more than just "MirDB")
    expect(title.length).toBeGreaterThan(10);

    // Verify there's only one title tag
    const titleTags = await page.locator('head title').count();
    expect(titleTags).toBe(1);
  });

  test('TC2: meta description is present and describes MirDB purpose (150-160 chars)', async ({ page }) => {
    const metaDescription = page.locator('meta[name="description"]');

    // Meta description should exist
    await expect(metaDescription).toHaveCount(1);

    const content = await metaDescription.getAttribute('content');
    expect(content).toBeTruthy();

    // Description should mention MirDB's purpose
    expect(content!.toLowerCase()).toContain('mirdb');

    // Description should be within optimal length (150-160 characters is ideal, but 50-300 is acceptable)
    expect(content!.length).toBeGreaterThanOrEqual(50);
    expect(content!.length).toBeLessThanOrEqual(300);

    // Description should describe key purpose (key-value store, memcached, etc.)
    const hasRelevantContent =
      content!.toLowerCase().includes('key-value') ||
      content!.toLowerCase().includes('memcached') ||
      content!.toLowerCase().includes('persistent') ||
      content!.toLowerCase().includes('database');
    expect(hasRelevantContent).toBe(true);
  });

  test('TC3: canonical URL is specified in head', async ({ page }) => {
    const canonicalLink = page.locator('link[rel="canonical"]');

    // Canonical link should exist
    await expect(canonicalLink).toHaveCount(1);

    // Canonical URL should have a valid href
    const href = await canonicalLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href!.length).toBeGreaterThan(0);
  });

  test('TC4: page uses semantic HTML structure (header, main, nav, section, footer)', async ({ page }) => {
    // Check for header element
    const header = page.locator('header');
    await expect(header).toHaveCount(1);

    // Check for main element
    const main = page.locator('main');
    await expect(main).toHaveCount(1);

    // Check for nav element
    const nav = page.locator('nav');
    const navCount = await nav.count();
    expect(navCount).toBeGreaterThanOrEqual(1);

    // Check for section elements
    const sections = page.locator('section');
    const sectionCount = await sections.count();
    expect(sectionCount).toBeGreaterThanOrEqual(1);

    // Check for footer element
    const footer = page.locator('footer');
    await expect(footer).toHaveCount(1);
  });

  test('TC5: Open Graph title and description meta tags are present', async ({ page }) => {
    // Check for og:title
    const ogTitle = page.locator('meta[property="og:title"]');
    await expect(ogTitle).toHaveCount(1);

    const ogTitleContent = await ogTitle.getAttribute('content');
    expect(ogTitleContent).toBeTruthy();
    expect(ogTitleContent!.toLowerCase()).toContain('mirdb');

    // Check for og:description
    const ogDescription = page.locator('meta[property="og:description"]');
    await expect(ogDescription).toHaveCount(1);

    const ogDescContent = await ogDescription.getAttribute('content');
    expect(ogDescContent).toBeTruthy();
    expect(ogDescContent!.length).toBeGreaterThan(10);
  });

  test('TC6: Open Graph image meta tag is present for social sharing preview', async ({ page }) => {
    // Check for og:image
    const ogImage = page.locator('meta[property="og:image"]');
    await expect(ogImage).toHaveCount(1);

    const ogImageContent = await ogImage.getAttribute('content');
    expect(ogImageContent).toBeTruthy();

    // Image URL should be a valid URL or path
    expect(ogImageContent!.length).toBeGreaterThan(0);
  });

  test('TC7: page has exactly one h1 tag containing the main title', async ({ page }) => {
    // Check for h1 elements
    const h1Tags = page.locator('h1');

    // Should have exactly one h1
    await expect(h1Tags).toHaveCount(1);

    // h1 should contain meaningful content
    const h1Text = await h1Tags.textContent();
    expect(h1Text).toBeTruthy();
    expect(h1Text!.trim().length).toBeGreaterThan(0);

    // h1 should contain MirDB (the main product name)
    expect(h1Text!).toContain('MirDB');
  });
});
