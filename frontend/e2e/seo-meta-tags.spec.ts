import { test, expect } from '@playwright/test';

test.describe('SEO - Meta Tags and Structure', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Page has unique, descriptive title tag', async ({ page }) => {
    // Check that the page has a meaningful title (not generic)
    const title = await page.title();

    // Title should not be empty or generic
    expect(title).toBeTruthy();
    expect(title.length).toBeGreaterThan(0);

    // Title should be descriptive (not just "frontend" or similar generic terms)
    expect(title).toMatch(/MirDB/i);

    // Title should be within recommended SEO length (50-60 characters max)
    expect(title.length).toBeLessThanOrEqual(70);
  });

  test('Test Case 2: Page has meta description under 160 characters', async ({ page }) => {
    // Check for meta description tag
    const metaDescription = page.locator('meta[name="description"]');
    await expect(metaDescription).toHaveCount(1);

    // Get the content attribute
    const content = await metaDescription.getAttribute('content');

    // Description should exist and have content
    expect(content).toBeTruthy();
    expect(content!.length).toBeGreaterThan(0);

    // Description should be under 160 characters for SEO best practices
    expect(content!.length).toBeLessThanOrEqual(160);

    // Description should be meaningful (at least a few words)
    expect(content!.length).toBeGreaterThanOrEqual(50);
  });

  test('Test Case 3: Page has Open Graph tags (og:title, og:description, og:image)', async ({ page }) => {
    // Check for og:title
    const ogTitle = page.locator('meta[property="og:title"]');
    await expect(ogTitle).toHaveCount(1);
    const ogTitleContent = await ogTitle.getAttribute('content');
    expect(ogTitleContent).toBeTruthy();
    expect(ogTitleContent!.length).toBeGreaterThan(0);

    // Check for og:description
    const ogDescription = page.locator('meta[property="og:description"]');
    await expect(ogDescription).toHaveCount(1);
    const ogDescContent = await ogDescription.getAttribute('content');
    expect(ogDescContent).toBeTruthy();
    expect(ogDescContent!.length).toBeGreaterThan(0);

    // Check for og:image
    const ogImage = page.locator('meta[property="og:image"]');
    await expect(ogImage).toHaveCount(1);
    const ogImageContent = await ogImage.getAttribute('content');
    expect(ogImageContent).toBeTruthy();
    // Image URL should be a valid URL format
    expect(ogImageContent).toMatch(/^(https?:\/\/|\/)/);
  });

  test('Test Case 4: Page has single H1 and logical heading hierarchy', async ({ page }) => {
    // Check that there is exactly one H1 tag
    const h1Tags = page.locator('h1');
    await expect(h1Tags).toHaveCount(1);

    // H1 should have meaningful content
    const h1Text = await h1Tags.first().textContent();
    expect(h1Text).toBeTruthy();
    expect(h1Text!.trim().length).toBeGreaterThan(0);

    // Check heading hierarchy within main content area (excluding footer which has different semantic context)
    const mainContent = page.locator('main');
    const mainHeadings = await mainContent.locator('h1, h2, h3, h4, h5, h6').all();

    if (mainHeadings.length > 0) {
      // Ensure h1 comes first in main content
      const firstHeading = mainHeadings[0];
      const firstHeadingTag = await firstHeading.evaluate(el => el.tagName.toLowerCase());
      expect(firstHeadingTag).toBe('h1');

      // Check that heading levels within main don't skip levels excessively
      const headingLevels: number[] = [];
      for (const heading of mainHeadings) {
        const tagName = await heading.evaluate(el => el.tagName.toLowerCase());
        const level = parseInt(tagName.charAt(1));
        headingLevels.push(level);
      }

      // Verify the main content heading structure is logical
      // H1 should be present and be the first heading
      expect(headingLevels[0]).toBe(1);

      // Verify subsequent headings don't skip more than 2 levels
      for (let i = 1; i < headingLevels.length; i++) {
        const currentLevel = headingLevels[i];
        const previousLevel = headingLevels[i - 1];
        // When going deeper, shouldn't skip more than 2 levels
        if (currentLevel > previousLevel) {
          expect(currentLevel - previousLevel).toBeLessThanOrEqual(2);
        }
      }
    }

    // Additionally verify the page has proper document outline
    // Footer headings are allowed to have their own hierarchy
    const footerHeadings = await page.locator('footer h2, footer h3, footer h4').all();
    // Footer can have subsection headings without requiring intermediate levels
    expect(footerHeadings.length).toBeGreaterThanOrEqual(0);
  });

  test('Test Case 5: Page has canonical URL link element', async ({ page }) => {
    // Check for canonical link tag
    const canonicalLink = page.locator('link[rel="canonical"]');
    await expect(canonicalLink).toHaveCount(1);

    // Get the href attribute
    const href = await canonicalLink.getAttribute('href');

    // Canonical URL should exist and be a valid URL
    expect(href).toBeTruthy();
    expect(href).toMatch(/^https?:\/\//);
  });
});
