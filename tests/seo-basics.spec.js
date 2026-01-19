// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('SEO Basics', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Page has descriptive title tag containing MirDB', async ({ page }) => {
    // Check that the page has a title tag with MirDB
    const title = await page.title();
    expect(title).toBeTruthy();
    expect(title.toLowerCase()).toContain('mirdb');
    // Verify the title is descriptive (more than just "MirDB")
    expect(title.length).toBeGreaterThan(5);
  });

  test('TC2: Page has meta description tag with relevant content', async ({ page }) => {
    // Check for meta description tag
    const metaDescription = page.locator('meta[name="description"]');
    await expect(metaDescription).toHaveCount(1);

    // Get the content attribute
    const content = await metaDescription.getAttribute('content');
    expect(content).toBeTruthy();
    expect(content.length).toBeGreaterThan(20);
    // Verify it mentions relevant keywords
    expect(content.toLowerCase()).toMatch(/mirdb|key-value|memcached|persistent/i);
  });

  test('TC3: Open Graph tags are present (og:title, og:description, og:image)', async ({ page }) => {
    // Check for og:title
    const ogTitle = page.locator('meta[property="og:title"]');
    await expect(ogTitle).toHaveCount(1);
    const ogTitleContent = await ogTitle.getAttribute('content');
    expect(ogTitleContent).toBeTruthy();
    expect(ogTitleContent.toLowerCase()).toContain('mirdb');

    // Check for og:description
    const ogDescription = page.locator('meta[property="og:description"]');
    await expect(ogDescription).toHaveCount(1);
    const ogDescContent = await ogDescription.getAttribute('content');
    expect(ogDescContent).toBeTruthy();
    expect(ogDescContent.length).toBeGreaterThan(20);

    // Check for og:image
    const ogImage = page.locator('meta[property="og:image"]');
    await expect(ogImage).toHaveCount(1);
    const ogImageContent = await ogImage.getAttribute('content');
    expect(ogImageContent).toBeTruthy();
  });

  test('TC4: Page uses semantic HTML elements (header, main, section, footer)', async ({ page }) => {
    // Check for header element
    const header = page.locator('header');
    await expect(header).toHaveCount(1);
    await expect(header).toBeVisible();

    // Check for main element
    const main = page.locator('main');
    await expect(main).toHaveCount(1);
    await expect(main).toBeVisible();

    // Check for section elements
    const sections = page.locator('section');
    const sectionCount = await sections.count();
    expect(sectionCount).toBeGreaterThanOrEqual(3);

    // Check for footer element
    const footer = page.locator('footer');
    await expect(footer).toHaveCount(1);
    await expect(footer).toBeVisible();
  });
});
