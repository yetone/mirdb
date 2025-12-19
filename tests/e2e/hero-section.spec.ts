import { test, expect } from '@playwright/test';

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Hero section contains h1 element with MirDB text', async ({ page }) => {
    // Test Case 1: Load homepage and inspect hero section DOM
    // Expected: Hero section contains h1 element with 'MirDB' text
    const heroSection = page.locator('section.hero, #hero, [data-testid="hero"]');
    await expect(heroSection).toBeVisible();

    const heading = heroSection.locator('h1');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('MirDB');
  });

  test('TC2: Tagline contains required keywords', async ({ page }) => {
    // Test Case 2: Query for tagline element in hero section
    // Expected: Tagline contains keywords: 'persistent', 'key-value', 'memcached'
    const heroSection = page.locator('section.hero, #hero, [data-testid="hero"]');

    const tagline = heroSection.locator('.tagline, [data-testid="tagline"], h2, .subtitle');
    await expect(tagline).toBeVisible();

    const taglineText = await tagline.textContent();
    const lowerText = taglineText?.toLowerCase() || '';

    // Check for required keywords
    expect(lowerText).toMatch(/persistent/);
    expect(lowerText).toMatch(/key.?value/);
    expect(lowerText).toMatch(/memcached/);
  });

  test('TC3: At least 2 differentiator bullet points are present', async ({ page }) => {
    // Test Case 3: Query for differentiator bullet points
    // Expected: At least 2 bullet points or feature highlights are present in hero
    const heroSection = page.locator('section.hero, #hero, [data-testid="hero"]');

    // Look for bullet points or feature highlights
    const highlights = heroSection.locator('ul li, .feature-highlight, .differentiator, [data-testid="highlight"]');
    const count = await highlights.count();

    expect(count).toBeGreaterThanOrEqual(2);
  });

  test('TC4: Get Started CTA button is present and visible', async ({ page }) => {
    // Test Case 4: Query for Get Started CTA button
    // Expected: Button with text 'Get Started' or similar is present and visible
    const heroSection = page.locator('section.hero, #hero, [data-testid="hero"]');

    const ctaButton = heroSection.locator('a, button').filter({ hasText: /get started/i });
    await expect(ctaButton).toBeVisible();

    // Check it looks like a primary CTA (has appropriate styling class)
    await expect(ctaButton.first()).toHaveAttribute('href', /#getting-started/);
  });

  test('TC5: GitHub CTA link is present in hero section', async ({ page }) => {
    // Test Case 5: Query for GitHub CTA link
    // Expected: Link to GitHub repository is present in hero section
    const heroSection = page.locator('section.hero, #hero, [data-testid="hero"]');

    const githubLink = heroSection.locator('a').filter({ hasText: /github/i });
    await expect(githubLink).toBeVisible();

    // Check it points to GitHub
    const href = await githubLink.getAttribute('href');
    expect(href).toMatch(/github\.com/);
  });

  test('TC6: Core content renders without JavaScript (static HTML)', async ({ page, browser }) => {
    // Test Case 6: Hero section renders without JavaScript
    // Expected: Core content visible even with JS disabled (static HTML)

    // Create a new context with JavaScript disabled
    const context = await browser.newContext({ javaScriptEnabled: false });
    const noJsPage = await context.newPage();

    await noJsPage.goto('/');

    // Verify hero section and core content are visible without JS
    const heroSection = noJsPage.locator('section.hero, #hero, [data-testid="hero"]');
    await expect(heroSection).toBeVisible();

    // Check h1 with MirDB is visible
    const heading = heroSection.locator('h1');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('MirDB');

    // Check tagline is visible
    const tagline = heroSection.locator('.tagline, [data-testid="tagline"], h2, .subtitle');
    await expect(tagline).toBeVisible();

    // Check CTA buttons are visible
    const ctaButtons = heroSection.locator('a, button');
    expect(await ctaButtons.count()).toBeGreaterThanOrEqual(2);

    await context.close();
  });
});
