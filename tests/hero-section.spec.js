const { test, expect } = require('@playwright/test');

/**
 * Hero Section Display Tests
 * Scenario: Validate the hero section displays MirDB logo, tagline, and primary call-to-action buttons correctly
 */

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Hero section contains logo.gif image element with proper src attribute', async ({ page }) => {
    // Load homepage and inspect hero section
    const heroSection = page.locator('.hero, [class*="hero"], section:first-of-type, header + section');
    await expect(heroSection.first()).toBeVisible();

    // Check that the logo.gif image element exists with proper src attribute
    const logo = page.locator('img[src*="logo.gif"], img[src*="logo"]');
    await expect(logo.first()).toBeVisible();

    // Verify the src attribute contains logo.gif
    const src = await logo.first().getAttribute('src');
    expect(src).toContain('logo.gif');
  });

  test('Test Case 2: Page contains headline with MirDB and Persistent Key-Value Store text', async ({ page }) => {
    // Check headline text content
    const pageContent = await page.textContent('body');

    // Verify 'MirDB' appears in the page
    expect(pageContent).toContain('MirDB');

    // Verify 'Persistent Key-Value Store' text appears
    expect(pageContent.toLowerCase()).toContain('persistent key-value store');

    // Verify there's a headline element with MirDB
    const headline = page.locator('h1, h2, [class*="headline"], [class*="title"]').filter({ hasText: /MirDB/i });
    await expect(headline.first()).toBeVisible();
  });

  test('Test Case 3: Two CTA buttons present - Get Started (primary) and View on GitHub (secondary)', async ({ page }) => {
    // Check CTA button presence
    const getStartedButton = page.locator('a, button').filter({ hasText: /Get Started/i });
    await expect(getStartedButton.first()).toBeVisible();

    const githubButton = page.locator('a, button').filter({ hasText: /View on GitHub/i });
    await expect(githubButton.first()).toBeVisible();

    // Verify both buttons are visible and clickable
    await expect(getStartedButton.first()).toBeEnabled();
    await expect(githubButton.first()).toBeEnabled();
  });

  test('Test Case 4: View on GitHub button links to https://github.com/yetone/mirdb', async ({ page }) => {
    // Click 'View on GitHub' button and verify link
    const githubButton = page.locator('a, button').filter({ hasText: /View on GitHub/i });
    await expect(githubButton.first()).toBeVisible();

    // Verify the href attribute points to the correct GitHub URL
    const href = await githubButton.first().getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');
  });
});
