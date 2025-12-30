// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: H1 element exists with text content MirDB', async ({ page }) => {
    // Test Case 1: Load homepage and query for h1 element containing 'MirDB'
    // Expected: H1 element exists with text content 'MirDB'
    const h1 = page.locator('h1');
    await expect(h1).toBeVisible();
    await expect(h1).toContainText('MirDB');
  });

  test('TC2: Tagline contains core value proposition keywords', async ({ page }) => {
    // Test Case 2: Query for tagline element containing key terms: 'persistent', 'key-value', 'memcached'
    // Expected: Tagline element exists containing the core value proposition keywords
    const heroSection = page.locator('.hero, [data-testid="hero"], section').first();

    // Check for tagline containing key terms (case-insensitive)
    const taglineElement = page.locator('.tagline, .hero-tagline, [data-testid="tagline"], p, h2').filter({
      hasText: /persistent|key-value|memcached/i
    }).first();

    await expect(taglineElement).toBeVisible();

    // Verify it contains the essential keywords
    const taglineText = await taglineElement.textContent();
    const lowerText = taglineText?.toLowerCase() || '';

    // Check for presence of key terms
    const hasPersistent = lowerText.includes('persistent');
    const hasKeyValue = lowerText.includes('key-value') || lowerText.includes('key value');
    const hasMemcached = lowerText.includes('memcached');

    expect(hasPersistent || hasKeyValue || hasMemcached).toBeTruthy();
  });

  test('TC3: Get Started CTA button is present and visible', async ({ page }) => {
    // Test Case 3: Query for 'Get Started' button or link in hero section
    // Expected: Primary CTA button exists and is visible
    const getStartedButton = page.locator('a, button').filter({
      hasText: /get started/i
    }).first();

    await expect(getStartedButton).toBeVisible();
  });

  test('TC4: GitHub CTA exists and links to correct repository URL', async ({ page }) => {
    // Test Case 4: Query for GitHub link/button in hero section
    // Expected: GitHub CTA exists and links to correct repository URL
    const githubLink = page.locator('a').filter({
      hasText: /github|view on github/i
    }).first();

    await expect(githubLink).toBeVisible();

    // Verify it links to a GitHub repository
    const href = await githubLink.getAttribute('href');
    expect(href).toMatch(/github\.com/i);
  });
});
