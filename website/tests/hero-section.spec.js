// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

// Base URL for the static HTML file
const BASE_URL = 'file://' + path.resolve(__dirname, '../dist/index.html');

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(BASE_URL);
  });

  test('Test Case 1: Product name MirDB is visible in hero section', async ({ page }) => {
    // Load homepage and check hero section
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    const productName = page.locator('[data-testid="product-name"]');
    await expect(productName).toBeVisible();
    await expect(productName).toHaveText('MirDB');
  });

  test('Test Case 2: Tagline is displayed correctly', async ({ page }) => {
    // Check tagline visibility
    const tagline = page.locator('[data-testid="tagline"]');
    await expect(tagline).toBeVisible();
    await expect(tagline).toHaveText('Persistent Key-Value Store with Memcached Protocol');
  });

  test('Test Case 3: Get Started button is visible and styled correctly', async ({ page }) => {
    // Check primary CTA button
    const getStartedButton = page.locator('[data-testid="cta-get-started"]');
    await expect(getStartedButton).toBeVisible();
    await expect(getStartedButton).toContainText('Get Started');

    // Verify it has the primary button styling (blue background)
    await expect(getStartedButton).toHaveClass(/btn-primary/);

    // Verify the button is clickable (has proper href)
    await expect(getStartedButton).toHaveAttribute('href', '#quickstart');
  });

  test('Test Case 4: View on GitHub button is visible and links correctly', async ({ page }) => {
    // Check secondary CTA button
    const githubButton = page.locator('[data-testid="cta-github"]');
    await expect(githubButton).toBeVisible();
    await expect(githubButton).toContainText('View on GitHub');

    // Verify it has the secondary button styling
    await expect(githubButton).toHaveClass(/btn-secondary/);

    // Verify GitHub link
    await expect(githubButton).toHaveAttribute('href', 'https://github.com/pjzhong/mirdb');

    // Verify it opens in new tab
    await expect(githubButton).toHaveAttribute('target', '_blank');
    await expect(githubButton).toHaveAttribute('rel', 'noopener noreferrer');
  });

  test('Test Case 5: Value proposition description is visible', async ({ page }) => {
    // Verify brief description presence
    const valueProposition = page.locator('[data-testid="value-proposition"]');
    await expect(valueProposition).toBeVisible();

    // Get the text content
    const text = await valueProposition.textContent();

    // Verify it contains meaningful content (2-3 sentences about MirDB)
    expect(text).toBeTruthy();
    expect(text.length).toBeGreaterThan(100); // Ensure substantial content

    // Verify key value proposition elements are mentioned
    expect(text).toContain('MirDB');
    expect(text).toContain('Memcached');
    expect(text).toContain('Rust');
    expect(text).toContain('LSM tree');
    expect(text).toContain('persistence');
  });
});
