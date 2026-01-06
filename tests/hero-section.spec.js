// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Hero section renders with product logo visible', async ({ page }) => {
    // Navigate to homepage and verify hero section renders with product logo visible
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    const heroLogo = page.locator('[data-testid="hero-logo"]');
    await expect(heroLogo).toBeVisible();
    await expect(heroLogo).toHaveAttribute('src', 'assets/logo.gif');
    await expect(heroLogo).toHaveAttribute('alt', 'MirDB Logo');
  });

  test('Test Case 2: Product name MirDB is displayed in hero section', async ({ page }) => {
    // Check that the product name 'MirDB' is displayed in hero section
    const heroTitle = page.locator('[data-testid="hero-title"]');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toHaveText('MirDB');
  });

  test('Test Case 3: Tagline is visible in hero section', async ({ page }) => {
    // Check that the tagline 'Persistent Key-Value Store with Memcached Protocol' is visible
    const heroTagline = page.locator('[data-testid="hero-tagline"]');
    await expect(heroTagline).toBeVisible();
    await expect(heroTagline).toHaveText('Persistent Key-Value Store with Memcached Protocol');
  });

  test('Test Case 4: Get Started button exists and is clickable', async ({ page }) => {
    // Query for primary CTA button and verify it exists and is clickable
    const getStartedButton = page.locator('[data-testid="cta-get-started"]');
    await expect(getStartedButton).toBeVisible();
    await expect(getStartedButton).toHaveText('Get Started');
    await expect(getStartedButton).toBeEnabled();

    // Verify the button is a link with proper href
    await expect(getStartedButton).toHaveAttribute('href', '#get-started');

    // Verify button is clickable by checking it's not disabled and has correct role/styling
    const isClickable = await getStartedButton.isEnabled();
    expect(isClickable).toBe(true);
  });

  test('Test Case 5: View on GitHub button exists and links to GitHub repository', async ({ page }) => {
    // Query for secondary CTA button and verify it links to GitHub
    const githubButton = page.locator('[data-testid="cta-github"]');
    await expect(githubButton).toBeVisible();
    await expect(githubButton).toHaveText('View on GitHub');
    await expect(githubButton).toBeEnabled();

    // Verify the button links to the GitHub repository
    await expect(githubButton).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    await expect(githubButton).toHaveAttribute('target', '_blank');
    await expect(githubButton).toHaveAttribute('rel', 'noopener noreferrer');
  });
});
