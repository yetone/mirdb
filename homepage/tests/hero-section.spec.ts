import { test, expect } from '@playwright/test';

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Product name MirDB is visible in the hero section', async ({ page }) => {
    // Load homepage and inspect hero section
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify product name 'MirDB' is visible
    const productName = heroSection.locator('h1');
    await expect(productName).toContainText('MirDB');
  });

  test('TC2: Tagline contains persistent key-value store and Memcached protocol', async ({ page }) => {
    // Check for tagline text content
    const heroSection = page.locator('[data-testid="hero-section"]');
    const tagline = heroSection.locator('[data-testid="tagline"]');

    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('persistent key-value store');
    await expect(tagline).toContainText('Memcached protocol');
  });

  test('TC3: Get Started CTA navigates to documentation or quick start section', async ({ page }) => {
    // Click 'Get Started' CTA button
    const getStartedButton = page.locator('[data-testid="cta-get-started"]');
    await expect(getStartedButton).toBeVisible();
    await expect(getStartedButton).toHaveText('Get Started');

    // Click and verify navigation to quick start section
    await getStartedButton.click();

    // Verify user is navigated to quick start section (anchor link)
    await expect(page).toHaveURL(/#quick-start/);

    // Verify the quick start section is visible
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();
  });

  test('TC4: View on GitHub CTA opens GitHub repository in new tab', async ({ page, context }) => {
    // Find the 'View on GitHub' CTA button
    const githubButton = page.locator('[data-testid="cta-github"]');
    await expect(githubButton).toBeVisible();
    await expect(githubButton).toHaveText('View on GitHub');

    // Verify the link has target="_blank" for new tab
    await expect(githubButton).toHaveAttribute('target', '_blank');

    // Verify it links to the GitHub repository
    await expect(githubButton).toHaveAttribute('href', /github\.com\/yetone\/mirdb/);
  });

  test('TC5: Hero content is fully visible without scrolling on 1280x720 viewport', async ({ page }) => {
    // Set viewport to standard desktop size (1280x720)
    await page.setViewportSize({ width: 1280, height: 720 });
    await page.goto('/');

    // Verify hero section is visible
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Check that hero section is within viewport
    const heroBox = await heroSection.boundingBox();
    expect(heroBox).not.toBeNull();

    if (heroBox) {
      // Verify the hero section fits within the viewport
      expect(heroBox.y).toBeGreaterThanOrEqual(0);
      expect(heroBox.y + heroBox.height).toBeLessThanOrEqual(720);
    }

    // Verify all key elements are visible
    await expect(page.locator('h1')).toBeVisible();
    await expect(page.locator('[data-testid="tagline"]')).toBeVisible();
    await expect(page.locator('[data-testid="cta-get-started"]')).toBeVisible();
    await expect(page.locator('[data-testid="cta-github"]')).toBeVisible();
  });
});
