import { test, expect } from '@playwright/test';

test.describe('Hero Section Display and Value Proposition', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Hero section renders with MirDB logo visible', async ({ page }) => {
    // Navigate to homepage root URL
    // Expected: Hero section renders with MirDB logo visible

    // Verify hero section exists
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify MirDB logo is visible
    const logo = page.locator('[data-testid="hero-logo"]');
    await expect(logo).toBeVisible();

    // Verify logo has proper alt text for accessibility
    await expect(logo).toHaveAttribute('alt', /mirdb/i);
  });

  test('TC2: Tagline text is present in hero section', async ({ page }) => {
    // Inspect hero section content
    // Expected: Tagline text 'Persistent Key-Value Store with Memcached Protocol Compatibility' is present

    const tagline = page.locator('[data-testid="hero-tagline"]');
    await expect(tagline).toBeVisible();
    await expect(tagline).toHaveText('Persistent Key-Value Store with Memcached Protocol Compatibility');
  });

  test('TC3: Get Started button scrolls to quick-start section', async ({ page }) => {
    // Click 'Get Started' button
    // Expected: Page scrolls to or navigates to quick-start section

    const getStartedButton = page.locator('[data-testid="cta-get-started"]');
    await expect(getStartedButton).toBeVisible();
    await expect(getStartedButton).toHaveText(/get started/i);

    // Click the Get Started button
    await getStartedButton.click();

    // Verify the quick-start section is now visible/in viewport
    const quickStartSection = page.locator('[data-testid="quick-start-section"]');
    await expect(quickStartSection).toBeInViewport();
  });

  test('TC4: View on GitHub button opens repository in new tab', async ({ page, context }) => {
    // Click 'View on GitHub' button
    // Expected: Opens MirDB GitHub repository in new tab

    const githubButton = page.locator('[data-testid="cta-github"]');
    await expect(githubButton).toBeVisible();
    await expect(githubButton).toHaveText(/view on github|github/i);

    // Verify it has the correct href and opens in new tab
    await expect(githubButton).toHaveAttribute('href', /github\.com\/yetone\/mirdb/);
    await expect(githubButton).toHaveAttribute('target', '_blank');

    // Verify it has proper security attributes for external links
    await expect(githubButton).toHaveAttribute('rel', /noopener/);
  });

  test('Hero section content is above the fold', async ({ page }) => {
    // Additional test to ensure hero content is above the fold (visible without scrolling)
    const heroSection = page.locator('[data-testid="hero-section"]');
    const logo = page.locator('[data-testid="hero-logo"]');
    const tagline = page.locator('[data-testid="hero-tagline"]');
    const getStartedButton = page.locator('[data-testid="cta-get-started"]');
    const githubButton = page.locator('[data-testid="cta-github"]');

    // All elements should be in the viewport on page load
    await expect(heroSection).toBeInViewport();
    await expect(logo).toBeInViewport();
    await expect(tagline).toBeInViewport();
    await expect(getStartedButton).toBeInViewport();
    await expect(githubButton).toBeInViewport();
  });

  test('Hero section displays MirDB product name', async ({ page }) => {
    // Verify the product name "MirDB" is prominently displayed
    const productName = page.locator('[data-testid="hero-product-name"]');
    await expect(productName).toBeVisible();
    await expect(productName).toHaveText(/mirdb/i);
  });
});
