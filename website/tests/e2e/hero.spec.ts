import { test, expect } from '@playwright/test';

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Hero section contains MirDB logo image with alt text', async ({ page }) => {
    // Load homepage and query hero section
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify logo image exists with proper alt text
    const logo = heroSection.locator('img.hero__logo');
    await expect(logo).toBeVisible();
    await expect(logo).toHaveAttribute('alt', 'MirDB Logo');
    await expect(logo).toHaveAttribute('src', /mirdb-logo\.svg/);
  });

  test('TC2: Text MirDB is visible in hero section', async ({ page }) => {
    // Query hero section for product name
    const heroSection = page.locator('#hero');
    const title = heroSection.locator('.hero__title');

    await expect(title).toBeVisible();
    await expect(title).toHaveText('MirDB');
  });

  test('TC3: Tagline is displayed in hero section', async ({ page }) => {
    // Query hero section for tagline
    const heroSection = page.locator('#hero');
    const tagline = heroSection.locator('.hero__tagline');

    await expect(tagline).toBeVisible();
    await expect(tagline).toHaveText('A Persistent Key-Value Store with Memcached Protocol');
  });

  test('TC4: Quick Start CTA button exists and is clickable', async ({ page }) => {
    // Query for Quick Start CTA button
    const heroSection = page.locator('#hero');
    const quickStartBtn = heroSection.locator('a.hero__cta:has-text("Quick Start")');

    await expect(quickStartBtn).toBeVisible();
    await expect(quickStartBtn).toBeEnabled();

    // Verify it has the correct href to quick start section
    await expect(quickStartBtn).toHaveAttribute('href', '#quickstart');

    // Verify button is clickable (no errors on click)
    await quickStartBtn.click();

    // URL should now have #quickstart anchor
    await expect(page).toHaveURL(/#quickstart$/);
  });

  test('TC5: GitHub CTA button exists and links to repository', async ({ page }) => {
    // Query for GitHub CTA button
    const heroSection = page.locator('#hero');
    const githubBtn = heroSection.locator('a.hero__cta:has-text("View on GitHub")');

    await expect(githubBtn).toBeVisible();
    await expect(githubBtn).toBeEnabled();

    // Verify it links to GitHub (contains github.com)
    const href = await githubBtn.getAttribute('href');
    expect(href).toContain('github.com');

    // Verify it opens in new tab
    await expect(githubBtn).toHaveAttribute('target', '_blank');
    await expect(githubBtn).toHaveAttribute('rel', /noopener/);
  });
});
