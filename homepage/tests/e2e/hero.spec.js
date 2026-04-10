/**
 * Hero Section E2E Tests
 * Owner: Scenario 1 - Hero Section
 *
 * Test cases:
 * - TC1: Hero section displays with MirDB branding and tagline
 * - TC3: Get Started CTA smooth scrolls to Getting Started section
 * - TC4: GitHub CTA opens repository in new tab
 */

import { test, expect } from '@playwright/test';

test.describe('Hero Section - E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Hero section displays with MirDB logo/name and tagline', async ({ page }) => {
    // Check hero section is visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Check MirDB name is displayed
    const heroTitle = page.locator('.hero-title');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toContainText('MirDB');

    // Check tagline mentions persistent key-value store
    const tagline = page.locator('.hero-tagline');
    await expect(tagline).toBeVisible();
    const taglineText = await tagline.textContent();
    expect(taglineText.toLowerCase()).toContain('persistent');
    expect(taglineText.toLowerCase()).toContain('key-value');

    // Check Memcached compatibility is mentioned
    expect(taglineText.toLowerCase()).toContain('memcached');
  });

  test('TC3: Get Started CTA button smooth scrolls to Getting Started section', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click Get Started button
    const getStartedBtn = page.locator('.hero-cta .btn-primary');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toContainText('Get Started');

    // Click and wait for scroll
    await getStartedBtn.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(1000);

    // Verify scroll happened
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    // Verify Getting Started section is now in viewport
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeInViewport();
  });

  test('TC4: GitHub CTA button opens MirDB repository in new tab', async ({ page, context }) => {
    // Get the GitHub button
    const githubBtn = page.locator('.hero-cta .btn-secondary');
    await expect(githubBtn).toBeVisible();
    await expect(githubBtn).toContainText('GitHub');

    // Verify target="_blank" for new tab
    await expect(githubBtn).toHaveAttribute('target', '_blank');

    // Verify href points to GitHub repository
    const href = await githubBtn.getAttribute('href');
    expect(href).toContain('github.com');
    expect(href).toContain('mirdb');

    // Verify rel="noopener" for security
    const rel = await githubBtn.getAttribute('rel');
    expect(rel).toContain('noopener');

    // Test that clicking opens a new page
    const pagePromise = context.waitForEvent('page');
    await githubBtn.click();

    // Wait for the new page to open
    const newPage = await pagePromise;

    // Verify new page URL contains GitHub
    expect(newPage.url()).toContain('github.com');
  });

  test('Hero section is visible above the fold', async ({ page }) => {
    // Hero should be immediately visible without scrolling
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeInViewport();

    // Verify hero content is visible
    const heroContent = page.locator('.hero-content');
    await expect(heroContent).toBeInViewport();

    // Verify CTA buttons are visible
    const ctaButtons = page.locator('.hero-cta');
    await expect(ctaButtons).toBeInViewport();
  });

  test('Hero section displays key feature highlights', async ({ page }) => {
    const featuresList = page.locator('.hero-features');
    await expect(featuresList).toBeVisible();

    // Check there are 3-5 feature items
    const features = featuresList.locator('li');
    const count = await features.count();
    expect(count).toBeGreaterThanOrEqual(3);
    expect(count).toBeLessThanOrEqual(5);

    // Verify all feature items are visible
    for (let i = 0; i < count; i++) {
      await expect(features.nth(i)).toBeVisible();
    }
  });
});
