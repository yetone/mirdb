/**
 * E2E tests for Hero section navigation.
 * Owner: Scenario 1 - Hero Section Display
 *
 * Tests:
 * - Hero section loads with all elements visible
 * - Get Started button scrolls to installation section
 * - View on GitHub button opens in new tab
 */

import { test, expect } from '@playwright/test';

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/mirdb/');
  });

  test('hero section renders with logo, tagline, and CTA buttons visible above the fold', async ({ page }) => {
    // Check hero section exists and is visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Check logo is present and visible
    const logo = page.locator('[data-testid="hero-logo"] img');
    await expect(logo).toBeVisible();
    await expect(logo).toHaveAttribute('alt', 'MirDB Logo');

    // Check tagline is visible with correct text
    const tagline = page.locator('[data-testid="hero-tagline"]');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('A Persistent Key-Value Store with Memcached Protocol');

    // Check both CTA buttons are visible
    const getStartedButton = page.locator('[data-testid="get-started-button"]');
    await expect(getStartedButton).toBeVisible();
    await expect(getStartedButton).toContainText('Get Started');

    const githubButton = page.locator('[data-testid="github-button"]');
    await expect(githubButton).toBeVisible();
    await expect(githubButton).toContainText('View on GitHub');

    // Verify all elements are above the fold (in viewport)
    await expect(heroSection).toBeInViewport();
  });

  test('clicking Get Started button scrolls smoothly to installation section', async ({ page }) => {
    // Get the initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the Get Started button
    const getStartedButton = page.locator('[data-testid="get-started-button"]');
    await getStartedButton.click();

    // Wait for smooth scroll animation to complete
    await page.waitForTimeout(1000);

    // Verify the page scrolled
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    // Verify the installation section is now in view
    const installationSection = page.locator('#installation');
    await expect(installationSection).toBeInViewport();
  });

  test('clicking View on GitHub button opens github.com/yetone/mirdb in a new tab', async ({ page, context }) => {
    // Set up listener for new page (popup/new tab)
    const pagePromise = context.waitForEvent('page');

    // Click the GitHub button
    const githubButton = page.locator('[data-testid="github-button"]');
    await expect(githubButton).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    await expect(githubButton).toHaveAttribute('target', '_blank');
    await expect(githubButton).toHaveAttribute('rel', /noopener/);

    await githubButton.click();

    // Wait for the new tab to open
    const newPage = await pagePromise;

    // Verify the new tab URL is the GitHub repository
    expect(newPage.url()).toContain('github.com/yetone/mirdb');
  });

  test('hero section has proper structure and semantic HTML', async ({ page }) => {
    // Check for single H1 in hero section
    const h1Elements = page.locator('#hero h1');
    await expect(h1Elements).toHaveCount(1);
    await expect(h1Elements).toContainText('MirDB');

    // Check hero is a section element
    const heroSection = page.locator('#hero');
    const tagName = await heroSection.evaluate((el) => el.tagName);
    expect(tagName).toBe('SECTION');
  });

  test('hero section displays correctly on mobile viewport', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });

    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    const logo = page.locator('[data-testid="hero-logo"] img');
    await expect(logo).toBeVisible();

    const tagline = page.locator('[data-testid="hero-tagline"]');
    await expect(tagline).toBeVisible();

    const ctaButtons = page.locator('[data-testid="hero-cta-buttons"]');
    await expect(ctaButtons).toBeVisible();

    // Buttons should stack vertically on mobile
    const getStartedButton = page.locator('[data-testid="get-started-button"]');
    const githubButton = page.locator('[data-testid="github-button"]');

    await expect(getStartedButton).toBeVisible();
    await expect(githubButton).toBeVisible();
  });

  test('hero section displays correctly on desktop viewport', async ({ page }) => {
    await page.setViewportSize({ width: 1440, height: 900 });

    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // All elements should be visible
    const logo = page.locator('[data-testid="hero-logo"] img');
    await expect(logo).toBeVisible();

    const tagline = page.locator('[data-testid="hero-tagline"]');
    await expect(tagline).toBeVisible();

    const getStartedButton = page.locator('[data-testid="get-started-button"]');
    const githubButton = page.locator('[data-testid="github-button"]');

    await expect(getStartedButton).toBeVisible();
    await expect(githubButton).toBeVisible();
  });

  test('hero terminal decoration is visible', async ({ page }) => {
    // Check for terminal-style decoration
    const terminal = page.locator('#hero .terminal');
    await expect(terminal).toBeVisible();

    // Check for terminal dots
    const terminalDots = page.locator('#hero .terminal-dot');
    await expect(terminalDots).toHaveCount(3);
  });
});
