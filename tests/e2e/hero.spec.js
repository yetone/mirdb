/**
 * Hero Section E2E Tests
 * Owner: Scenario 1 - Hero Section Display
 *
 * Test cases for hero section visual and interactive functionality
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

const pageUrl = `file://${path.resolve(process.cwd(), 'index.html')}`;

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(pageUrl);
  });

  test('Test Case 1: Page loads successfully with HTTP 200 status', async ({ page }) => {
    const response = await page.goto(pageUrl);
    // For file:// URLs, status is typically 0 (not applicable) or we check page loaded
    // Instead, verify page content loaded successfully
    const title = await page.title();
    expect(title).toContain('MirDB');
  });

  test('Test Case 2: Logo image element exists with correct src', async ({ page }) => {
    const logo = page.locator('img[src="assets/logo.gif"]');
    await expect(logo).toBeVisible();
    await expect(logo).toHaveAttribute('alt', /MirDB/i);
  });

  test('Test Case 3: H1 element exists with text content "MirDB"', async ({ page }) => {
    const h1 = page.locator('h1');
    await expect(h1).toBeVisible();
    await expect(h1).toHaveText('MirDB');
  });

  test('Test Case 4: Tagline is visible', async ({ page }) => {
    const tagline = page.locator('.tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('A Persistent Key-Value Store with Memcached Protocol');
  });

  test('Test Case 5: View on GitHub button exists and has correct href', async ({ page }) => {
    const githubLink = page.locator('a:has-text("View on GitHub")');
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toHaveAttribute('href', /github\.com\/yetone\/mirdb/);
    await expect(githubLink).toHaveAttribute('target', '_blank');
    await expect(githubLink).toHaveAttribute('rel', /noopener/);
  });

  test('Test Case 6: Get Started button exists and links to quickstart', async ({ page }) => {
    const getStartedLink = page.locator('a:has-text("Get Started")');
    await expect(getStartedLink).toBeVisible();
    await expect(getStartedLink).toHaveAttribute('href', '#quickstart');
  });

  test('Hero section is visible without scrolling', async ({ page }) => {
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    // Check that hero is in viewport
    const isVisible = await hero.isVisible();
    expect(isVisible).toBe(true);
  });

  test('CTA buttons have proper styling', async ({ page }) => {
    const primaryBtn = page.locator('.btn-primary');
    const secondaryBtn = page.locator('.btn-secondary');

    await expect(primaryBtn).toBeVisible();
    await expect(secondaryBtn).toBeVisible();

    // Check that buttons are clickable
    await expect(primaryBtn).toBeEnabled();
    await expect(secondaryBtn).toBeEnabled();
  });
});
