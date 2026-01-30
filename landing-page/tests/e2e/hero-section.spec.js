/**
 * Hero Section E2E Tests
 * Owner: Scenario 1 - Hero Section Display
 */
const { test, expect } = require('@playwright/test');

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Hero section visible with MirDB title and tagline', async ({ page }) => {
    // Verify hero section exists and is visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify product name (MirDB) is visible
    const heroTitle = page.locator('.hero-title');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toHaveText('MirDB');

    // Verify tagline contains key information
    const heroSubtitle = page.locator('.hero-subtitle');
    await expect(heroSubtitle).toBeVisible();
    await expect(heroSubtitle).toContainText('Persistent Key-Value Store');
    await expect(heroSubtitle).toContainText('Memcached Protocol');
  });

  test('TC2: Hero section contains h1 with product name, subtitle with tagline, and two CTA buttons', async ({ page }) => {
    // Verify h1 element with product name
    const h1 = page.locator('#hero h1');
    await expect(h1).toBeVisible();
    await expect(h1).toHaveText('MirDB');

    // Verify subtitle element
    const subtitle = page.locator('.hero-subtitle');
    await expect(subtitle).toBeVisible();

    // Verify two CTA buttons exist
    const ctaButtons = page.locator('.hero-cta .btn');
    await expect(ctaButtons).toHaveCount(2);

    // Verify button text
    const getStartedBtn = page.locator('.hero-cta .btn-primary');
    await expect(getStartedBtn).toHaveText('Get Started');

    const githubBtn = page.locator('.hero-cta .btn-secondary');
    await expect(githubBtn).toHaveText('View on GitHub');
  });

  test('TC3: Click Get Started button smooth scrolls to Getting Started section', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);
    expect(initialScrollY).toBe(0);

    // Click Get Started button
    const getStartedBtn = page.locator('.hero-cta .btn-primary');
    await getStartedBtn.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(1000);

    // Verify page scrolled to getting-started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeInViewport();

    // Verify URL hash is updated
    const url = page.url();
    expect(url).toContain('#getting-started');
  });

  test('TC4: Click View on GitHub button opens GitHub repository in new tab', async ({ page, context }) => {
    // Listen for new page (tab) to open
    const pagePromise = context.waitForEvent('page');

    // Click View on GitHub button
    const githubBtn = page.locator('.hero-cta .btn-secondary');
    await githubBtn.click();

    // Wait for new tab to open
    const newPage = await pagePromise;
    await newPage.waitForLoadState();

    // Verify new tab URL is GitHub repository
    const newPageUrl = newPage.url();
    expect(newPageUrl).toContain('github.com/yetone/mirdb');
  });

  test('Hero section logo is visible', async ({ page }) => {
    const heroLogo = page.locator('.hero-logo');
    await expect(heroLogo).toBeVisible();
    await expect(heroLogo).toHaveAttribute('src', 'assets/images/logo.gif');
    await expect(heroLogo).toHaveAttribute('alt', 'MirDB Logo');
  });

  test('Hero section is above the fold', async ({ page }) => {
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeInViewport();

    // Verify hero content is visible without scrolling
    const heroTitle = page.locator('.hero-title');
    await expect(heroTitle).toBeInViewport();

    const ctaButtons = page.locator('.hero-cta');
    await expect(ctaButtons).toBeInViewport();
  });

  test('CTA buttons are properly styled', async ({ page }) => {
    // Primary button (Get Started)
    const primaryBtn = page.locator('.hero-cta .btn-primary');
    await expect(primaryBtn).toHaveCSS('cursor', 'pointer');

    // Secondary button (View on GitHub)
    const secondaryBtn = page.locator('.hero-cta .btn-secondary');
    await expect(secondaryBtn).toHaveCSS('cursor', 'pointer');

    // Verify buttons have proper attributes
    await expect(secondaryBtn).toHaveAttribute('target', '_blank');
    await expect(secondaryBtn).toHaveAttribute('rel', 'noopener noreferrer');
  });
});
