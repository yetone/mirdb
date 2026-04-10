/**
 * Hero Section E2E Tests
 * Owner: Scenario 1 - Hero Section
 *
 * Test cases:
 * - Test Case 1: Hero section displays with MirDB branding and tagline
 * - Test Case 3: Get Started CTA smooth scrolls to Getting Started section
 * - Test Case 4: GitHub CTA opens repository in new tab
 */

import { test, expect } from '@playwright/test';

test.describe('Hero Section - Value Proposition Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Hero section displays with MirDB logo/name and tagline', async ({ page }) => {
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

  test('Hero section is visible above the fold', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 800 });

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

  test('Hero section displays 3-5 key feature highlights', async ({ page }) => {
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

test.describe('Hero Section - CTA Buttons', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 3: Get Started CTA button smooth scrolls to Getting Started section', async ({ page }) => {
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

  test('Get Started button has correct href attribute', async ({ page }) => {
    const getStartedBtn = page.locator('.hero-cta a:has-text("Get Started")');
    await expect(getStartedBtn).toHaveAttribute('href', '#getting-started');
  });

  test('Get Started button has primary styling', async ({ page }) => {
    const getStartedBtn = page.locator('.hero-cta .btn-primary');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toContainText('Get Started');
  });

  test('Test Case 4: GitHub CTA button opens MirDB repository in new tab', async ({ page, context }) => {
    // Get the GitHub button
    const githubBtn = page.locator('.hero-cta .btn-secondary');
    await expect(githubBtn).toBeVisible();
    await expect(githubBtn).toContainText('GitHub');

    // Verify target="_blank" for new tab
    await expect(githubBtn).toHaveAttribute('target', '_blank');

    // Verify href points to GitHub repository
    const href = await githubBtn.getAttribute('href');
    expect(href).toContain('github.com');
    expect(href.toLowerCase()).toContain('mirdb');

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

  test('GitHub button has secondary styling', async ({ page }) => {
    const githubBtn = page.locator('.hero-cta .btn-secondary');
    await expect(githubBtn).toBeVisible();
    const text = await githubBtn.textContent();
    expect(text.toLowerCase()).toContain('github');
  });

  test('Both CTA buttons are visible and properly positioned', async ({ page }) => {
    const ctaContainer = page.locator('.hero-cta');
    await expect(ctaContainer).toBeVisible();

    const buttons = page.locator('.hero-cta a');
    await expect(buttons).toHaveCount(2);

    // Both buttons should be visible
    await expect(buttons.nth(0)).toBeVisible();
    await expect(buttons.nth(1)).toBeVisible();

    // Buttons should be on the same row (flex layout)
    const firstBox = await buttons.nth(0).boundingBox();
    const secondBox = await buttons.nth(1).boundingBox();
    expect(Math.abs(firstBox.y - secondBox.y)).toBeLessThan(20);
  });
});

test.describe('Hero Section - Styling and Layout', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Hero section has proper content wrapper for centering', async ({ page }) => {
    const heroContent = page.locator('.hero-content');
    await expect(heroContent).toBeVisible();

    // Check max-width is set for readable line length
    const maxWidth = await heroContent.evaluate(el =>
      window.getComputedStyle(el).maxWidth
    );
    expect(maxWidth).not.toBe('none');
  });

  test('Hero section is centered on page', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 800 });

    const heroSection = page.locator('.hero');
    const textAlign = await heroSection.evaluate(el =>
      window.getComputedStyle(el).textAlign
    );
    expect(textAlign).toBe('center');
  });
});
