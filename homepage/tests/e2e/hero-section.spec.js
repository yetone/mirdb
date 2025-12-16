// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Hero Section Display and Value Proposition
 * Scenario: Verify that the hero section displays the product name, tagline,
 * value proposition, and primary CTAs correctly as per REQ-1
 */

test.describe('Hero Section Display and Value Proposition', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/');
  });

  /**
   * Test Case 1: Hero section content validation
   * Input: Load homepage and inspect hero section
   * Expected: Hero section contains 'MirDB' headline, tagline about Memcached protocol compatibility,
   * and description mentioning Rust and LSM-tree
   */
  test('TC1: Hero section displays correct content (headline, tagline, description)', async ({ page }) => {
    // Verify hero section is visible
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify hero title contains 'MirDB'
    const heroTitle = page.locator('[data-testid="hero-title"]');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toContainText('MirDB');

    // Verify tagline contains the expected text about Memcached protocol compatibility
    const heroTagline = page.locator('[data-testid="hero-tagline"]');
    await expect(heroTagline).toBeVisible();
    await expect(heroTagline).toContainText('high-performance');
    await expect(heroTagline).toContainText('persistent key-value store');
    await expect(heroTagline).toContainText('Memcached protocol compatibility');

    // Verify description mentions Rust and LSM-tree
    const heroDescription = page.locator('[data-testid="hero-description"]');
    await expect(heroDescription).toBeVisible();
    await expect(heroDescription).toContainText('Rust');
    await expect(heroDescription).toContainText('LSM-tree');
  });

  /**
   * Test Case 2: Primary CTA button validation
   * Input: Check for primary CTA button
   * Expected: 'Get Started' button is visible, clickable, and has appropriate styling/hover states
   */
  test('TC2: Get Started button is visible, clickable, and has appropriate styling', async ({ page }) => {
    // Verify Get Started button exists and is visible
    const getStartedBtn = page.locator('[data-testid="get-started-btn"]');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toContainText('Get Started');

    // Verify button is enabled and clickable
    await expect(getStartedBtn).toBeEnabled();

    // Verify button has primary styling (white background on gradient hero)
    const backgroundColor = await getStartedBtn.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    // White background should have high RGB values
    expect(backgroundColor).toMatch(/rgb\(255,\s*255,\s*255\)|rgba\(255,\s*255,\s*255/);

    // Verify button has appropriate href
    const href = await getStartedBtn.getAttribute('href');
    expect(href).toBe('#getting-started');

    // Test hover state - button should remain interactable
    await getStartedBtn.hover();
    await expect(getStartedBtn).toBeVisible();

    // Verify clicking works (navigates to getting-started section)
    await getStartedBtn.click();
    // After clicking, the getting-started section should be visible
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeInViewport();
  });

  /**
   * Test Case 3: Secondary CTA button validation
   * Input: Check for secondary CTA button
   * Expected: 'View on GitHub' button is visible, clickable, and links to GitHub repository
   */
  test('TC3: View on GitHub button is visible, clickable, and links to GitHub', async ({ page }) => {
    // Verify GitHub button exists and is visible
    const githubBtn = page.locator('[data-testid="github-btn"]');
    await expect(githubBtn).toBeVisible();
    await expect(githubBtn).toContainText('View on GitHub');

    // Verify button is enabled and clickable
    await expect(githubBtn).toBeEnabled();

    // Verify button links to GitHub repository
    const href = await githubBtn.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Verify it opens in new tab (target="_blank")
    const target = await githubBtn.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify security attribute for external link
    const rel = await githubBtn.getAttribute('rel');
    expect(rel).toContain('noopener');

    // Verify button has secondary styling (transparent with white border)
    const backgroundColor = await githubBtn.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    // Transparent or rgba with 0 alpha
    expect(backgroundColor).toMatch(/transparent|rgba\(0,\s*0,\s*0,\s*0\)/);

    // Test hover state
    await githubBtn.hover();
    await expect(githubBtn).toBeVisible();
  });
});
