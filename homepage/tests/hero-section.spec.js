// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Hero Section Value Proposition Display Tests
 *
 * Scenario: Verify that the hero section effectively communicates MirDB's
 * value proposition with clear headline, tagline, and CTAs
 */

test.describe('Hero Section Value Proposition Display', () => {
  test.beforeEach(async ({ page }) => {
    // Step 1: Load homepage - Navigate to the MirDB homepage root URL
    await page.goto('/');
  });

  /**
   * Test Case 1: Check for product name 'MirDB' in hero section
   * Expected: Product name 'MirDB' is prominently displayed in the hero section
   */
  test('should display product name MirDB prominently in hero section', async ({ page }) => {
    // Wait for hero section to be visible
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Check that MirDB product name is prominently displayed in the hero section's h1
    const heroHeading = heroSection.locator('h1');
    await expect(heroHeading).toBeVisible();

    // Verify it contains MirDB
    const headingText = await heroHeading.textContent();
    expect(headingText).toMatch(/MirDB/i);
  });

  /**
   * Test Case 2: Check for tagline containing 'persistent' and 'Memcached'
   * Expected: Tagline communicates persistent key-value store with Memcached compatibility
   */
  test('should display tagline with persistent and Memcached keywords', async ({ page }) => {
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Check for tagline containing both "persistent" and "Memcached"
    const heroText = await heroSection.textContent();
    expect(heroText.toLowerCase()).toContain('persistent');
    expect(heroText.toLowerCase()).toContain('memcached');

    // Verify the tagline element specifically
    const tagline = heroSection.locator('.hero-tagline');
    await expect(tagline).toBeVisible();
    const taglineText = await tagline.textContent();
    expect(taglineText.toLowerCase()).toContain('persistent');
    expect(taglineText.toLowerCase()).toContain('memcached');
  });

  /**
   * Test Case 3: Click 'Get Started' primary CTA button
   * Expected: Button is visible, clickable, and navigates to getting started section or documentation
   */
  test('should have functional Get Started primary CTA button', async ({ page }) => {
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Find the Get Started button
    const getStartedButton = heroSection.locator('a.btn-primary, a:has-text("Get Started")').first();

    await expect(getStartedButton).toBeVisible();

    // Verify the button has a valid href
    const href = await getStartedButton.getAttribute('href');
    expect(href).toMatch(/(#quickstart|#getting-started|\/docs)/i);

    // Test that clicking works
    await getStartedButton.click();

    // After clicking, should scroll to quickstart section
    await page.waitForTimeout(500);

    // Verify we navigated (URL should contain the hash)
    const url = page.url();
    expect(url).toContain('#quickstart');
  });

  /**
   * Test Case 4: Click 'View on GitHub' secondary CTA button
   * Expected: Button opens GitHub repository in new tab
   */
  test('should have functional View on GitHub secondary CTA button', async ({ page }) => {
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Find the GitHub button/link
    const githubButton = heroSection.locator('a:has-text("GitHub"), a:has-text("View on GitHub")').first();

    await expect(githubButton).toBeVisible();

    // Verify the button links to GitHub
    const href = await githubButton.getAttribute('href');
    expect(href).toMatch(/github\.com/i);

    // Verify it opens in new tab
    const target = await githubButton.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify it has noopener for security
    const rel = await githubButton.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  /**
   * Test Case 5: Verify hero section visibility above the fold
   * This validates the user can understand value proposition quickly
   * Note: The manual part (10 seconds to understand) is validated by checking
   * all key elements are visible without scrolling
   */
  test('should display hero section above the fold with all key elements visible', async ({ page }) => {
    // Set viewport to standard desktop size
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.goto('/');

    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Check that hero section is above the fold (within viewport)
    const boundingBox = await heroSection.boundingBox();
    expect(boundingBox).not.toBeNull();
    expect(boundingBox.y).toBeLessThan(1080);
    expect(boundingBox.y).toBeGreaterThanOrEqual(0);

    // Verify all key elements are visible without scrolling:
    // 1. Product name (in hero section h1)
    const productName = heroSection.locator('h1');
    await expect(productName).toBeVisible();
    await expect(productName).toBeInViewport();

    // 2. Tagline with value proposition
    const tagline = heroSection.locator('.hero-tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toBeInViewport();

    // 3. Primary CTA
    const getStartedButton = heroSection.locator('a.btn-primary, a:has-text("Get Started")').first();
    await expect(getStartedButton).toBeVisible();
    await expect(getStartedButton).toBeInViewport();

    // 4. Secondary CTA (GitHub)
    const githubButton = heroSection.locator('a:has-text("GitHub")').first();
    await expect(githubButton).toBeVisible();
    await expect(githubButton).toBeInViewport();
  });
});
