// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for MirDB Homepage Hero Section
 * Tests based on REQ-1: Display product name, logo, and tagline prominently in the hero section
 */

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage before each test
    await page.goto('http://localhost:3000');
  });

  test('TC1: Hero section contains MirDB logo, product name, and tagline', async ({ page }) => {
    // Test Case 1: Load homepage and inspect hero section
    // Expected: Hero section contains MirDB logo, product name, and tagline

    // Verify hero section exists
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify MirDB logo is present
    const logo = page.locator('[data-testid="hero-logo"]');
    await expect(logo).toBeVisible();

    // Verify product name "MirDB" is displayed
    const productName = page.locator('[data-testid="hero-product-name"]');
    await expect(productName).toBeVisible();
    await expect(productName).toContainText('MirDB');

    // Verify tagline contains key messaging about persistent key-value store with memcached protocol
    const tagline = page.locator('[data-testid="hero-tagline"]');
    await expect(tagline).toBeVisible();
    const taglineText = await tagline.textContent();
    expect(taglineText?.toLowerCase()).toMatch(/persistent.*key.*value.*store|memcached.*protocol/i);
  });

  test('TC2: Get Started button is visible and functional', async ({ page }) => {
    // Test Case 2: Check for 'Get Started' button in hero section
    // Expected: Button is visible, clickable, and navigates to quick-start section or documentation

    const heroSection = page.locator('[data-testid="hero-section"]');

    // Verify "Get Started" button exists in hero section
    const getStartedButton = heroSection.locator('[data-testid="cta-get-started"]');
    await expect(getStartedButton).toBeVisible();
    await expect(getStartedButton).toBeEnabled();

    // Verify button text
    await expect(getStartedButton).toContainText(/get started/i);

    // Verify button has a valid href (either anchor link or external link)
    const href = await getStartedButton.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toMatch(/^(#|http)/);
  });

  test('TC3: GitHub button is visible and links to repository', async ({ page }) => {
    // Test Case 3: Check for 'GitHub' button in hero section
    // Expected: Button is visible, clickable, and links to MirDB GitHub repository

    const heroSection = page.locator('[data-testid="hero-section"]');

    // Verify "GitHub" button exists in hero section
    const githubButton = heroSection.locator('[data-testid="cta-github"]');
    await expect(githubButton).toBeVisible();
    await expect(githubButton).toBeEnabled();

    // Verify button text contains GitHub reference
    await expect(githubButton).toContainText(/github/i);

    // Verify button links to GitHub (external link)
    const href = await githubButton.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toMatch(/github\.com/i);
  });

  test('TC4: Hero section is above the fold on desktop (1920x1080)', async ({ page }) => {
    // Test Case 4: Verify hero section is above the fold on desktop (1920x1080)
    // Expected: Hero section is fully visible without scrolling

    // Set viewport to 1920x1080 (desktop)
    await page.setViewportSize({ width: 1920, height: 1080 });

    // Reload to ensure proper rendering at new viewport size
    await page.goto('http://localhost:3000');

    // Verify hero section is visible
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Get the bounding box of the hero section
    const boundingBox = await heroSection.boundingBox();
    expect(boundingBox).toBeTruthy();

    // Verify the entire hero section is within the viewport (above the fold)
    // The bottom of the hero section should be within the viewport height
    const viewportHeight = 1080;
    expect(boundingBox.y).toBeGreaterThanOrEqual(0);
    expect(boundingBox.y + boundingBox.height).toBeLessThanOrEqual(viewportHeight);

    // Verify hero section is at the top of the page (first major content area)
    // Hero section should start within the first 200 pixels (accounting for nav bar)
    expect(boundingBox.y).toBeLessThan(200);
  });
});
