import { test, expect } from '@playwright/test';

test.describe('Navigation Functionality', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Navigation bar is visible at top of page', async ({ page }) => {
    // Check that the navigation bar exists and is visible
    const navbar = page.locator('nav.navbar');
    await expect(navbar).toBeVisible();

    // Verify it has proper role and aria-label for accessibility
    await expect(navbar).toHaveAttribute('role', 'navigation');
    await expect(navbar).toHaveAttribute('aria-label', 'main navigation');

    // Verify navigation is at the top of the page (fixed positioning)
    const navBox = await navbar.boundingBox();
    expect(navBox).not.toBeNull();
    expect(navBox!.y).toBeLessThanOrEqual(10); // Should be at or near top
  });

  test('TC2: Logo or MirDB text is present in navigation', async ({ page }) => {
    // Check for the logo/product name in navigation
    const navLogo = page.locator('.nav-logo');
    await expect(navLogo).toBeVisible();
    await expect(navLogo).toHaveText('MirDB');

    // Verify the logo links to homepage
    await expect(navLogo).toHaveAttribute('href', '#');
  });

  test('TC3: Click Features nav link scrolls to features section', async ({ page }) => {
    // Get the features section
    const featuresSection = page.locator('#features');

    // Initially verify we're at the top of the page
    await expect(page.locator('.hero')).toBeInViewport();

    // Click the Features link in navigation
    await page.click('.nav-links a[href="#features"]');

    // Wait for smooth scroll animation
    await page.waitForTimeout(500);

    // Verify the features section is now in view
    await expect(featuresSection).toBeInViewport();
  });

  test('TC4: Click Getting Started nav link scrolls to getting started section', async ({ page }) => {
    // Get the getting started section
    const gettingStartedSection = page.locator('#getting-started');

    // Initially verify we're at the top of the page
    await expect(page.locator('.hero')).toBeInViewport();

    // Click the Getting Started link in navigation
    await page.click('.nav-links a[href="#getting-started"]');

    // Wait for smooth scroll animation
    await page.waitForTimeout(500);

    // Verify the getting started section is now in view
    await expect(gettingStartedSection).toBeInViewport();
  });

  test('TC5: GitHub link has href to repository and opens in new tab', async ({ page }) => {
    // Find the GitHub link in navigation
    const githubLink = page.locator('.nav-links a:has-text("GitHub")');
    await expect(githubLink).toBeVisible();

    // Verify it has the correct href to the repository
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

    // Verify it opens in a new tab (target="_blank")
    await expect(githubLink).toHaveAttribute('target', '_blank');

    // Verify security attributes for external links
    await expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  test('All navigation links are present', async ({ page }) => {
    // Verify all expected navigation items are present
    const navLinks = page.locator('.nav-links a');

    // Count should match expected number of links
    await expect(navLinks).toHaveCount(5);

    // Check each navigation item
    await expect(page.locator('.nav-links a[href="#features"]')).toBeVisible();
    await expect(page.locator('.nav-links a[href="#architecture"]')).toBeVisible();
    await expect(page.locator('.nav-links a[href="#getting-started"]')).toBeVisible();
    await expect(page.locator('.nav-links a[href="#commands"]')).toBeVisible();
    await expect(page.locator('.nav-links a:has-text("GitHub")')).toBeVisible();
  });

  test('Architecture nav link scrolls to architecture section', async ({ page }) => {
    // Get the architecture section
    const architectureSection = page.locator('#architecture');

    // Click the Architecture link in navigation
    await page.click('.nav-links a[href="#architecture"]');

    // Wait for smooth scroll animation
    await page.waitForTimeout(500);

    // Verify the architecture section is now in view
    await expect(architectureSection).toBeInViewport();
  });

  test('Commands nav link scrolls to commands section', async ({ page }) => {
    // Get the commands section
    const commandsSection = page.locator('#commands');

    // Click the Commands link in navigation
    await page.click('.nav-links a[href="#commands"]');

    // Wait for smooth scroll animation
    await page.waitForTimeout(500);

    // Verify the commands section is now in view
    await expect(commandsSection).toBeInViewport();
  });
});
