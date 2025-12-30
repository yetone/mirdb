// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: Header and Navigation Display
 * Scenario: Verify that the header section displays the MirDB product name, logo, and tagline prominently,
 * with functional navigation links
 */

test.describe('Header and Navigation Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Header contains MirDB logo/name text visible and prominently displayed
   * Input: Load landing page and inspect header element
   * Expected: Header contains MirDB logo/name text visible and prominently displayed
   */
  test('TC1: Header contains MirDB logo/name text visible and prominently displayed', async ({ page }) => {
    // Verify header element exists
    const header = page.locator('header.header');
    await expect(header).toBeVisible();

    // Verify MirDB logo/name is visible
    const logoText = page.locator('.logo-text');
    await expect(logoText).toBeVisible();
    await expect(logoText).toHaveText('MirDB');

    // Verify logo image is present
    const logoImage = page.locator('.logo-image');
    await expect(logoImage).toBeVisible();
    await expect(logoImage).toHaveAttribute('alt', 'MirDB Logo');

    // Verify tagline is visible
    const tagline = page.locator('.tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Persistent Key-Value Store');

    // Verify header is prominently displayed (check it's at the top)
    const headerBox = await header.boundingBox();
    expect(headerBox).not.toBeNull();
    expect(headerBox.y).toBeLessThanOrEqual(10); // Header should be at the top

    // Verify logo text has prominent styling (larger font)
    const fontSize = await logoText.evaluate(el => window.getComputedStyle(el).fontSize);
    const fontSizeValue = parseFloat(fontSize);
    expect(fontSizeValue).toBeGreaterThanOrEqual(24); // At least 24px font size
  });

  /**
   * Test Case 2: Navigation contains links: Features, Getting Started, Documentation, GitHub
   * Input: Check for navigation links in header
   * Expected: Navigation contains links: Features, Getting Started, Documentation, GitHub
   */
  test('TC2: Navigation contains all required links', async ({ page }) => {
    // Verify navigation element exists
    const nav = page.locator('nav.main-nav');
    await expect(nav).toBeVisible();

    // Verify all required navigation links are present and visible
    const featuresLink = page.locator('.nav-link', { hasText: 'Features' });
    await expect(featuresLink).toBeVisible();
    await expect(featuresLink).toHaveAttribute('href', '#features');

    const gettingStartedLink = page.locator('.nav-link', { hasText: 'Getting Started' });
    await expect(gettingStartedLink).toBeVisible();
    await expect(gettingStartedLink).toHaveAttribute('href', '#getting-started');

    const documentationLink = page.locator('.nav-link', { hasText: 'Documentation' });
    await expect(documentationLink).toBeVisible();
    await expect(documentationLink).toHaveAttribute('href', '#documentation');

    const githubLink = page.locator('.nav-link', { hasText: 'GitHub' });
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

    // Verify navigation list contains exactly 4 items
    const navItems = page.locator('.nav-list li');
    await expect(navItems).toHaveCount(4);
  });

  /**
   * Test Case 3: Click 'Features' navigation link scrolls to Features section
   * Input: Click 'Features' navigation link
   * Expected: Page scrolls to or displays the Features section
   */
  test('TC3: Features navigation link scrolls to Features section', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click on Features link
    const featuresLink = page.locator('.nav-link', { hasText: 'Features' });
    await featuresLink.click();

    // Wait for scroll animation to complete
    await page.waitForTimeout(500);

    // Verify Features section is now visible in viewport
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify the page has scrolled (if features section is not at the top)
    const featuresBox = await featuresSection.boundingBox();
    expect(featuresBox).not.toBeNull();

    // Features section should now be near the top of the viewport
    expect(featuresBox.y).toBeLessThan(200);

    // Verify the Features section heading is visible
    const featuresHeading = page.locator('#features h2');
    await expect(featuresHeading).toBeVisible();
    await expect(featuresHeading).toHaveText('Features');
  });

  /**
   * Test Case 4: Click 'GitHub' navigation link opens GitHub repository
   * Input: Click 'GitHub' navigation link
   * Expected: Link opens GitHub repository in new tab or navigates to GitHub
   */
  test('TC4: GitHub navigation link has correct href and target attributes', async ({ page }) => {
    // Find the GitHub link
    const githubLink = page.locator('.nav-link.github-link');
    await expect(githubLink).toBeVisible();

    // Verify href points to GitHub repository
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

    // Verify link opens in new tab
    await expect(githubLink).toHaveAttribute('target', '_blank');

    // Verify rel attribute for security (noopener noreferrer)
    await expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');

    // Test that clicking opens a new page (without actually navigating away)
    const [newPage] = await Promise.all([
      page.context().waitForEvent('page'),
      githubLink.click()
    ]);

    // Verify new page was opened
    expect(newPage).toBeTruthy();

    // Verify the new page URL contains github.com/yetone/mirdb
    const newPageUrl = newPage.url();
    expect(newPageUrl).toContain('github.com/yetone/mirdb');

    // Close the new page
    await newPage.close();
  });

  /**
   * Additional accessibility test for navigation
   */
  test('Navigation has proper accessibility attributes', async ({ page }) => {
    // Verify header has banner role
    const header = page.locator('header.header');
    await expect(header).toHaveAttribute('role', 'banner');

    // Verify nav has navigation role and aria-label
    const nav = page.locator('nav.main-nav');
    await expect(nav).toHaveAttribute('role', 'navigation');
    await expect(nav).toHaveAttribute('aria-label', 'Main navigation');

    // Verify logo link has aria-label
    const logoLink = page.locator('.logo-link');
    await expect(logoLink).toHaveAttribute('aria-label', 'MirDB Home');
  });
});
