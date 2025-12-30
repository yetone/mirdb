// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for MirDB Homepage Navigation Bar Functionality
 * Scenario: Verify that the navigation bar functions correctly with links to key sections
 */

test.describe('Navigation Bar Functionality', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage before each test
    await page.goto('http://localhost:3000');
  });

  test('TC1: Navigation bar displays MirDB logo', async ({ page }) => {
    /**
     * Test Case 1: Check navigation bar contains Logo
     * Expected: Navigation bar displays MirDB logo
     */

    // Verify navigation bar exists
    const navbar = page.locator('nav.navbar');
    await expect(navbar).toBeVisible();

    // Verify logo link exists in navigation
    const logoLink = page.locator('nav.navbar .nav-logo');
    await expect(logoLink).toBeVisible();

    // Verify logo image is displayed
    const logoImg = page.locator('nav.navbar .nav-logo-img');
    await expect(logoImg).toBeVisible();

    // Verify logo text "MirDB" is displayed
    const logoText = page.locator('nav.navbar .nav-logo-text');
    await expect(logoText).toBeVisible();
    await expect(logoText).toHaveText('MirDB');

    // Verify logo link points to top of page (home)
    const href = await logoLink.getAttribute('href');
    expect(href).toBe('#');
  });

  test('TC2: Navigation has Features link that scrolls to features section', async ({ page }) => {
    /**
     * Test Case 2: Check navigation bar contains Features link
     * Expected: Navigation has Features link that scrolls to features section
     */

    // Verify navigation bar exists
    const navbar = page.locator('nav.navbar');
    await expect(navbar).toBeVisible();

    // Verify Features link exists
    const featuresLink = page.locator('nav.navbar .nav-links a[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await expect(featuresLink).toHaveText('Features');

    // Click the Features link
    await featuresLink.click();

    // Verify URL includes #features
    await expect(page).toHaveURL(/.*#features/);

    // Verify features section is visible
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify features section is in viewport (scrolled into view)
    await expect(featuresSection).toBeInViewport();
  });

  test('TC3: Navigation has Docs link pointing to documentation', async ({ page }) => {
    /**
     * Test Case 3: Check navigation bar contains Docs link
     * Expected: Navigation has Docs link pointing to documentation
     */

    // Verify navigation bar exists
    const navbar = page.locator('nav.navbar');
    await expect(navbar).toBeVisible();

    // Verify Docs link exists
    const docsLink = page.locator('nav.navbar .nav-links a:has-text("Docs")');
    await expect(docsLink).toBeVisible();

    // Verify Docs link has a valid href pointing to documentation
    const href = await docsLink.getAttribute('href');
    expect(href).toBeTruthy();

    // Docs should link to GitHub README (the documentation) or similar
    expect(href).toMatch(/github\.com.*readme|#docs|docs/i);
  });

  test('TC4: Navigation has GitHub link opening repository in new tab', async ({ page }) => {
    /**
     * Test Case 4: Check navigation bar contains GitHub link
     * Expected: Navigation has GitHub link opening repository in new tab
     */

    // Verify navigation bar exists
    const navbar = page.locator('nav.navbar');
    await expect(navbar).toBeVisible();

    // Verify GitHub link exists
    const githubLink = page.locator('nav.navbar .nav-links a:has-text("GitHub")');
    await expect(githubLink).toBeVisible();

    // Verify GitHub link points to MirDB repository
    const href = await githubLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Verify link opens in new tab
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify security attributes for external link
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toBeTruthy();
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');
  });

  test('Logo link returns to top of page', async ({ page }) => {
    /**
     * Additional test: Verify logo acts as home link
     * Expected: Clicking logo returns user to top of page
     */

    // First scroll down to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Click the logo to go back to top
    const logoLink = page.locator('nav.navbar .nav-logo');
    await logoLink.click();

    // Verify URL has # (top of page)
    await expect(page).toHaveURL(/.*#$/);

    // Verify the hero section is visible (user is at top)
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeInViewport();
  });

  test('Navigation bar is sticky and visible when scrolling', async ({ page }) => {
    /**
     * Verify navigation is always accessible
     */

    // Scroll down to bottom of page
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));

    // Wait for scroll to complete
    await page.waitForTimeout(100);

    // Verify navigation bar is still visible
    const navbar = page.locator('nav.navbar');
    await expect(navbar).toBeVisible();
    await expect(navbar).toBeInViewport();
  });

  test('Navigation bar is at top of page', async ({ page }) => {
    /**
     * Verify navigation bar is positioned at the top
     */

    const navbar = page.locator('nav.navbar');
    await expect(navbar).toBeVisible();

    // Get bounding box to verify position
    const boundingBox = await navbar.boundingBox();
    expect(boundingBox).toBeTruthy();

    // Navigation should be at the very top of the page
    expect(boundingBox.y).toBeLessThanOrEqual(10);
  });

  test('All navigation links are functional', async ({ page }) => {
    /**
     * Integration test: Verify all nav links work correctly
     */

    const navLinks = page.locator('nav.navbar .nav-links a');
    const count = await navLinks.count();

    // Should have at least 3 links (Features, Docs, GitHub)
    expect(count).toBeGreaterThanOrEqual(3);

    // Verify all links have valid href
    for (let i = 0; i < count; i++) {
      const link = navLinks.nth(i);
      const href = await link.getAttribute('href');
      expect(href).toBeTruthy();
      expect(href.trim()).not.toBe('');
    }
  });
});
