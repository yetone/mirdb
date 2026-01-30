/**
 * Navigation E2E Tests
 * Owner: Scenario 2 - Navigation Bar Functionality
 *
 * End-to-end tests for navigation bar functionality including:
 * - Smooth scrolling to sections
 * - Mobile hamburger menu
 * - Navigation link interactions
 */

const { test, expect } = require('@playwright/test');

test.describe('Navigation Bar Functionality', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');
  });

  test('navigation bar displays logo and all navigation elements', async ({ page }) => {
    // Check logo
    const logo = page.locator('.nav-logo');
    await expect(logo).toBeVisible();

    // Check navigation links are present
    const featuresLink = page.locator('a[href="#features"]').first();
    const usageLink = page.locator('a[href="#usage"]').first();
    const architectureLink = page.locator('a[href="#architecture"]').first();
    const getStartedLink = page.locator('a[href="#getting-started"]').first();
    const githubLink = page.locator('.nav-github');

    await expect(featuresLink).toBeAttached();
    await expect(usageLink).toBeAttached();
    await expect(architectureLink).toBeAttached();
    await expect(getStartedLink).toBeAttached();
    await expect(githubLink).toBeVisible();
  });

  test('clicking Features link scrolls to Features section', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the Features link
    await page.locator('.nav-links a[href="#features"]').click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify scroll position changed
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    // Verify the Features section is near the top of the viewport
    const featuresSection = page.locator('#features');
    const boundingBox = await featuresSection.boundingBox();
    expect(boundingBox).not.toBeNull();

    // The section should be visible at the top (accounting for nav height ~70px)
    expect(boundingBox.y).toBeLessThan(100);
  });

  test('clicking Usage link scrolls to Usage section', async ({ page }) => {
    // Click the Usage link
    await page.locator('.nav-links a[href="#usage"]').click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify the Usage section is near the top of the viewport
    const usageSection = page.locator('#usage');
    const boundingBox = await usageSection.boundingBox();
    expect(boundingBox).not.toBeNull();
    expect(boundingBox.y).toBeLessThan(100);
  });

  test('clicking Architecture link scrolls to Architecture section', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the Architecture link
    await page.locator('.nav-links a[href="#architecture"]').click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify scroll position changed significantly
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    // Verify the Architecture section is visible near the top of the viewport
    const architectureSection = page.locator('#architecture');
    const boundingBox = await architectureSection.boundingBox();
    expect(boundingBox).not.toBeNull();
    // Account for nav height (~70px) and some buffer
    expect(boundingBox.y).toBeLessThan(150);
  });

  test('clicking Get Started link scrolls to Getting Started section', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the Get Started link
    await page.locator('.nav-links a[href="#getting-started"]').click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify scroll position changed significantly
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    // Verify the Getting Started section is visible in the viewport
    const gettingStartedSection = page.locator('#getting-started');
    const boundingBox = await gettingStartedSection.boundingBox();
    expect(boundingBox).not.toBeNull();
    // Section should be near the top (accounting for nav height ~70px and some buffer)
    expect(boundingBox.y).toBeLessThan(200);
  });

  test('navigation bar is fixed at the top when scrolling', async ({ page }) => {
    // Scroll down the page
    await page.evaluate(() => window.scrollTo(0, 500));
    await page.waitForTimeout(100);

    // Check that navigation is still visible
    const nav = page.locator('.main-nav');
    await expect(nav).toBeVisible();

    // Check nav position is at top of viewport
    const boundingBox = await nav.boundingBox();
    expect(boundingBox.y).toBe(0);
  });

  test('GitHub link opens in new tab', async ({ page }) => {
    const githubLink = page.locator('.nav-github');

    // Check target attribute
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Check rel attribute for security
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });
});

test.describe('Mobile Navigation - Hamburger Menu', () => {
  test.use({ viewport: { width: 375, height: 667 } }); // iPhone SE size

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('hamburger menu is visible on mobile', async ({ page }) => {
    const hamburger = page.locator('.nav-toggle');
    await expect(hamburger).toBeVisible();
  });

  test('navigation collapses to hamburger menu on mobile', async ({ page }) => {
    // Hamburger should be visible
    const hamburger = page.locator('.nav-toggle');
    await expect(hamburger).toBeVisible();

    // Menu should initially be hidden (off-screen)
    const navMenu = page.locator('.nav-menu');
    const navMenuBox = await navMenu.boundingBox();

    // Menu is positioned off-screen to the right initially
    // It should either not be visible or its left edge should be off-screen
    if (navMenuBox) {
      // Menu exists but should be positioned off-screen (right: -100%)
      const viewportWidth = 375;
      expect(navMenuBox.x).toBeGreaterThanOrEqual(viewportWidth - 10);
    }
  });

  test('clicking hamburger opens mobile menu', async ({ page }) => {
    const hamburger = page.locator('.nav-toggle');
    const navMenu = page.locator('.nav-menu');

    // Click hamburger to open menu
    await hamburger.click();
    await page.waitForTimeout(400);

    // Menu should be visible (has is-open class)
    await expect(navMenu).toHaveClass(/is-open/);

    // Aria-expanded should be true
    const ariaExpanded = await hamburger.getAttribute('aria-expanded');
    expect(ariaExpanded).toBe('true');
  });

  test('clicking nav link closes mobile menu', async ({ page }) => {
    const hamburger = page.locator('.nav-toggle');
    const navMenu = page.locator('.nav-menu');

    // Open menu
    await hamburger.click();
    await page.waitForTimeout(400);

    // Click a nav link
    await page.locator('.nav-links a[href="#features"]').click();
    await page.waitForTimeout(400);

    // Menu should be closed
    await expect(navMenu).not.toHaveClass(/is-open/);
  });

  test('pressing Escape closes mobile menu', async ({ page }) => {
    const hamburger = page.locator('.nav-toggle');
    const navMenu = page.locator('.nav-menu');

    // Open menu
    await hamburger.click();
    await page.waitForTimeout(400);

    // Press Escape
    await page.keyboard.press('Escape');
    await page.waitForTimeout(400);

    // Menu should be closed
    await expect(navMenu).not.toHaveClass(/is-open/);
  });

  test('clicking outside menu closes it', async ({ page }) => {
    const hamburger = page.locator('.nav-toggle');
    const navMenu = page.locator('.nav-menu');
    const navOverlay = page.locator('.nav-overlay');

    // Open menu
    await hamburger.click();
    await page.waitForTimeout(400);

    // Menu should be open
    await expect(navMenu).toHaveClass(/is-open/);

    // Overlay should be visible
    await expect(navOverlay).toHaveClass(/is-visible/);

    // Click on the overlay to close the menu
    // Use dispatchEvent to ensure click event fires
    await navOverlay.evaluate(el => el.click());
    await page.waitForTimeout(400);

    // Menu should be closed
    await expect(navMenu).not.toHaveClass(/is-open/);
  });
});

test.describe('Responsive Navigation', () => {
  test('navigation links visible on desktop', async ({ page }) => {
    await page.setViewportSize({ width: 1024, height: 768 });
    await page.goto('/');

    // Nav links should be visible
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Hamburger should be hidden
    const hamburger = page.locator('.nav-toggle');
    await expect(hamburger).not.toBeVisible();
  });

  test('navigation collapses at tablet breakpoint', async ({ page }) => {
    await page.setViewportSize({ width: 767, height: 1024 });
    await page.goto('/');

    // Hamburger should be visible
    const hamburger = page.locator('.nav-toggle');
    await expect(hamburger).toBeVisible();
  });
});
