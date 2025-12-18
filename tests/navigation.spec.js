// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = path.resolve(__dirname, '../index.html');

/**
 * Navigation Functionality Tests
 * Verify the navigation bar provides easy access to all page sections with smooth scrolling
 */
test.describe('Navigation Functionality', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(`file://${indexPath}`);
  });

  /**
   * Test Case 1: Navigation bar presence
   * Input: Check navigation bar presence
   * Expected: Navigation bar exists with links to Features, Quick Start, Commands, Configuration sections
   */
  test('TC1: Navigation bar exists with links to all sections', async ({ page }) => {
    // Verify navigation bar exists
    const navbar = page.locator('[data-testid="navbar"]');
    await expect(navbar).toBeVisible();

    // Verify navigation has proper role and aria-label
    await expect(navbar).toHaveAttribute('role', 'navigation');
    await expect(navbar).toHaveAttribute('aria-label', 'Main navigation');

    // Verify Features link exists
    const featuresLink = page.locator('[data-testid="nav-features"]');
    await expect(featuresLink).toBeVisible();
    await expect(featuresLink).toHaveAttribute('href', '#features');
    await expect(featuresLink).toHaveText('Features');

    // Verify Quick Start link exists
    const quickstartLink = page.locator('[data-testid="nav-quickstart"]');
    await expect(quickstartLink).toBeVisible();
    await expect(quickstartLink).toHaveAttribute('href', '#quickstart');
    await expect(quickstartLink).toHaveText('Quick Start');

    // Verify Commands link exists
    const commandsLink = page.locator('[data-testid="nav-commands"]');
    await expect(commandsLink).toBeVisible();
    await expect(commandsLink).toHaveAttribute('href', '#commands');
    await expect(commandsLink).toHaveText('Commands');

    // Verify Configuration link exists
    const configLink = page.locator('[data-testid="nav-configuration"]');
    await expect(configLink).toBeVisible();
    await expect(configLink).toHaveAttribute('href', '#configuration');
    await expect(configLink).toHaveText('Configuration');
  });

  /**
   * Test Case 2: Navigation sticky behavior
   * Input: Scroll page and check navigation sticky behavior
   * Expected: Navigation remains visible at top of viewport when scrolling
   */
  test('TC2: Navigation remains visible at top when scrolling', async ({ page }) => {
    // Get initial navbar position
    const navbar = page.locator('[data-testid="navbar"]');
    await expect(navbar).toBeVisible();

    // Scroll down the page significantly
    await page.evaluate(() => window.scrollBy(0, 1000));

    // Wait for scroll to complete
    await page.waitForTimeout(100);

    // Verify navbar is still visible
    await expect(navbar).toBeVisible();

    // Verify navbar is at the top of viewport
    const navbarBoundingBox = await navbar.boundingBox();
    expect(navbarBoundingBox).not.toBeNull();
    expect(navbarBoundingBox.y).toBeLessThanOrEqual(5); // Should be at or near top (allowing small margin)

    // Verify navbar has sticky positioning via computed style
    const position = await navbar.evaluate((el) => window.getComputedStyle(el).position);
    expect(position).toBe('sticky');
  });

  /**
   * Test Case 3: Features navigation link smooth scroll
   * Input: Click Features navigation link
   * Expected: Page smoothly scrolls to Features section
   */
  test('TC3: Click Features link scrolls to Features section', async ({ page }) => {
    // Verify smooth scroll behavior is enabled on html
    const scrollBehavior = await page.evaluate(() => window.getComputedStyle(document.documentElement).scrollBehavior);
    expect(scrollBehavior).toBe('smooth');

    // Click the Features link
    const featuresLink = page.locator('[data-testid="nav-features"]');
    await featuresLink.click();

    // Wait for scroll animation to complete
    await page.waitForTimeout(500);

    // Verify Features section is now visible and near top of viewport
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify the Features section is near the top of the viewport
    const sectionBoundingBox = await featuresSection.boundingBox();
    expect(sectionBoundingBox).not.toBeNull();
    // Section should be near top (accounting for sticky navbar height ~60px and some margin)
    expect(sectionBoundingBox.y).toBeLessThan(100);
  });

  /**
   * Test Case 4: Quick Start navigation link smooth scroll
   * Input: Click Quick Start navigation link
   * Expected: Page smoothly scrolls to Quick Start section
   */
  test('TC4: Click Quick Start link scrolls to Quick Start section', async ({ page }) => {
    // Verify smooth scroll behavior is enabled on html
    const scrollBehavior = await page.evaluate(() => window.getComputedStyle(document.documentElement).scrollBehavior);
    expect(scrollBehavior).toBe('smooth');

    // Click the Quick Start link
    const quickstartLink = page.locator('[data-testid="nav-quickstart"]');
    await quickstartLink.click();

    // Wait for scroll animation to complete
    await page.waitForTimeout(500);

    // Verify Quick Start section is now visible and near top of viewport
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Verify the Quick Start section is near the top of the viewport
    const sectionBoundingBox = await quickstartSection.boundingBox();
    expect(sectionBoundingBox).not.toBeNull();
    // Section should be near top (accounting for sticky navbar height ~60px and some margin)
    expect(sectionBoundingBox.y).toBeLessThan(100);
  });

  /**
   * Test Case 5: External GitHub link
   * Input: Click external GitHub link
   * Expected: Link opens in new tab (target='_blank') with rel='noopener'
   */
  test('TC5: External GitHub link opens in new tab with noopener', async ({ page }) => {
    // Verify GitHub link exists in navigation
    const githubLink = page.locator('[data-testid="nav-github"]');
    await expect(githubLink).toBeVisible();

    // Verify link has target="_blank" for new tab
    await expect(githubLink).toHaveAttribute('target', '_blank');

    // Verify link has rel="noopener" or rel="noopener noreferrer" for security
    const relAttribute = await githubLink.getAttribute('rel');
    expect(relAttribute).not.toBeNull();
    expect(relAttribute).toContain('noopener');

    // Verify link points to GitHub
    const hrefAttribute = await githubLink.getAttribute('href');
    expect(hrefAttribute).toContain('github.com');
    expect(hrefAttribute).toContain('mirdb');
  });

  /**
   * Additional test: Navigation links are keyboard accessible
   */
  test('Navigation links are keyboard accessible', async ({ page }) => {
    const navbar = page.locator('[data-testid="navbar"]');
    await expect(navbar).toBeVisible();

    // Focus on brand link first
    const brandLink = page.locator('[data-testid="nav-brand"]');
    await brandLink.focus();
    await expect(brandLink).toBeFocused();

    // Tab through navigation links
    await page.keyboard.press('Tab');
    const featuresLink = page.locator('[data-testid="nav-features"]');
    await expect(featuresLink).toBeFocused();

    await page.keyboard.press('Tab');
    const quickstartLink = page.locator('[data-testid="nav-quickstart"]');
    await expect(quickstartLink).toBeFocused();

    await page.keyboard.press('Tab');
    const commandsLink = page.locator('[data-testid="nav-commands"]');
    await expect(commandsLink).toBeFocused();

    await page.keyboard.press('Tab');
    const configLink = page.locator('[data-testid="nav-configuration"]');
    await expect(configLink).toBeFocused();
  });

  /**
   * Additional test: Commands and Configuration links scroll correctly
   */
  test('Commands and Configuration links scroll to correct sections', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);
    expect(initialScrollY).toBe(0);

    // Test Commands link
    const commandsLink = page.locator('[data-testid="nav-commands"]');
    await commandsLink.click();
    await page.waitForTimeout(500);

    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();
    const commandsBoundingBox = await commandsSection.boundingBox();
    expect(commandsBoundingBox).not.toBeNull();
    expect(commandsBoundingBox.y).toBeLessThan(100);

    // Scroll back to top
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.waitForTimeout(300);

    // Test Configuration link - verify scroll changed from top
    const configLink = page.locator('[data-testid="nav-configuration"]');
    await configLink.click();
    await page.waitForTimeout(500);

    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Verify we scrolled down (configuration is at the bottom so may not reach top)
    const scrollYAfterConfig = await page.evaluate(() => window.scrollY);
    expect(scrollYAfterConfig).toBeGreaterThan(0);

    // Verify configuration section is visible in viewport
    const configBoundingBox = await configSection.boundingBox();
    expect(configBoundingBox).not.toBeNull();
    // Configuration is the last section, so it may not scroll all the way to top
    // Just verify it's visible on screen (within viewport height)
    const viewportHeight = await page.evaluate(() => window.innerHeight);
    expect(configBoundingBox.y).toBeLessThan(viewportHeight);
  });
});
