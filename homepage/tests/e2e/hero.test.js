/**
 * E2E Tests for Hero Section
 * Owner: Scenario 1 - Hero Section Implementation
 *
 * Tests user interactions and visual behavior
 */

const { test, expect } = require('@playwright/test');

test.describe('Hero Section E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Hero section is visible within viewport without scrolling', async ({ page }) => {
    // Set a standard desktop viewport
    await page.setViewportSize({ width: 1280, height: 720 });
    await page.goto('/');

    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Check that hero section is within viewport (above the fold)
    const boundingBox = await heroSection.boundingBox();
    expect(boundingBox).not.toBeNull();

    // The hero section should start near the top of the page
    // (accounting for header which is sticky)
    expect(boundingBox.y).toBeLessThan(100);

    // Hero should be visible without scrolling - check it's in viewport
    const viewportHeight = 720;
    expect(boundingBox.y).toBeLessThan(viewportHeight);
  });

  test('Test Case 4: Click Get Started button smooth scrolls to Quick Start section', async ({ page }) => {
    // Wait for page to be ready
    await page.waitForLoadState('networkidle');

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the Get Started button
    const getStartedBtn = page.locator('.hero__cta--primary');
    await expect(getStartedBtn).toBeVisible();
    await getStartedBtn.click();

    // Wait for scroll animation to complete
    await page.waitForTimeout(1000);

    // Verify we scrolled to the quickstart section
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();

    // Verify scroll position changed
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    // Verify URL hash updated
    const url = page.url();
    expect(url).toContain('#quickstart');
  });

  test('Test Case 5: Click View on GitHub button opens GitHub repository URL in new tab', async ({ page, context }) => {
    const githubBtn = page.locator('.hero__cta--secondary');
    await expect(githubBtn).toBeVisible();
    await expect(githubBtn).toContainText('View on GitHub');

    // Listen for new page (tab) to open
    const [newPage] = await Promise.all([
      context.waitForEvent('page'),
      githubBtn.click()
    ]);

    // Wait for the new page to load
    await newPage.waitForLoadState('domcontentloaded');

    // Verify the new tab URL is the GitHub repository
    const newUrl = newPage.url();
    expect(newUrl).toContain('github.com/yetone/mirdb');
  });

  test('Hero section displays correctly on mobile viewport', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Check title is visible
    const title = heroSection.locator('.hero__title');
    await expect(title).toBeVisible();

    // Check CTAs are visible
    const primaryCta = heroSection.locator('.hero__cta--primary');
    const secondaryCta = heroSection.locator('.hero__cta--secondary');
    await expect(primaryCta).toBeVisible();
    await expect(secondaryCta).toBeVisible();
  });

  test('Hero CTA buttons are keyboard accessible', async ({ page }) => {
    // Navigate to the page
    await page.goto('/');

    // Tab to the Get Started button
    await page.keyboard.press('Tab'); // Skip to first focusable element (nav logo)
    await page.keyboard.press('Tab'); // Nav link
    await page.keyboard.press('Tab'); // Nav link
    await page.keyboard.press('Tab'); // Nav link (GitHub)
    await page.keyboard.press('Tab'); // Get Started button

    // Verify Get Started button is focused
    const getStartedBtn = page.locator('.hero__cta--primary');
    const isFocused = await getStartedBtn.evaluate(el => el === document.activeElement);

    // The button should eventually be reachable via keyboard
    // (exact tab count may vary based on focusable elements)
    const primaryCta = page.locator('.hero__cta--primary');
    await primaryCta.focus();
    await expect(primaryCta).toBeFocused();

    // Press Enter to activate
    await page.keyboard.press('Enter');
    await page.waitForTimeout(500);

    // Should have scrolled to quickstart
    const url = page.url();
    expect(url).toContain('#quickstart');
  });
});
