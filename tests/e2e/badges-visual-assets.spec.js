/**
 * E2E tests for Status Badges and Visual Assets.
 * Owner: Scenario 8 - Status Badges and Visual Assets
 *
 * Tests:
 * - CircleCI build status badge is displayed and links correctly
 * - logo.gif is displayed on the page
 * - usage.gif is displayed in the Quick Start section
 * - All images have descriptive alt attributes
 */

const { test, expect } = require('@playwright/test');

const filePath = 'file://' + process.cwd() + '/index.html';

test.describe('Status Badges and Visual Assets', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(filePath);
  });

  // Test Case 1: CircleCI badge image exists
  test('CircleCI badge image is displayed on the page', async ({ page }) => {
    const badgeImg = page.locator('img[src*="circleci"]').or(
      page.locator('img[src*="atompunk.yetone.fun/github/yetone/mirdb"]')
    );
    await expect(badgeImg).toBeVisible();
  });

  // Test Case 2: Badge links to correct destination
  test('CircleCI badge links to https://circleci.com/gh/yetone/mirdb', async ({ page }) => {
    const badgeLink = page.locator('a[href="https://circleci.com/gh/yetone/mirdb"]');
    await expect(badgeLink).toBeVisible();

    // The link should contain an img
    const img = badgeLink.locator('img');
    await expect(img).toBeVisible();
  });

  // Test Case 3: logo.gif is displayed
  test('logo.gif is displayed on the page', async ({ page }) => {
    const logoImg = page.locator('img[src*="assets/logo.gif"]');
    await expect(logoImg).toBeVisible();
  });

  // Test Case 4: usage.gif is displayed
  test('usage.gif is displayed on the page', async ({ page }) => {
    const usageImg = page.locator('img[src*="assets/usage.gif"]');
    await expect(usageImg).toBeVisible();
  });

  // Test Case 5: All images have non-empty alt attributes
  test('all images have non-empty alt attributes', async ({ page }) => {
    const images = page.locator('img');
    const count = await images.count();
    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const img = images.nth(i);
      const alt = await img.getAttribute('alt');
      expect(alt).toBeTruthy();
      expect(alt.trim().length).toBeGreaterThan(0);
    }
  });

  test('logo image has descriptive alt text', async ({ page }) => {
    const logoImg = page.locator('img[src*="assets/logo.gif"]');
    await expect(logoImg).toHaveAttribute('alt', /MirDB logo/i);
  });

  test('usage image has descriptive alt text', async ({ page }) => {
    const usageImg = page.locator('img[src*="assets/usage.gif"]');
    await expect(usageImg).toHaveAttribute('alt', /MirDB usage demonstration/i);
  });
});
