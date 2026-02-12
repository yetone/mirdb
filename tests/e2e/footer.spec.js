/**
 * Footer E2E Tests
 * Owner: Scenario 5 - Footer Content and Links
 *
 * Test cases for footer section validation
 */

const { test, expect } = require('@playwright/test');
const { loadPage } = require('../test-utils/helpers');

test.describe('Footer Content and Links', () => {
  test.beforeEach(async ({ page }) => {
    await loadPage(page);
  });

  test('Test Case 1: Footer element exists', async ({ page }) => {
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();
  });

  test('Test Case 2: GitHub repository link exists with correct URL', async ({ page }) => {
    const githubLink = page.locator('footer a[href="https://github.com/yetone/mirdb"]');
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
  });

  test('Test Case 3: GitHub link opens in new tab', async ({ page }) => {
    const githubLink = page.locator('footer a[href="https://github.com/yetone/mirdb"]');
    await expect(githubLink).toHaveAttribute('target', '_blank');
  });

  test('Test Case 4: MIT license text appears in footer', async ({ page }) => {
    const footer = page.locator('footer');
    await expect(footer).toContainText('MIT');
  });

  test('Test Case 5: Author attribution appears in footer', async ({ page }) => {
    const footer = page.locator('footer');
    const footerText = await footer.textContent();
    const hasAuthorInfo = footerText.toLowerCase().includes('yetone') ||
                          footerText.toLowerCase().includes('created by') ||
                          footerText.toLowerCase().includes('author');
    expect(hasAuthorInfo).toBe(true);
  });

  test('Test Case 6: CircleCI badge is displayed', async ({ page }) => {
    const circleciBadge = page.locator('footer img[src*="circleci"]');
    await expect(circleciBadge).toBeVisible();
  });

  test('Test Case 7: CircleCI badge is clickable and links to CI page', async ({ page }) => {
    const badgeLink = page.locator('footer a[href*="circleci.com"]');
    await expect(badgeLink).toBeVisible();

    const badge = badgeLink.locator('img');
    await expect(badge).toBeVisible();
  });

  test('Footer has proper accessibility structure', async ({ page }) => {
    const footer = page.locator('footer');

    const githubLink = footer.locator('a[href="https://github.com/yetone/mirdb"]');
    await expect(githubLink).toHaveAttribute('rel', /noopener/);

    const circleciBadge = footer.locator('img[src*="circleci"]');
    await expect(circleciBadge).toHaveAttribute('alt', /.+/);
  });

  test('Footer is visible when scrolled to bottom', async ({ page }) => {
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    const footer = page.locator('footer');
    await expect(footer).toBeInViewport();
  });
});
