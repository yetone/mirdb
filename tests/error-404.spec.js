// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const error404Path = 'file://' + path.resolve(__dirname, '../404.html');
const indexPath = 'file://' + path.resolve(__dirname, '../index.html');

test.describe('Error Handling - 404 Page', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(error404Path);
  });

  // Test Case 1: Navigate to non-existent page and verify appropriate 404 handling
  test('TC1: 404 page displays appropriate error handling for non-existent pages', async ({ page }) => {
    // Verify the error page is displayed
    const errorPage = page.locator('#error-page');
    await expect(errorPage).toBeVisible();

    // Verify 404 error code is displayed prominently
    const errorCode = page.locator('.error-code');
    await expect(errorCode).toBeVisible();
    await expect(errorCode).toHaveText('404');

    // Verify error title is present
    const errorTitle = page.locator('.error-title');
    await expect(errorTitle).toBeVisible();
    await expect(errorTitle).toContainText('Page Not Found');

    // Verify error message explains the situation
    const errorMessage = page.locator('.error-message');
    await expect(errorMessage).toBeVisible();
    const messageText = await errorMessage.textContent();
    expect(messageText.toLowerCase()).toContain("doesn't exist");
  });

  // Test Case 2: Check 404 page has navigation back to home
  test('TC2: 404 page has navigation back to homepage', async ({ page }) => {
    // Find the home link
    const homeLink = page.locator('#home-link');
    await expect(homeLink).toBeVisible();

    // Verify it has text indicating it goes to homepage
    const linkText = await homeLink.textContent();
    expect(linkText.toLowerCase()).toMatch(/home|back/i);

    // Verify the link points to the homepage
    const href = await homeLink.getAttribute('href');
    expect(href).toContain('index.html');

    // Click the link and verify navigation to homepage
    await homeLink.click();
    await page.waitForURL(/index\.html/);

    // Verify we're on the homepage by checking for hero section
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();
  });

  // Additional test: 404 page has proper accessibility structure
  test('TC3: 404 page has proper accessibility structure', async ({ page }) => {
    // Check for proper heading hierarchy
    const h1 = page.locator('h1.error-code');
    await expect(h1).toBeVisible();

    const h2 = page.locator('h2.error-title');
    await expect(h2).toBeVisible();

    // Check for navigation landmark
    const nav = page.locator('nav.error-cta');
    await expect(nav).toBeVisible();

    // Verify nav has aria-label
    const ariaLabel = await nav.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();

    // Check for main landmark
    const main = page.locator('main#error-page');
    await expect(main).toBeVisible();

    // Verify main has role attribute
    const role = await main.getAttribute('role');
    expect(role).toBe('main');
  });

  // Additional test: 404 page has GitHub link as secondary action
  test('TC4: 404 page provides GitHub link as alternative navigation', async ({ page }) => {
    // Find the GitHub link
    const githubLink = page.locator('#github-link');
    await expect(githubLink).toBeVisible();

    // Verify it has text mentioning GitHub
    const linkText = await githubLink.textContent();
    expect(linkText.toLowerCase()).toContain('github');

    // Verify the link points to the correct GitHub repository
    const href = await githubLink.getAttribute('href');
    expect(href).toContain('github.com/yetone/mirdb');
  });

  // Additional test: 404 page is responsive
  test('TC5: 404 page is responsive on mobile viewport', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });

    // Verify error content is visible
    const errorContent = page.locator('.error-content');
    await expect(errorContent).toBeVisible();

    // Verify all key elements are visible
    const errorCode = page.locator('.error-code');
    await expect(errorCode).toBeVisible();

    const homeLink = page.locator('#home-link');
    await expect(homeLink).toBeVisible();

    // Verify content is within viewport (no horizontal scroll needed)
    const box = await errorContent.boundingBox();
    expect(box).toBeTruthy();
    expect(box.x).toBeGreaterThanOrEqual(0);
    expect(box.x + box.width).toBeLessThanOrEqual(375);
  });

  // Additional test: Links on 404 page are keyboard accessible
  test('TC6: 404 page navigation links are keyboard accessible', async ({ page }) => {
    // Tab to the first link (home link)
    await page.keyboard.press('Tab');

    // Check that home link is focused
    const homeLink = page.locator('#home-link');
    await expect(homeLink).toBeFocused();

    // Tab to the next link (GitHub link)
    await page.keyboard.press('Tab');

    // Check that GitHub link is focused
    const githubLink = page.locator('#github-link');
    await expect(githubLink).toBeFocused();

    // Verify Enter key would activate the link
    const href = await githubLink.getAttribute('href');
    expect(href).toBeTruthy();
  });

  // Additional test: 404 page has proper page title
  test('TC7: 404 page has appropriate page title', async ({ page }) => {
    const title = await page.title();
    expect(title.toLowerCase()).toContain('404');
    expect(title).toContain('MirDB');
  });
});
