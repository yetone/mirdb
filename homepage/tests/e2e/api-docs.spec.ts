/**
 * API Documentation Links E2E Tests
 * Owner: Scenario 6 - API Documentation Links
 *
 * Test coverage:
 * - Navigation contains link to API documentation
 * - API section exists with Memcached protocol documentation
 * - API documentation explains GET, SET, DELETE commands
 * - Navigation link scrolls to API section
 */
import { test, expect } from '@playwright/test';

test.describe('API Documentation Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Navigation contains link to API documentation', async ({ page }) => {
    // Verify header navigation exists
    const nav = page.locator('nav[aria-label="Main navigation"]');
    await expect(nav).toBeVisible();

    // Verify API/Docs link exists in navigation
    const apiLink = page.locator('nav a[href="#api"]');
    await expect(apiLink).toBeVisible();

    // Verify link has appropriate text (API or Docs)
    const linkText = await apiLink.textContent();
    expect(linkText?.toLowerCase()).toMatch(/api|docs|documentation/);
  });

  test('TC2: API documentation section explains Memcached protocol commands', async ({ page }) => {
    // Verify API section exists
    const apiSection = page.locator('#api');
    await expect(apiSection).toBeVisible();

    // Verify section has heading
    const apiHeading = page.locator('#api h2, #api-heading');
    await expect(apiHeading).toBeVisible();

    // Verify Memcached protocol is mentioned
    const apiContent = await apiSection.textContent();
    expect(apiContent?.toLowerCase()).toMatch(/memcached.*protocol/i);

    // Verify GET command documentation
    const getCommand = page.locator('#api [data-command="get"]');
    await expect(getCommand).toBeVisible();

    // Verify SET command documentation
    const setCommand = page.locator('#api [data-command="set"]');
    await expect(setCommand).toBeVisible();

    // Verify DELETE command documentation
    const deleteCommand = page.locator('#api [data-command="delete"]');
    await expect(deleteCommand).toBeVisible();
  });

  test('TC3: API navigation link scrolls to API section', async ({ page }) => {
    // Click the API link in navigation
    const apiLink = page.locator('nav a[href="#api"]');
    await apiLink.click();

    // Verify URL hash changes to #api
    await expect(page).toHaveURL(/#api/);

    // Verify API section is now in viewport
    const apiSection = page.locator('#api');
    await expect(apiSection).toBeInViewport();
  });

  test('TC4: API section displays response codes table', async ({ page }) => {
    // Navigate to API section
    await page.goto('/#api');

    // Verify response codes are documented
    const apiSection = page.locator('#api');
    const responseCodesContent = await apiSection.textContent();

    // Check for common Memcached response codes
    expect(responseCodesContent).toContain('STORED');
    expect(responseCodesContent).toContain('NOT_STORED');
    expect(responseCodesContent).toContain('DELETED');
    expect(responseCodesContent).toContain('NOT_FOUND');
  });

  test('TC5: API section has proper semantic structure', async ({ page }) => {
    // Verify API section has proper aria-labelledby
    const apiSection = page.locator('#api');
    await expect(apiSection).toBeVisible();

    // Check for section heading
    const heading = page.locator('#api h2');
    await expect(heading).toBeVisible();

    // Check that command sections have proper structure
    const commandSections = page.locator('#api .api-command, #api article');
    const count = await commandSections.count();
    expect(count).toBeGreaterThan(0);
  });
});
