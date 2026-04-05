import { test, expect } from '@playwright/test';

test.describe('Technical Specifications Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Section with technical specifications or configuration exists', async ({ page }) => {
    // Query for technical specifications section
    const specsSection = page.locator('#specs');
    await expect(specsSection).toBeVisible();

    // Verify section has meaningful content
    const specsContainer = specsSection.locator('.specs__container');
    await expect(specsContainer).toBeVisible();

    // Verify section title exists
    const title = specsSection.locator('.specs__title');
    await expect(title).toBeVisible();
    await expect(title).toContainText(/technical specifications|configuration/i);
  });

  test('TC2: Default port number is displayed', async ({ page }) => {
    // Navigate to specs section and verify default port information
    const specsSection = page.locator('#specs');
    await expect(specsSection).toBeVisible();

    // Look for port information - MirDB uses 12333 by default
    const portInfo = specsSection.locator('[data-config="port"], .specs__config-value:has-text("12333")');

    // Alternative: search for port text anywhere in the section
    const specsContent = await specsSection.textContent();
    expect(specsContent).toContain('12333');
  });

  test('TC3: List of supported memcached operations is displayed', async ({ page }) => {
    // Navigate to specs section and verify operations list
    const specsSection = page.locator('#specs');
    await expect(specsSection).toBeVisible();

    // Verify operations heading exists
    const operationsHeading = specsSection.locator('.specs__operations-title, h3:has-text("Supported Operations")');
    await expect(operationsHeading).toBeVisible();

    // Verify common memcached operations are listed
    const operationsList = specsSection.locator('.specs__operations-list');
    await expect(operationsList).toBeVisible();

    // Check for core memcached operations
    const specsContent = await specsSection.textContent();
    expect(specsContent).toMatch(/GET/i);
    expect(specsContent).toMatch(/SET/i);
    expect(specsContent).toMatch(/DELETE/i);
  });
});
