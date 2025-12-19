// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

// Base URL for the static HTML file
const BASE_URL = 'file://' + path.resolve(__dirname, '../dist/index.html');

test.describe('Memcached vs MirDB Comparison Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(BASE_URL);
  });

  test('Test Case 1: Check comparison section presence', async ({ page }) => {
    // Navigate to comparison section
    const comparisonSection = page.locator('[data-testid="comparison-section"]');
    await expect(comparisonSection).toBeVisible();

    // Check that section has proper heading
    const heading = comparisonSection.locator('h2');
    await expect(heading).toBeVisible();
    const headingText = await heading.textContent();
    expect(headingText.toLowerCase()).toMatch(/memcached|comparison|vs|mirdb/);
  });

  test('Test Case 2: Check persistence comparison - MirDB persistence vs memcached volatility', async ({ page }) => {
    // Navigate to comparison section
    const comparisonSection = page.locator('[data-testid="comparison-section"]');
    await expect(comparisonSection).toBeVisible();

    // Check for MirDB persistence information
    const mirdbPersistence = page.locator('[data-testid="mirdb-persistence"]');
    await expect(mirdbPersistence).toBeVisible();
    const mirdbText = await mirdbPersistence.textContent();
    expect(mirdbText.toLowerCase()).toMatch(/persist|durable|survives|disk|sstable/);

    // Check for memcached volatility information
    const memcachedVolatility = page.locator('[data-testid="memcached-volatility"]');
    await expect(memcachedVolatility).toBeVisible();
    const memcachedText = await memcachedVolatility.textContent();
    expect(memcachedText.toLowerCase()).toMatch(/volatile|memory|lost|restart|ram/);
  });

  test('Test Case 3: Check memcached protocol compatibility statement', async ({ page }) => {
    // Navigate to comparison section
    const comparisonSection = page.locator('[data-testid="comparison-section"]');
    await expect(comparisonSection).toBeVisible();

    // Check for protocol compatibility statement
    const compatibilityNote = page.locator('[data-testid="protocol-compatibility"]');
    await expect(compatibilityNote).toBeVisible();
    const compatText = await compatibilityNote.textContent();
    expect(compatText.toLowerCase()).toMatch(/compatible|protocol|memcached|client/);
  });

  test('Verify comparison table or comparison cards exist', async ({ page }) => {
    // Navigate to comparison section
    const comparisonSection = page.locator('[data-testid="comparison-section"]');
    await expect(comparisonSection).toBeVisible();

    // Check for either a comparison table or comparison items
    const comparisonTable = page.locator('[data-testid="comparison-table"]');
    const comparisonItems = page.locator('[data-testid^="comparison-item-"]');

    // At least one of these should exist
    const tableExists = await comparisonTable.count() > 0;
    const itemsExist = await comparisonItems.count() > 0;

    expect(tableExists || itemsExist).toBeTruthy();
  });

  test('Verify key differences are highlighted', async ({ page }) => {
    // Navigate to comparison section
    const comparisonSection = page.locator('[data-testid="comparison-section"]');
    await expect(comparisonSection).toBeVisible();

    // Get all text content from the comparison section
    const sectionText = await comparisonSection.textContent();
    const lowerText = sectionText.toLowerCase();

    // Check that key differences are mentioned
    expect(lowerText).toMatch(/persist|durability|storage/);
    expect(lowerText).toMatch(/memcached|protocol|compatible/);
  });
});
