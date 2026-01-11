// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Feature Comparison Section E2E Tests for MirDB Homepage
 * Tests REQ-8: Feature comparison section (MirDB vs memcached vs alternatives)
 */

test.describe('Feature Comparison Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Look for comparison table or section
  test('TC1: should have comparison section present showing MirDB differentiators', async ({ page }) => {
    // Find the comparison section
    const comparisonSection = await page.locator('[data-testid="comparison-section"], #comparison, .comparison-section');
    await expect(comparisonSection).toBeVisible();

    // Verify the section has a title
    const sectionTitle = await comparisonSection.locator('h2');
    await expect(sectionTitle).toBeVisible();

    // Verify there's a comparison table or comparison content
    const comparisonTable = await comparisonSection.locator('table, .comparison-table, .comparison-grid');
    await expect(comparisonTable).toBeVisible();

    // Verify MirDB is mentioned in the comparison
    const comparisonText = await comparisonSection.textContent();
    expect(comparisonText.toLowerCase()).toContain('mirdb');
  });

  // Test Case 2: Check comparison mentions memcached
  test('TC2: should explain difference from memcached (adds persistence)', async ({ page }) => {
    // Find the comparison section
    const comparisonSection = await page.locator('[data-testid="comparison-section"], #comparison, .comparison-section');
    await expect(comparisonSection).toBeVisible();

    // Get all text content from the comparison section
    const comparisonText = await comparisonSection.textContent();
    const lowerCaseText = comparisonText.toLowerCase();

    // Verify memcached is mentioned
    expect(lowerCaseText).toContain('memcached');

    // Verify persistence is mentioned as a differentiator
    // MirDB adds persistence while memcached doesn't have it
    expect(lowerCaseText).toContain('persist');
  });

  // Test Case 3: Check comparison mentions Redis
  test('TC3: should explain difference from Redis (memcached protocol)', async ({ page }) => {
    // Find the comparison section
    const comparisonSection = await page.locator('[data-testid="comparison-section"], #comparison, .comparison-section');
    await expect(comparisonSection).toBeVisible();

    // Get all text content from the comparison section
    const comparisonText = await comparisonSection.textContent();
    const lowerCaseText = comparisonText.toLowerCase();

    // Verify Redis is mentioned
    expect(lowerCaseText).toContain('redis');

    // Verify the memcached protocol is highlighted as differentiator from Redis
    // MirDB uses memcached protocol while Redis uses its own protocol
    expect(lowerCaseText).toContain('protocol');
  });

  // Additional test: Verify LevelDB comparison
  test('TC4: should mention LevelDB in comparison', async ({ page }) => {
    const comparisonSection = await page.locator('[data-testid="comparison-section"], #comparison, .comparison-section');
    await expect(comparisonSection).toBeVisible();

    const comparisonText = await comparisonSection.textContent();
    const lowerCaseText = comparisonText.toLowerCase();

    // Verify LevelDB is mentioned
    expect(lowerCaseText).toContain('leveldb');
  });

  // Test for table structure accessibility
  test('TC5: comparison table should have proper accessibility attributes', async ({ page }) => {
    const comparisonSection = await page.locator('[data-testid="comparison-section"], #comparison, .comparison-section');
    await expect(comparisonSection).toBeVisible();

    // Check for proper aria-labelledby
    const hasAriaLabel = await comparisonSection.evaluate(el => {
      return el.hasAttribute('aria-labelledby') || el.querySelector('[aria-labelledby]') !== null;
    });
    expect(hasAriaLabel).toBe(true);

    // If there's a table, check for headers
    const table = await comparisonSection.locator('table');
    const tableCount = await table.count();

    if (tableCount > 0) {
      const headers = await table.locator('th');
      const headerCount = await headers.count();
      expect(headerCount).toBeGreaterThan(0);
    }
  });

  // Test for navigation link to comparison section
  test('TC6: should be able to navigate to comparison section', async ({ page }) => {
    // Look for navigation link to comparison section
    const navLink = await page.locator('nav a[href="#comparison"]');
    const navLinkCount = await navLink.count();

    if (navLinkCount > 0) {
      await navLink.click();

      // Verify the comparison section is in view
      const comparisonSection = await page.locator('[data-testid="comparison-section"], #comparison, .comparison-section');
      await expect(comparisonSection).toBeInViewport();
    } else {
      // If no nav link, just verify the section exists
      const comparisonSection = await page.locator('[data-testid="comparison-section"], #comparison, .comparison-section');
      await expect(comparisonSection).toBeVisible();
    }
  });
});
