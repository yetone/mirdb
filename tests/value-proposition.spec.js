const { test, expect } = require('@playwright/test');

/**
 * Value Proposition Section Tests
 * Scenario: Validate the three-column value proposition section clearly communicates MirDB's key benefits
 */

test.describe('Value Proposition Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Section contains three distinct column elements for benefits display', async ({ page }) => {
    // Navigate to value proposition section and inspect structure
    const valuePropositionSection = page.locator('#value-proposition, [class*="value-proposition"], .value-prop');
    await expect(valuePropositionSection.first()).toBeVisible();

    // Verify three-column layout - look for benefit/feature cards or columns
    const columns = valuePropositionSection.locator('.value-prop-column, .benefit-card, .feature-card, [class*="column"], [class*="benefit"], [class*="card"]');
    const columnCount = await columns.count();

    // Verify there are exactly 3 columns
    expect(columnCount).toBe(3);

    // Verify each column is visible
    for (let i = 0; i < 3; i++) {
      await expect(columns.nth(i)).toBeVisible();
    }
  });

  test('Test Case 2: Page contains text about Memcached protocol compatibility and drop-in replacement', async ({ page }) => {
    // Check for Memcached Protocol content
    const pageContent = await page.textContent('body');

    // Verify 'Memcached' appears in the page
    expect(pageContent.toLowerCase()).toContain('memcached');

    // Verify protocol-related text
    expect(pageContent.toLowerCase()).toMatch(/memcached.*protocol|protocol.*memcached/);

    // Verify drop-in replacement or compatibility text
    expect(pageContent.toLowerCase()).toMatch(/drop-in|drop in|compatible|compatibility/);

    // Verify there's a visible element containing Memcached Protocol information
    const memcachedContent = page.locator('[class*="value-prop"], [class*="benefit"], section').filter({ hasText: /Memcached.*Protocol/i });
    await expect(memcachedContent.first()).toBeVisible();
  });

  test('Test Case 3: Page contains text about data persistence and surviving restarts', async ({ page }) => {
    // Check for Persistence content
    const pageContent = await page.textContent('body');

    // Verify 'Persistence' or 'persistent' appears in the page
    expect(pageContent.toLowerCase()).toContain('persist');

    // Verify text about data surviving restarts
    expect(pageContent.toLowerCase()).toMatch(/restart|surviv|durable|durability/);

    // Verify there's a visible element containing Persistence information
    const persistenceContent = page.locator('[class*="value-prop"], [class*="benefit"], section').filter({ hasText: /Persistence/i });
    await expect(persistenceContent.first()).toBeVisible();
  });

  test('Test Case 4: Page contains text about LSM Tree architecture for efficient storage', async ({ page }) => {
    // Check for LSM Tree content
    const pageContent = await page.textContent('body');

    // Verify 'LSM' appears in the page
    expect(pageContent.toLowerCase()).toContain('lsm');

    // Verify tree-related text
    expect(pageContent.toLowerCase()).toMatch(/lsm.*tree|lsm tree/);

    // Verify text about efficient storage or architecture
    expect(pageContent.toLowerCase()).toMatch(/efficien|architecture|storage/);

    // Verify there's a visible element containing LSM Tree information
    const lsmContent = page.locator('[class*="value-prop"], [class*="benefit"], section').filter({ hasText: /LSM.*Tree/i });
    await expect(lsmContent.first()).toBeVisible();
  });
});
