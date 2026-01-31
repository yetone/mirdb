/**
 * Comparison Section E2E Tests
 * Owner: Scenario 6 - Comparison Section
 *
 * Tests:
 * - Comparison table structure
 * - MirDB vs Memcached vs Redis
 * - Use case guidance
 *
 * Requirements: REQ-6
 */

const { test, expect } = require('@playwright/test');

test.describe('Comparison Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: should display comparison table with Feature, MirDB, Memcached, and Redis columns', async ({ page }) => {
    // Navigate to comparison section
    const comparisonSection = page.locator('#comparison');
    await expect(comparisonSection).toBeVisible();

    // Check for comparison table
    const comparisonTable = comparisonSection.locator('table.comparison-table');
    await expect(comparisonTable).toBeVisible();

    // Verify table headers contain all expected columns
    const tableHeaders = comparisonTable.locator('thead th');
    await expect(tableHeaders).toHaveCount(4);

    // Check for Feature column
    await expect(tableHeaders.nth(0)).toContainText('Feature');

    // Check for MirDB column
    await expect(tableHeaders.nth(1)).toContainText('MirDB');

    // Check for Memcached column
    await expect(tableHeaders.nth(2)).toContainText('Memcached');

    // Check for Redis column
    await expect(tableHeaders.nth(3)).toContainText('Redis');
  });

  test('TC2: should show correct persistence comparison - MirDB: Yes, Memcached: No, Redis: Yes', async ({ page }) => {
    const comparisonSection = page.locator('#comparison');
    const comparisonTable = comparisonSection.locator('table.comparison-table');

    // Find the persistence row
    const persistenceRow = comparisonTable.locator('tr[data-feature="persistence"]');
    await expect(persistenceRow).toBeVisible();

    // Check MirDB persistence value (should be Yes)
    const mirdbPersistence = persistenceRow.locator('td').nth(0);
    await expect(mirdbPersistence).toContainText('Yes');

    // Check Memcached persistence value (should be No)
    const memcachedPersistence = persistenceRow.locator('td').nth(1);
    await expect(memcachedPersistence).toContainText('No');

    // Check Redis persistence value (should be Yes)
    const redisPersistence = persistenceRow.locator('td').nth(2);
    await expect(redisPersistence).toContainText('Yes');
  });

  test('TC3: should show correct protocol comparison - MirDB: Memcached, Redis: Redis', async ({ page }) => {
    const comparisonSection = page.locator('#comparison');
    const comparisonTable = comparisonSection.locator('table.comparison-table');

    // Find the protocol row
    const protocolRow = comparisonTable.locator('tr[data-feature="protocol"]');
    await expect(protocolRow).toBeVisible();

    // Check MirDB protocol (should be Memcached)
    const mirdbProtocol = protocolRow.locator('td').nth(0);
    await expect(mirdbProtocol).toContainText('Memcached');

    // Check Memcached protocol (should be Memcached)
    const memcachedProtocol = protocolRow.locator('td').nth(1);
    await expect(memcachedProtocol).toContainText('Memcached');

    // Check Redis protocol (should be Redis)
    const redisProtocol = protocolRow.locator('td').nth(2);
    await expect(redisProtocol).toContainText('Redis');
  });

  test('TC4: should show correct storage architecture - MirDB: LSM tree, Redis: RDB/AOF', async ({ page }) => {
    const comparisonSection = page.locator('#comparison');
    const comparisonTable = comparisonSection.locator('table.comparison-table');

    // Find the storage architecture row
    const storageRow = comparisonTable.locator('tr[data-feature="storage"]');
    await expect(storageRow).toBeVisible();

    // Check MirDB storage (should be LSM tree)
    const mirdbStorage = storageRow.locator('td').nth(0);
    await expect(mirdbStorage).toContainText('LSM');

    // Check Redis storage (should contain RDB/AOF)
    const redisStorage = storageRow.locator('td').nth(2);
    await expect(redisStorage).toContainText('RDB');
    await expect(redisStorage).toContainText('AOF');
  });

  test('TC5: should display use case guidance text explaining when to choose MirDB', async ({ page }) => {
    const comparisonSection = page.locator('#comparison');

    // Check for use case guidance section
    const useCaseGuidance = comparisonSection.locator('.use-case-guidance');
    await expect(useCaseGuidance).toBeVisible();

    // Verify it contains explanatory text about when to choose MirDB
    const guidanceText = await useCaseGuidance.textContent();

    // Should mention choosing MirDB
    expect(guidanceText.toLowerCase()).toContain('choose');
    expect(guidanceText.toLowerCase()).toContain('mirdb');

    // Should provide context about use cases
    expect(guidanceText.toLowerCase()).toMatch(/persistence|memcached|drop-in|replacement|compatible/);
  });

  test('should display comparison section with proper heading structure', async ({ page }) => {
    const comparisonSection = page.locator('#comparison');

    // Check section title exists
    const sectionTitle = comparisonSection.locator('#comparison-title');
    await expect(sectionTitle).toBeVisible();
    await expect(sectionTitle).toHaveText('Comparison');

    // Verify heading is h2
    const tagName = await sectionTitle.evaluate((el) => el.tagName);
    expect(tagName).toBe('H2');
  });

  test('should be accessible via navigation', async ({ page }) => {
    // Click the Comparison link in navigation
    const comparisonLink = page.locator('nav a[href="#comparison"]');
    await expect(comparisonLink).toBeVisible();
    await comparisonLink.click();

    // Verify the comparison section is now in view
    const comparisonSection = page.locator('#comparison');
    await expect(comparisonSection).toBeInViewport();
  });

  test('comparison table should have proper accessibility attributes', async ({ page }) => {
    const comparisonSection = page.locator('#comparison');
    const comparisonTable = comparisonSection.locator('table.comparison-table');

    // Table should be visible
    await expect(comparisonTable).toBeVisible();

    // Table should have appropriate scope attributes on headers
    const headerCells = comparisonTable.locator('thead th');
    const count = await headerCells.count();

    for (let i = 0; i < count; i++) {
      const scope = await headerCells.nth(i).getAttribute('scope');
      expect(scope).toBe('col');
    }

    // Row headers should have proper scope
    const rowHeaders = comparisonTable.locator('tbody th');
    const rowHeaderCount = await rowHeaders.count();

    for (let i = 0; i < rowHeaderCount; i++) {
      const scope = await rowHeaders.nth(i).getAttribute('scope');
      expect(scope).toBe('row');
    }
  });

  test('should display memory-first comparison row', async ({ page }) => {
    const comparisonSection = page.locator('#comparison');
    const comparisonTable = comparisonSection.locator('table.comparison-table');

    // Find the memory-first row
    const memoryRow = comparisonTable.locator('tr[data-feature="memory-first"]');
    await expect(memoryRow).toBeVisible();

    // All three should support memory-first operations
    const cells = memoryRow.locator('td');
    await expect(cells.nth(0)).toContainText('Yes');
    await expect(cells.nth(1)).toContainText('Yes');
    await expect(cells.nth(2)).toContainText('Yes');
  });

  test('use case guidance should include MirDB advantages', async ({ page }) => {
    const comparisonSection = page.locator('#comparison');
    const useCaseGuidance = comparisonSection.locator('.use-case-guidance');

    // Should mention key advantages
    const guidanceText = await useCaseGuidance.textContent();

    // Should mention persistence advantage over memcached
    expect(guidanceText.toLowerCase()).toMatch(/persist|durable|survives/);

    // Should mention protocol compatibility
    expect(guidanceText.toLowerCase()).toMatch(/memcached|protocol|compatible|drop-in/);
  });
});
