const { test, expect } = require('@playwright/test');

test.describe('Feature Comparison Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Feature comparison section exists on the page', async ({ page }) => {
    // Query for comparison table or section
    const comparisonSection = page.locator('#comparison');

    // Verify feature comparison section exists on the page
    await expect(comparisonSection).toBeVisible();

    // Verify section has proper heading
    const sectionTitle = comparisonSection.locator('.section-title');
    await expect(sectionTitle).toBeVisible();
    await expect(sectionTitle).toContainText('Comparison');
  });

  test('TC2: Comparison shows MirDB has persistence while memcached does not', async ({ page }) => {
    // Navigate to comparison section
    const comparisonSection = page.locator('#comparison');
    await comparisonSection.scrollIntoViewIfNeeded();

    // Check persistence feature in comparison
    const comparisonTable = page.locator('[data-testid="comparison-table"]');
    await expect(comparisonTable).toBeVisible();

    // Find the persistence row
    const persistenceRow = page.locator('[data-testid="comparison-row-persistence"]');
    await expect(persistenceRow).toBeVisible();

    // Verify MirDB shows positive indicator for persistence (checkmark or "Yes")
    const mirdbPersistence = persistenceRow.locator('[data-testid="mirdb-persistence"]');
    await expect(mirdbPersistence).toBeVisible();

    // Check for a positive indicator (checkmark symbol or text indicating "Yes")
    const mirdbPersistenceText = await mirdbPersistence.textContent();
    expect(mirdbPersistenceText.match(/(\u2713|\u2714|Yes|true)/i) ||
           await mirdbPersistence.locator('.check-icon, .checkmark, svg').count() > 0).toBeTruthy();

    // Verify memcached shows negative indicator for persistence (X or "No")
    const memcachedPersistence = persistenceRow.locator('[data-testid="memcached-persistence"]');
    await expect(memcachedPersistence).toBeVisible();

    // Check for a negative indicator (X symbol or text indicating "No")
    const memcachedPersistenceText = await memcachedPersistence.textContent();
    expect(memcachedPersistenceText.match(/(\u2717|\u2718|No|false|X|-)/i) ||
           await memcachedPersistence.locator('.x-icon, .cross, svg').count() > 0).toBeTruthy();
  });

  test('TC3: Comparison shows MirDB is memcached protocol compatible', async ({ page }) => {
    // Navigate to comparison section
    const comparisonSection = page.locator('#comparison');
    await comparisonSection.scrollIntoViewIfNeeded();

    // Check protocol compatibility in comparison
    const comparisonTable = page.locator('[data-testid="comparison-table"]');
    await expect(comparisonTable).toBeVisible();

    // Find the protocol compatibility row
    const protocolRow = page.locator('[data-testid="comparison-row-protocol"]');
    await expect(protocolRow).toBeVisible();

    // Verify MirDB shows positive indicator for memcached protocol
    const mirdbProtocol = protocolRow.locator('[data-testid="mirdb-protocol"]');
    await expect(mirdbProtocol).toBeVisible();

    // Check for a positive indicator (checkmark symbol or text indicating "Yes")
    const mirdbProtocolText = await mirdbProtocol.textContent();
    expect(mirdbProtocolText.match(/(\u2713|\u2714|Yes|true)/i) ||
           await mirdbProtocol.locator('.check-icon, .checkmark, svg').count() > 0).toBeTruthy();

    // Verify memcached also shows positive indicator (it's the native protocol)
    const memcachedProtocol = protocolRow.locator('[data-testid="memcached-protocol"]');
    await expect(memcachedProtocol).toBeVisible();

    const memcachedProtocolText = await memcachedProtocol.textContent();
    expect(memcachedProtocolText.match(/(\u2713|\u2714|Yes|true|Native)/i) ||
           await memcachedProtocol.locator('.check-icon, .checkmark, svg').count() > 0).toBeTruthy();
  });

  test('TC4: Comparison uses clear visual format (table, checkmarks, or similar)', async ({ page }) => {
    // Navigate to comparison section
    const comparisonSection = page.locator('#comparison');
    await comparisonSection.scrollIntoViewIfNeeded();

    // Verify comparison is visually clear with structured layout
    const comparisonTable = page.locator('[data-testid="comparison-table"]');
    await expect(comparisonTable).toBeVisible();

    // Check for table headers
    const mirdbHeader = page.locator('[data-testid="comparison-header-mirdb"]');
    const memcachedHeader = page.locator('[data-testid="comparison-header-memcached"]');

    await expect(mirdbHeader).toBeVisible();
    await expect(mirdbHeader).toContainText('MirDB');

    await expect(memcachedHeader).toBeVisible();
    await expect(memcachedHeader).toContainText('Memcached');

    // Verify there are multiple comparison rows for different features
    const comparisonRows = comparisonTable.locator('[data-testid^="comparison-row-"]');
    const rowCount = await comparisonRows.count();
    expect(rowCount).toBeGreaterThanOrEqual(3); // At least 3 features compared

    // Verify visual indicators are present (checkmarks or X marks)
    const checkmarks = comparisonTable.locator('.comparison-yes, .check, [data-value="yes"]');
    const crossmarks = comparisonTable.locator('.comparison-no, .cross, [data-value="no"]');

    const checkmarkCount = await checkmarks.count();
    const crossmarkCount = await crossmarks.count();

    // Should have at least some visual indicators
    expect(checkmarkCount + crossmarkCount).toBeGreaterThan(0);
  });

  test('TC5: Comparison section is accessible via navigation', async ({ page }) => {
    // Check if comparison link exists in navigation
    const navLink = page.locator('[data-testid="nav-link-comparison"]');

    // Verify link is visible in desktop nav
    await expect(navLink).toBeVisible();

    // Click and verify navigation to comparison section
    await navLink.click();

    // Verify the comparison section is now in view
    const comparisonSection = page.locator('#comparison');
    await expect(comparisonSection).toBeInViewport();
  });
});
