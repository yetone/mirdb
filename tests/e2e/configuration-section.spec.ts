import { test, expect } from '@playwright/test';

test.describe('Configuration Parameters Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Search page for configuration or defaults section
  // Expected: Configuration information is present on page
  test('TC1: Configuration section exists on page', async ({ page }) => {
    // Look for configuration section by id, class, or heading
    const configSection = page.locator('#configuration, .configuration, section:has(h2:text-matches("configuration", "i"))');
    await expect(configSection).toBeVisible();

    // Also verify there's a heading about configuration/defaults
    const configHeading = page.locator('h2').filter({ hasText: /configuration|defaults/i });
    await expect(configHeading).toBeVisible();
  });

  // Test Case 2: Search for listen address '12333'
  // Expected: Default port 12333 is documented
  test('TC2: Default listen address port 12333 is documented', async ({ page }) => {
    // Search for the port number 12333 in the page content
    const pageContent = await page.textContent('body');
    expect(pageContent).toContain('12333');

    // Verify it's in context of listen address
    const configSection = page.locator('#configuration, .configuration');
    const configText = await configSection.textContent();
    expect(configText?.toLowerCase()).toContain('listen');
    expect(configText).toContain('12333');

    // Check for full address format 0.0.0.0:12333
    expect(configText).toContain('0.0.0.0:12333');
  });

  // Test Case 3: Search for memtable size configuration
  // Expected: Memtable max size (4MB) is documented
  test('TC3: Memtable max size (4MB) is documented', async ({ page }) => {
    const configSection = page.locator('#configuration, .configuration');
    const configText = await configSection.textContent();
    const lowerText = configText?.toLowerCase() || '';

    // Check for memtable mention
    expect(lowerText).toContain('memtable');

    // Check for 4MB value (could be "4MB", "4 MB", or similar)
    expect(configText).toMatch(/4\s*MB/i);
  });

  // Test Case 4: Search for work directory default
  // Expected: Default work directory /tmp/mirdb or configuration option shown
  test('TC4: Default work directory is documented', async ({ page }) => {
    const configSection = page.locator('#configuration, .configuration');
    const configText = await configSection.textContent();
    const lowerText = configText?.toLowerCase() || '';

    // Check for work directory mention
    expect(lowerText).toMatch(/work.*dir|directory/);

    // Check for the default value /tmp/mirdb
    expect(configText).toContain('/tmp/mirdb');
  });

  // Additional verifications per PRD requirements
  test('TC5: SSTable max size (100MB) is documented', async ({ page }) => {
    const configSection = page.locator('#configuration, .configuration');
    const configText = await configSection.textContent();
    const lowerText = configText?.toLowerCase() || '';

    // Check for SSTable mention
    expect(lowerText).toContain('sstable');

    // Check for 100MB value
    expect(configText).toMatch(/100\s*MB/i);
  });

  test('TC6: Max LSM levels (7) is documented', async ({ page }) => {
    const configSection = page.locator('#configuration, .configuration');
    const configText = await configSection.textContent();
    const lowerText = configText?.toLowerCase() || '';

    // Check for LSM levels mention
    expect(lowerText).toMatch(/lsm.*level|level.*lsm/);

    // Check for 7 levels
    expect(configText).toContain('7');
  });

  test('TC7: Block size (4KB) is documented', async ({ page }) => {
    const configSection = page.locator('#configuration, .configuration');
    const configText = await configSection.textContent();
    const lowerText = configText?.toLowerCase() || '';

    // Check for block size mention
    expect(lowerText).toMatch(/block.*size/);

    // Check for 4KB value
    expect(configText).toMatch(/4\s*KB/i);
  });

  test('TC8: Configuration displayed in structured format (table)', async ({ page }) => {
    const configSection = page.locator('#configuration, .configuration');

    // Check for table element with configuration data
    const table = configSection.locator('table');
    await expect(table).toBeVisible();

    // Verify table has header row
    const headerRow = table.locator('tr').first();
    await expect(headerRow).toBeVisible();

    // Verify there are multiple rows (at least header + some data rows)
    const rows = table.locator('tr');
    const rowCount = await rows.count();
    expect(rowCount).toBeGreaterThanOrEqual(4); // header + at least 3 config params
  });
});
