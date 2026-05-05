const { test, expect } = require('@playwright/test');

test('Configuration values are displayed correctly', async ({ page }) => {
  await page.goto('http://localhost:3000');

  // Check Configuration section exists
  const configSection = page.locator('#configuration');
  await expect(configSection).toBeVisible();

  // Check table structure
  const table = configSection.locator('table.config-table');
  await expect(table).toBeVisible();

  // Verify listen address
  await expect(table.locator('tr', { hasText: 'Listen address' })).toContainText('0.0.0.0:12333');

  // Verify max LSM levels and SSTable size
  await expect(table.locator('tr', { hasText: 'Max LSM levels' })).toContainText('7');
  await expect(table.locator('tr', { hasText: 'SSTable max size' })).toContainText('100MB');

  // Verify table format
  const headers = table.locator('thead th');
  await expect(headers.nth(0)).toHaveText('Setting');
  await expect(headers.nth(1)).toHaveText('Default Value');
});