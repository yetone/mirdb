/**
 * Configuration Section E2E Tests
 * Owner: Scenario 6 - Configuration Documentation Section
 *
 * Tests for:
 * - Configuration table display
 * - Default port (12333) documentation
 * - Work directory (/tmp/mirdb) documentation
 * - Max LSM levels (7) documentation
 * - Size limits (SSTable 100MB, memtable 4MB) documentation
 */

import { test, expect } from '@playwright/test';
import { CONFIG_VALUES } from '../fixtures/test-data.js';

test.describe('Configuration Documentation Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('configuration section is visible on the page', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();
    await expect(configSection.locator('.config__title')).toHaveText('Configuration');
  });

  test('configuration section has proper accessibility attributes', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await expect(configSection).toHaveAttribute('aria-labelledby', 'config-heading');

    const heading = page.locator('#config-heading');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Configuration');
  });

  // Test Case 1: Check for default port documentation
  test('displays default port 12333 documentation', async ({ page }) => {
    const configSection = page.locator('#configuration');

    // Check for port configuration row
    const portRow = configSection.locator('[data-config="port"]');
    await expect(portRow).toBeVisible();

    // Check for addr key
    const keyCell = portRow.locator('.config__key');
    await expect(keyCell).toContainText('addr');

    // Check for port value 12333
    const valueCell = portRow.locator('.config__value');
    const valueText = await valueCell.textContent();
    expect(valueText).toContain('12333');
  });

  // Test Case 2: Check for work directory documentation
  test('displays default work directory /tmp/mirdb documentation', async ({ page }) => {
    const configSection = page.locator('#configuration');

    // Check for work_dir configuration row
    const workDirRow = configSection.locator('[data-config="work_dir"]');
    await expect(workDirRow).toBeVisible();

    // Check for work_dir key
    const keyCell = workDirRow.locator('.config__key');
    await expect(keyCell).toContainText('work_dir');

    // Check for /tmp/mirdb value
    const valueCell = workDirRow.locator('.config__value');
    const valueText = await valueCell.textContent();
    expect(valueText).toContain('/tmp/mirdb');
  });

  // Test Case 3: Check for max levels documentation
  test('displays max LSM levels (7) configuration option', async ({ page }) => {
    const configSection = page.locator('#configuration');

    // Check for max_level configuration row
    const maxLevelRow = configSection.locator('[data-config="max_level"]');
    await expect(maxLevelRow).toBeVisible();

    // Check for max_level key
    const keyCell = maxLevelRow.locator('.config__key');
    await expect(keyCell).toContainText('max_level');

    // Check for value 7
    const valueCell = maxLevelRow.locator('.config__value');
    const valueText = await valueCell.textContent();
    expect(valueText).toContain('7');

    // Check description mentions LSM
    const descCell = maxLevelRow.locator('.config__description');
    const descText = await descCell.textContent();
    expect(descText.toLowerCase()).toContain('lsm');
  });

  // Test Case 4: Check for size limits documentation
  test('displays SSTable max size (100MB) and memtable max size (4MB) documentation', async ({ page }) => {
    const configSection = page.locator('#configuration');

    // Check for sst_max_size configuration row
    const sstRow = configSection.locator('[data-config="sst_max_size"]');
    await expect(sstRow).toBeVisible();

    // Check for SSTable value 100MB
    const sstValue = sstRow.locator('.config__value');
    const sstText = await sstValue.textContent();
    expect(sstText).toContain('100MB');

    // Check for mem_table_max_size configuration row
    const memRow = configSection.locator('[data-config="mem_table_max_size"]');
    await expect(memRow).toBeVisible();

    // Check for memtable value 4MB
    const memValue = memRow.locator('.config__value');
    const memText = await memValue.textContent();
    expect(memText).toContain('4MB');
  });

  test('configuration table displays all expected rows', async ({ page }) => {
    const configTable = page.locator('.config__table');
    await expect(configTable).toBeVisible();

    // Check all configuration rows are present
    const rows = configTable.locator('.config__row');
    await expect(rows).toHaveCount(7);
  });

  test('configuration table has proper header columns', async ({ page }) => {
    const headers = page.locator('.config__header');
    await expect(headers).toHaveCount(3);

    const headerTexts = await headers.allTextContents();
    expect(headerTexts).toContain('Parameter');
    expect(headerTexts).toContain('Default Value');
    expect(headerTexts).toContain('Description');
  });

  test('configuration table has aria-label for accessibility', async ({ page }) => {
    const configTable = page.locator('.config__table');
    await expect(configTable).toHaveAttribute('aria-label', 'Configuration options');
  });

  test('displays example configuration code block', async ({ page }) => {
    const exampleSection = page.locator('.config__example');
    await expect(exampleSection).toBeVisible();

    const exampleTitle = exampleSection.locator('.config__example-title');
    await expect(exampleTitle).toHaveText('Example Configuration');

    const codeBlock = exampleSection.locator('.config__code');
    await expect(codeBlock).toBeVisible();

    // Verify code contains TOML configuration
    const codeText = await codeBlock.textContent();
    expect(codeText).toContain('addr');
    expect(codeText).toContain('12333');
    expect(codeText).toContain('work_dir');
    expect(codeText).toContain('mirdb');
  });

  test('displays configuration note with command line usage', async ({ page }) => {
    const note = page.locator('.config__note');
    await expect(note).toBeVisible();

    const noteText = await note.textContent();
    expect(noteText).toContain('mirdb -c');
    expect(noteText).toContain('config.toml');
  });

  test('all configuration values use monospace code styling', async ({ page }) => {
    const valueCodeElements = page.locator('.config__value code');
    const count = await valueCodeElements.count();
    expect(count).toBe(7);

    // Each value should be visible and styled as code
    for (let i = 0; i < count; i++) {
      const codeEl = valueCodeElements.nth(i);
      await expect(codeEl).toBeVisible();
    }
  });

  test('configuration section displays intro text', async ({ page }) => {
    const intro = page.locator('.config__intro');
    await expect(intro).toBeVisible();

    const introText = await intro.textContent();
    expect(introText.toLowerCase()).toContain('toml');
    expect(introText.toLowerCase()).toContain('configuration');
  });

  test('configuration rows have descriptions', async ({ page }) => {
    const descriptions = page.locator('.config__description');
    const count = await descriptions.count();
    expect(count).toBe(7);

    // Each description should have meaningful content
    for (let i = 0; i < count; i++) {
      const desc = descriptions.nth(i);
      const text = await desc.textContent();
      expect(text.length).toBeGreaterThan(10);
    }
  });
});
