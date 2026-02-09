/**
 * E2E tests for Configuration section.
 * Owner: Scenario 6 - Configuration Reference
 *
 * Tests visual layout and responsive behavior of the configuration table.
 */

import { test, expect } from '@playwright/test';

test.describe('Configuration Section E2E', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForSelector('#configuration');
  });

  test('configuration table displays properly on desktop viewport', async ({ page }) => {
    await page.setViewportSize({ width: 1200, height: 800 });

    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    const configTable = page.locator('.config-table');
    await expect(configTable).toBeVisible();

    // Check table headers are visible
    const headers = page.locator('.config-table th');
    await expect(headers.nth(0)).toHaveText('Option');
    await expect(headers.nth(1)).toHaveText('Default');
    await expect(headers.nth(2)).toHaveText('Description');
  });

  test('configuration table is scrollable or stacks vertically on mobile viewport', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });

    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // The table wrapper should exist
    const tableWrapper = page.locator('.config-table-wrapper');
    await expect(tableWrapper).toBeVisible();

    // Check the computed styles - on mobile it should either:
    // 1. Have overflow-x for horizontal scrolling, OR
    // 2. Stack vertically (table rows become flex column)
    const wrapperStyle = await tableWrapper.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        overflowX: computed.overflowX,
      };
    });

    // Also check if the tbody has flex layout (stacked cards)
    const tbodyStyle = await page.locator('.config-table tbody').evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        display: computed.display,
        flexDirection: computed.flexDirection,
      };
    });

    // Either the wrapper has scroll capability OR the tbody stacks vertically
    const isScrollable = wrapperStyle.overflowX === 'auto' || wrapperStyle.overflowX === 'scroll';
    const isStacked = tbodyStyle.display === 'flex' && tbodyStyle.flexDirection === 'column';

    expect(isScrollable || isStacked).toBe(true);
  });

  test('configuration section is accessible', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await expect(configSection).toHaveAttribute('aria-labelledby', 'configuration-heading');

    const heading = page.locator('#configuration-heading');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Configuration Reference');

    const table = page.locator('.config-table');
    await expect(table).toHaveAttribute('role', 'table');
    await expect(table).toHaveAttribute('aria-label', 'Configuration options');
  });

  test('displays address configuration option', async ({ page }) => {
    const addrRow = page.locator('.config-row', { has: page.locator('code', { hasText: 'addr' }) });
    await expect(addrRow).toBeVisible();

    const defaultValue = addrRow.locator('.config-default code');
    await expect(defaultValue).toHaveText('0.0.0.0:12333');
  });

  test('displays work directory configuration option', async ({ page }) => {
    const workDirRow = page.locator('.config-row', { has: page.locator('code', { hasText: 'work_dir' }) });
    await expect(workDirRow).toBeVisible();

    const defaultValue = workDirRow.locator('.config-default code');
    await expect(defaultValue).toHaveText('/tmp/mirdb');
  });

  test('each configuration option has a description', async ({ page }) => {
    const configRows = page.locator('.config-row');
    const rowCount = await configRows.count();

    expect(rowCount).toBeGreaterThan(0);

    for (let i = 0; i < rowCount; i++) {
      const row = configRows.nth(i);
      const description = row.locator('.config-description');
      await expect(description).toBeVisible();

      const descText = await description.textContent();
      expect(descText?.trim().length).toBeGreaterThan(0);
    }
  });
});
